"""API para descargar metadatos y binarios de adjuntos de Oracle Service Cloud.

Ambos metodos son asincronos: encolan un job en segundo plano y devuelven un
job_id; el avance y el resultado se consultan en GET /jobs/{job_id}.

- POST /GetMetadataAttachments: toma un lote (batch_size) de archivos
  ServiceRequest_*.csv aun no procesados de la carpeta de entrada, consulta los
  adjuntos de cada Reference Number y genera un CSV de metadatos por archivo.
  Los archivos completados sin errores quedan registrados en
  _processed_files.json (en la carpeta de salida) y no se vuelven a tomar.
- POST /GetAttachmentBinary: toma un CSV de metadatos (o un lote de una
  carpeta de CSVs de metadatos) y descarga el binario de cada adjunto.
  Registro equivalente en _downloaded_files.json.
"""

import base64
import binascii
import csv
import json
import logging
import os
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from app import config
from app.jobs import JobManager
from app.osc_client import METADATA_FIELDS, RETRYABLE_STATUS, OscClient, get_file_contents_href
from app.storage import GcpStorage, LocalStorage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Log dedicado SOLO a errores (ademas de la consola). Facilita revisar despues
# que salio mal sin tener que buscar entre todo el log INFO.
_error_file_handler = logging.FileHandler(config.get_error_log_file(), encoding="utf-8")
_error_file_handler.setLevel(logging.ERROR)
_error_file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
)
logging.getLogger().addHandler(_error_file_handler)

# Al apagar el servidor (Ctrl+C) se avisa a los jobs en curso para que
# cancelen las llamadas pendientes; sin esto la terminal queda bloqueada
# hasta agotar toda la cola de SRs.
shutdown_event = threading.Event()
_job_threads: list[threading.Thread] = []


class ShutdownRequested(Exception):
    pass


class CircuitOpen(Exception):
    """El servicio parece caido: demasiados fallos transitorios consecutivos."""


class CircuitBreaker:
    """Abre el circuito tras N fallos transitorios consecutivos (5xx/429/red).

    Evita quemar reintentos contra un servicio caido (p. ej. mantenimiento de
    Oracle): el job se detiene como 'interrupted' y se reanuda al relanzarlo.
    Un exito reinicia el contador. threshold <= 0 lo desactiva.
    """

    def __init__(self, threshold: int):
        self.threshold = threshold
        self._consecutive = 0
        self._lock = threading.Lock()

    def record_success(self) -> None:
        with self._lock:
            self._consecutive = 0

    def record_failure(self) -> int:
        with self._lock:
            self._consecutive += 1
            return self._consecutive

    def is_open(self) -> bool:
        with self._lock:
            return self.threshold > 0 and self._consecutive >= self.threshold


def is_transient_error(exc: Exception) -> bool:
    """True si el error es de servicio/red (5xx, 429, conexion, timeout)."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        return response is not None and response.status_code in RETRYABLE_STATUS
    return False


def start_job_thread(target, args) -> None:
    thread = threading.Thread(target=target, args=args, daemon=True)
    _job_threads.append(thread)
    thread.start()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    shutdown_event.set()
    # Espera a que los jobs cancelen lo pendiente y dejen su estatus como
    # 'interrupted'; las llamadas ya en vuelo terminan (acotadas por OSC_TIMEOUT)
    for thread in _job_threads:
        if thread.is_alive():
            thread.join(timeout=config.get_timeout() + 30)


app = FastAPI(
    title="down-attach-osc",
    description="Descarga de metadatos y binarios de adjuntos de solicitudes de servicio (Oracle Service Cloud)",
    lifespan=lifespan,
)

jobs = JobManager()

SR_ID_COLUMN = "Service Request ID"
REFERENCE_COLUMN = "Reference Number"
HREF_COLUMN = "FileContentsHref"
OUTPUT_COLUMNS = [SR_ID_COLUMN, REFERENCE_COLUMN, *METADATA_FIELDS, HREF_COLUMN]

METADATA_STATE_FILE = "_processed_files.json"
BINARY_STATE_FILE = "_downloaded_files.json"
CLOB_MESSAGES_STATE_FILE = "_processed_clob_messages.json"

# GetMetadataClobAndMessages: campos CLOB (base64 -> texto) y campos de mensajes
CLOB_FIELDS = ["arin_comentarios_cifrado_c", "col_tex_plantilla_c"]
CLOB_OUTPUT_COLUMNS = [REFERENCE_COLUMN, *CLOB_FIELDS]
MESSAGE_FIELDS = [
    "MessageId",
    "CreationDate",
    "CreatedBy",
    "SrId",
    "SrNumber",
    "MessageTypeCd",
    "ChannelTypeCd",
    "ChannelId",
    "StatusCd",
    "ProcessingStatusCd",
    "NotificationProcessingStatusCd",
    "TemplateName",
]
MESSAGE_CONTENT_COLUMN = "MessageContent"  # ruta al archivo HTML guardado
MESSAGE_OUTPUT_COLUMNS = [*MESSAGE_FIELDS, MESSAGE_CONTENT_COLUMN]
MESSAGE_CONTENT_DIR = "message_content"  # subcarpeta de HTMLs, por SR

INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

_state_lock = threading.Lock()

# Archivos reservados por jobs en curso. Permite lanzar varios jobs en lote
# sobre la misma carpeta: cada uno reserva sus archivos al iniciar y los
# demas los saltan, evitando que dos jobs escriban el mismo CSV/checkpoint.
_active_files_lock = threading.Lock()
_active_files: set[tuple[str, str, str]] = set()


def _file_key(output_folder: str, state_file: str, name: str) -> tuple[str, str, str]:
    return (os.path.abspath(output_folder), state_file, name)


def reserve_batch(
    files: list[str], output_folder: str, state_file: str, state: dict, batch_size: int
) -> tuple[list[str], int]:
    """Selecciona y reserva atomicamente el siguiente lote de archivos pendientes.

    Excluye los ya procesados (manifiesto) y los reservados por otros jobs.
    """
    with _active_files_lock:
        pending = [
            f for f in files
            if os.path.basename(f) not in state
            and _file_key(output_folder, state_file, os.path.basename(f)) not in _active_files
        ]
        batch = pending[:batch_size] if batch_size > 0 else pending
        for f in batch:
            _active_files.add(_file_key(output_folder, state_file, os.path.basename(f)))
        return batch, len(pending) - len(batch)


def reserve_explicit(paths: list[str], output_folder: str, state_file: str) -> list[str]:
    """Reserva archivos pedidos explicitamente; devuelve los que ya estan ocupados."""
    with _active_files_lock:
        busy = [
            os.path.basename(p) for p in paths
            if _file_key(output_folder, state_file, os.path.basename(p)) in _active_files
        ]
        if busy:
            return busy
        for p in paths:
            _active_files.add(_file_key(output_folder, state_file, os.path.basename(p)))
        return []


def release_file(output_folder: str, state_file: str, name: str) -> None:
    with _active_files_lock:
        _active_files.discard(_file_key(output_folder, state_file, name))


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def build_client(pool_size: int = 10) -> OscClient:
    try:
        return OscClient(
            domain=config.get_domain(),
            username=config.get_username(),
            password=config.get_password(),
            timeout=config.get_timeout(),
            max_retries=config.get_max_retries(),
            backoff=config.get_retry_backoff(),
            abort_event=shutdown_event,
            pool_size=pool_size,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


def sanitize_filename(name: str) -> str:
    return INVALID_FILENAME_CHARS.sub("_", name).strip() or "sin_nombre"


def decode_clob(value) -> str:
    """Decodifica un campo CLOB base64 a texto UTF-8.

    Los campos arin_comentarios_cifrado_c / col_tex_plantilla_c vienen en
    base64 (texto plano, no cifrado). Si el valor es null o no es base64
    valido, se devuelve tal cual para no perder informacion.
    """
    if not value:
        return ""
    try:
        return base64.b64decode(value, validate=True).decode("utf-8", errors="replace")
    except (binascii.Error, ValueError):
        return str(value)


def attachment_base_name(row: dict) -> str:
    """Nombre original del adjunto (FileName; fallback a Title o 'adjunto')."""
    return row.get("FileName") or row.get("Title") or "adjunto"


def attachment_prefix(row: dict) -> str:
    """Identificador unico del adjunto para desambiguar nombres repetidos."""
    return (row.get("DmDocumentId") or row.get("AttachedDocumentId") or "").strip()


def target_subdir(reference: str) -> str:
    return sanitize_filename(reference) or "sin_sr"


def resolve_target_rel(row: dict, duplicate_keys: set) -> str:
    """Ruta relativa del binario ('subcarpetaSR/nombre'); prefija con DmDocumentId solo si choca.

    Es agnostica del destino: LocalStorage la une a output_folder y GcpStorage
    la antepone con el prefix del bucket.
    """
    reference = (row.get(REFERENCE_COLUMN) or "").strip()
    subdir = target_subdir(reference)
    base = sanitize_filename(attachment_base_name(row))
    if (subdir, base) in duplicate_keys:
        # Nombre repetido en el mismo SR: prefija con el id unico del adjunto
        prefix = attachment_prefix(row)
        name = sanitize_filename(f"{prefix}_{attachment_base_name(row)}") if prefix else base
    else:
        name = base
    return f"{subdir}/{name}"


def load_state(folder: str, state_file: str) -> dict:
    path = os.path.join(folder, state_file)
    if not os.path.isfile(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def mark_processed(folder: str, state_file: str, key: str, entry: dict) -> None:
    with _state_lock:
        state = load_state(folder, state_file)
        state[key] = entry
        path = os.path.join(folder, state_file)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)


def list_csv_files(folder: str) -> list[str]:
    return sorted(
        os.path.join(folder, name)
        for name in os.listdir(folder)
        if name.lower().endswith(".csv") and not name.startswith("_")
    )


# ---------------------------------------------------------------------------
# GetMetadataAttachments
# ---------------------------------------------------------------------------

class MetadataRequest(BaseModel):
    input_folder: str
    output_folder: str
    # Nombres de archivos especificos a procesar (dentro de input_folder).
    # Si se envia, se procesan exactamente esos, ignorando batch_size y el manifiesto.
    files: Optional[list[str]] = None
    batch_size: int = 10  # 0 = procesar todos los pendientes
    force: bool = False  # true = reprocesar aunque esten en _processed_files.json
    # Llamadas en paralelo solo para este job; si no se envia, usa OSC_MAX_WORKERS
    max_workers: Optional[int] = Field(default=None, ge=1, le=64)


def read_sr_numbers(csv_path: str) -> list[tuple[str, str]]:
    """Lee (Service Request ID, Reference Number) de un CSV de entrada, sin duplicados."""
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or REFERENCE_COLUMN not in reader.fieldnames:
            raise ValueError(
                f"El archivo {os.path.basename(csv_path)} no tiene la columna '{REFERENCE_COLUMN}'"
            )
        for row in reader:
            reference = (row.get(REFERENCE_COLUMN) or "").strip()
            if not reference or reference in seen:
                continue
            seen.add(reference)
            pairs.append(((row.get(SR_ID_COLUMN) or "").strip(), reference))
    return pairs


def process_input_file(
    client: OscClient,
    csv_path: str,
    output_folder: str,
    max_workers: int,
    job_id: str,
    force: bool = False,
    breaker: CircuitBreaker | None = None,
) -> dict:
    """Consulta los adjuntos de cada SR del archivo y escribe el CSV de metadatos.

    Escritura incremental: cada SR consultado se agrega de inmediato al CSV de
    salida y su numero queda registrado en un checkpoint (<salida>.progress).
    Si el proceso se corta, la siguiente corrida retoma solo los SR faltantes.
    """
    file_name = os.path.basename(csv_path)
    pairs = read_sr_numbers(csv_path)
    output_name = f"{os.path.splitext(file_name)[0]}_attachments.csv"
    output_path = os.path.join(output_folder, output_name)
    progress_path = output_path + ".progress"

    done: set[str] = set()
    if force:
        for path in (output_path, progress_path):
            if os.path.exists(path):
                os.remove(path)
    elif os.path.isfile(progress_path) and os.path.isfile(output_path):
        with open(progress_path, encoding="utf-8") as fh:
            done = {line.strip() for line in fh if line.strip()}

    pending = [pair for pair in pairs if pair[1] not in done]
    resuming = bool(done)
    logger.info(
        "Procesando %s (%d solicitudes, %d ya consultadas, %d pendientes)",
        file_name, len(pairs), len(pairs) - len(pending), len(pending),
    )
    jobs.set_progress(
        job_id,
        current_file=file_name,
        current_file_srs=len(pairs),
        current_file_pending=len(pending),
    )

    errors: list[dict] = []
    write_lock = threading.Lock()

    with open(output_path, "a" if resuming else "w", newline="", encoding="utf-8-sig") as out_fh, \
            open(progress_path, "a" if resuming else "w", encoding="utf-8") as prog_fh:
        writer = csv.DictWriter(out_fh, fieldnames=OUTPUT_COLUMNS)
        if not resuming:
            writer.writeheader()
            out_fh.flush()

        def fetch(pair: tuple[str, str]) -> tuple[tuple[str, str], list[dict]]:
            # Con el circuito abierto o en apagado, drena la cola sin llamar al API
            if shutdown_event.is_set() or (breaker and breaker.is_open()):
                raise CircuitOpen()
            return pair, client.get_attachments(pair[1])

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(fetch, pair): pair for pair in pending}
            for future in as_completed(futures):
                if shutdown_event.is_set() or (breaker and breaker.is_open()):
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                sr_id, reference = futures[future]
                try:
                    _, items = future.result()
                except CircuitOpen:
                    continue  # SR no consultado; queda pendiente para la proxima corrida
                except Exception as exc:
                    logger.error("Error consultando adjuntos de SR %s: %s", reference, exc)
                    errors.append({"srNumber": reference, "error": str(exc)})
                    jobs.increment(job_id, "sr_errors")
                    if breaker and is_transient_error(exc):
                        breaker.record_failure()
                    continue
                if breaker:
                    breaker.record_success()
                rows = []
                for item in items:
                    row = {SR_ID_COLUMN: sr_id, REFERENCE_COLUMN: reference}
                    for field in METADATA_FIELDS:
                        row[field] = item.get(field, "")
                    row[HREF_COLUMN] = get_file_contents_href(item)
                    rows.append(row)
                with write_lock:
                    writer.writerows(rows)
                    out_fh.flush()
                    prog_fh.write(reference + "\n")
                    prog_fh.flush()
                jobs.increment(job_id, "srs_consulted")

    if shutdown_event.is_set():
        # El checkpoint queda en disco: la proxima corrida retoma los SR faltantes
        raise ShutdownRequested()
    if breaker and breaker.is_open():
        raise CircuitOpen(
            f"{breaker.threshold} fallos transitorios consecutivos (5xx/429/red)"
        )

    with open(output_path, newline="", encoding="utf-8-sig") as fh:
        attachments = max(sum(1 for _ in fh) - 1, 0)

    if not errors:
        # Archivo completo y sin errores: el checkpoint ya no hace falta
        os.remove(progress_path)

    # Verificacion: todo SR esperado quedo consultado o registrado como error
    consulted = len(pairs) - len(errors)
    verification = {
        "expected_srs": len(pairs),
        "consulted": consulted,
        "failed": len(errors),
        "ok": len(errors) == 0,
    }
    if errors:
        logger.error(
            "Verificacion %s: %d de %d SR fallaron y quedan pendientes de reintento",
            output_name, len(errors), len(pairs),
        )

    logger.info("Generado %s (%d adjuntos, %d errores)", output_name, attachments, len(errors))
    return {
        "input_file": file_name,
        "output_file": output_path,
        "service_requests": len(pairs),
        "resumed_srs": len(pairs) - len(pending),
        "attachments": attachments,
        "errors": errors,
        "verification": verification,
    }


def run_metadata_job(
    job_id: str,
    client: OscClient,
    batch: list[str],
    output_folder: str,
    force: bool = False,
    max_workers: int | None = None,
) -> None:
    max_workers = max_workers or config.get_max_workers()
    breaker = CircuitBreaker(config.get_circuit_threshold())
    results: list[dict] = []
    had_errors = False
    try:
        for index, csv_path in enumerate(batch, start=1):
            file_name = os.path.basename(csv_path)
            try:
                result = process_input_file(client, csv_path, output_folder, max_workers, job_id, force, breaker)
            except ShutdownRequested:
                logger.info("Job %s interrumpido por apagado del servidor en %s", job_id, file_name)
                jobs.finish(job_id, "interrupted", result={"results": results})
                return
            except CircuitOpen as exc:
                message = (
                    f"Circuit breaker abierto en {file_name}: {exc}. El servicio parece caido "
                    "(mantenimiento?); el avance quedo en el checkpoint. Relance el metodo "
                    "cuando el servicio se recupere (verifique con GET /health)."
                )
                logger.error("Job %s: %s", job_id, message)
                jobs.finish(job_id, "interrupted", result={"results": results}, error=message)
                return
            except Exception as exc:
                result = {"input_file": file_name, "error": str(exc)}
            finally:
                release_file(output_folder, METADATA_STATE_FILE, file_name)
            results.append(result)

            file_failed = "error" in result or bool(result.get("errors"))
            had_errors = had_errors or file_failed
            # Solo los archivos completados sin errores quedan como procesados;
            # los que fallaron vuelven a ser candidatos en la siguiente corrida.
            if not file_failed:
                mark_processed(
                    output_folder,
                    METADATA_STATE_FILE,
                    file_name,
                    {
                        "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "job_id": job_id,
                        "output_file": result["output_file"],
                        "service_requests": result["service_requests"],
                        "attachments": result["attachments"],
                    },
                )
            jobs.set_progress(job_id, processed_files=index, current_file=None)
        summary = {
            "files": len(results),
            "expected_srs": sum(r.get("service_requests", 0) for r in results),
            "consulted": sum(r.get("verification", {}).get("consulted", 0) for r in results),
            "failed_srs": sum(len(r.get("errors", [])) for r in results),
            "all_ok": all(r.get("verification", {}).get("ok", False) for r in results),
        }
        jobs.finish(
            job_id,
            "completed_with_errors" if had_errors else "completed",
            result={"summary": summary, "results": results},
        )
    except Exception as exc:
        logger.exception("Job %s fallo", job_id)
        jobs.finish(job_id, "failed", error=str(exc))
    finally:
        # Libera cualquier reserva restante (archivos no procesados por corte o fallo)
        for csv_path in batch:
            release_file(output_folder, METADATA_STATE_FILE, os.path.basename(csv_path))


@app.post("/GetMetadataAttachments")
def get_metadata_attachments(request: MetadataRequest):
    if not os.path.isdir(request.input_folder):
        raise HTTPException(status_code=400, detail=f"La carpeta de entrada no existe: {request.input_folder}")

    effective_workers = request.max_workers or config.get_max_workers()
    client = build_client(pool_size=effective_workers)
    os.makedirs(request.output_folder, exist_ok=True)
    if request.files:
        # Seleccion explicita: se procesan exactamente esos archivos
        batch = [os.path.join(request.input_folder, name) for name in request.files]
        missing = [name for name, path in zip(request.files, batch) if not os.path.isfile(path)]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Archivos no encontrados en {request.input_folder}: {', '.join(missing)}",
            )
        busy = reserve_explicit(batch, request.output_folder, METADATA_STATE_FILE)
        if busy:
            raise HTTPException(
                status_code=409,
                detail=f"Archivos en proceso por otro job: {', '.join(busy)}",
            )
        pending_after = 0
    else:
        input_files = list_csv_files(request.input_folder)
        if not input_files:
            raise HTTPException(status_code=400, detail="La carpeta de entrada no contiene archivos .csv")
        state = {} if request.force else load_state(request.output_folder, METADATA_STATE_FILE)
        batch, pending_after = reserve_batch(
            input_files, request.output_folder, METADATA_STATE_FILE, state, request.batch_size
        )
        if not batch:
            return {
                "job_id": None,
                "message": "No hay archivos pendientes: todos estan registrados en "
                f"{METADATA_STATE_FILE} o en proceso por otro job (use force=true para reprocesar)",
                "total_files": len(input_files),
            }

    job = jobs.create("GetMetadataAttachments", request.model_dump())
    jobs.set_progress(
        job["job_id"],
        total_files=len(batch),
        processed_files=0,
        pending_after_batch=pending_after,
        srs_consulted=0,
        sr_errors=0,
    )
    start_job_thread(
        run_metadata_job,
        (job["job_id"], client, batch, request.output_folder, request.force, effective_workers),
    )
    return {
        "job_id": job["job_id"],
        "status": "running",
        "files_in_batch": [os.path.basename(f) for f in batch],
        "pending_after_batch": pending_after,
        "status_url": f"/jobs/{job['job_id']}",
    }


# ---------------------------------------------------------------------------
# GetAttachmentBinary
# ---------------------------------------------------------------------------

class BinaryRequest(BaseModel):
    metadata_csv: Optional[str] = None  # un CSV especifico, o...
    metadata_folder: Optional[str] = None  # ...una carpeta de CSVs de metadatos
    output_folder: str  # binarios (destino local) y siempre el manifiesto/checkpoints
    batch_size: int = 10  # solo aplica con metadata_folder; 0 = todos
    overwrite: bool = False
    force: bool = False  # true = ignorar _downloaded_files.json
    # Descargas en paralelo solo para este job; si no se envia, usa OSC_MAX_WORKERS
    max_workers: Optional[int] = Field(default=None, ge=1, le=64)
    # Destino de los binarios: "local" (en output_folder) o "gcp" (bucket)
    destination: str = "local"
    gcp_bucket: Optional[str] = None  # requerido si destination=gcp
    gcp_prefix: str = ""  # prefijo opcional dentro del bucket

    @model_validator(mode="after")
    def _validate(self):
        if bool(self.metadata_csv) == bool(self.metadata_folder):
            raise ValueError("Debe indicar metadata_csv o metadata_folder (solo uno)")
        if self.destination not in ("local", "gcp"):
            raise ValueError("destination debe ser 'local' o 'gcp'")
        if self.destination == "gcp" and not self.gcp_bucket:
            raise ValueError("gcp_bucket es requerido cuando destination='gcp'")
        return self


def build_storage(request: "BinaryRequest"):
    """Crea el backend de almacenamiento segun destination (valida credenciales)."""
    if request.destination == "gcp":
        try:
            return GcpStorage(
                bucket=request.gcp_bucket,
                prefix=request.gcp_prefix,
                credentials_file=config.get_gcp_service_account_file(),
            )
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="Falta la dependencia google-cloud-storage (pip install -r requirements.txt)",
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"No se pudo inicializar GCP: {exc}")
    return LocalStorage(request.output_folder)


def download_metadata_file(
    client: OscClient,
    csv_path: str,
    storage,
    overwrite: bool,
    job_id: str,
    max_workers: int | None = None,
    breaker: CircuitBreaker | None = None,
) -> dict:
    file_name = os.path.basename(csv_path)
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or HREF_COLUMN not in reader.fieldnames:
            raise ValueError(
                f"El archivo {file_name} no tiene la columna '{HREF_COLUMN}' "
                "(debe ser generado por GetMetadataAttachments)"
            )
        entries = [row for row in reader if (row.get(HREF_COLUMN) or "").strip()]

    logger.info("Descargando %s (%d adjuntos) -> %s", file_name, len(entries), storage.kind)
    jobs.set_progress(job_id, current_file=file_name, current_file_rows=len(entries))

    # Precarga (en GCP: lista el prefijo una vez) para omitir lo ya subido
    storage.preload()

    # Detecta que destinos (subcarpeta SR + nombre original) se repiten dentro
    # del CSV. Solo esos adjuntos se prefijan con el DmDocumentId para no
    # perderse; los que no chocan conservan su FileName original.
    counts = Counter(
        (target_subdir((row.get(REFERENCE_COLUMN) or "").strip()), sanitize_filename(attachment_base_name(row)))
        for row in entries
    )
    duplicate_keys = {key for key, n in counts.items() if n > 1}

    skipped = 0
    downloaded = 0
    errors: list[dict] = []
    done_rels: set[str] = set()
    counters_lock = threading.Lock()

    def download(row: dict) -> None:
        nonlocal skipped, downloaded
        # Con el circuito abierto o en apagado, drena la cola sin llamar al API
        if shutdown_event.is_set() or (breaker and breaker.is_open()):
            raise CircuitOpen()
        rel = resolve_target_rel(row, duplicate_keys)
        if storage.exists(rel) and not overwrite:
            with counters_lock:
                skipped += 1
                done_rels.add(rel)
            jobs.increment(job_id, "skipped_existing")
            return
        client.stream_binary(row[HREF_COLUMN].strip(), storage.sink(rel))
        if breaker:
            breaker.record_success()
        with counters_lock:
            downloaded += 1
            done_rels.add(rel)
        jobs.increment(job_id, "downloaded")

    with ThreadPoolExecutor(max_workers=max_workers or config.get_max_workers()) as pool:
        futures = {pool.submit(download, row): row for row in entries}
        for future in as_completed(futures):
            if shutdown_event.is_set() or (breaker and breaker.is_open()):
                pool.shutdown(wait=False, cancel_futures=True)
                break
            row = futures[future]
            try:
                future.result()
            except CircuitOpen:
                continue  # descarga no intentada; queda pendiente para la proxima corrida
            except Exception as exc:
                reference = (row.get(REFERENCE_COLUMN) or "").strip()
                logger.error("Error descargando adjunto de SR %s (%s): %s", reference, row.get("FileName"), exc)
                errors.append({"srNumber": reference, "fileName": row.get("FileName"), "error": str(exc)})
                jobs.increment(job_id, "download_errors")
                if breaker and is_transient_error(exc):
                    breaker.record_failure()

    if shutdown_event.is_set():
        # Los archivos ya descargados se omiten en la proxima corrida
        raise ShutdownRequested()
    if breaker and breaker.is_open():
        raise CircuitOpen(
            f"{breaker.threshold} fallos transitorios consecutivos (5xx/429/red)"
        )

    # Verificacion: cada adjunto esperado quedo guardado (descargado u omitido
    # por existir). Una subida/escritura que no lanzo excepcion esta confirmada.
    missing = [
        resolve_target_rel(row, duplicate_keys)
        for row in entries
        if resolve_target_rel(row, duplicate_keys) not in done_rels
    ]
    if missing:
        logger.error(
            "Verificacion %s: faltan %d de %d adjuntos en %s (ej.: %s)",
            file_name, len(missing), len(entries), storage.kind, "; ".join(missing[:5]),
        )
    verification = {
        "expected": len(entries),
        "stored": len(entries) - len(missing),
        "missing_count": len(missing),
        "missing_sample": missing[:20],
        "ok": not missing and not errors,
    }

    return {
        "metadata_file": file_name,
        "destination": storage.kind,
        "total_rows": len(entries),
        "downloaded": downloaded,
        "skipped_existing": skipped,
        "errors": errors,
        "verification": verification,
    }


def run_binary_job(
    job_id: str,
    client: OscClient,
    batch: list[str],
    output_folder: str,
    overwrite: bool,
    track_state: bool,
    storage,
    max_workers: int | None = None,
) -> None:
    breaker = CircuitBreaker(config.get_circuit_threshold())
    results: list[dict] = []
    had_errors = False
    try:
        for index, csv_path in enumerate(batch, start=1):
            file_name = os.path.basename(csv_path)
            try:
                result = download_metadata_file(
                    client, csv_path, storage, overwrite, job_id, max_workers, breaker
                )
            except ShutdownRequested:
                logger.info("Job %s interrumpido por apagado del servidor en %s", job_id, file_name)
                jobs.finish(job_id, "interrupted", result={"results": results})
                return
            except CircuitOpen as exc:
                message = (
                    f"Circuit breaker abierto en {file_name}: {exc}. El servicio parece caido "
                    "(mantenimiento?); lo ya descargado queda en disco. Relance el metodo "
                    "cuando el servicio se recupere (verifique con GET /health)."
                )
                logger.error("Job %s: %s", job_id, message)
                jobs.finish(job_id, "interrupted", result={"results": results}, error=message)
                return
            except Exception as exc:
                result = {"metadata_file": file_name, "error": str(exc)}
            finally:
                release_file(output_folder, BINARY_STATE_FILE, file_name)
            results.append(result)

            file_failed = "error" in result or bool(result.get("errors"))
            had_errors = had_errors or file_failed
            if track_state and not file_failed:
                mark_processed(
                    output_folder,
                    BINARY_STATE_FILE,
                    file_name,
                    {
                        "job_id": job_id,
                        "downloaded": result["downloaded"],
                        "skipped_existing": result["skipped_existing"],
                        "total_rows": result["total_rows"],
                    },
                )
            jobs.set_progress(job_id, processed_files=index, current_file=None)
        summary = {
            "files": len(results),
            "expected": sum(r.get("total_rows", 0) for r in results),
            "downloaded": sum(r.get("downloaded", 0) for r in results),
            "skipped_existing": sum(r.get("skipped_existing", 0) for r in results),
            "missing": sum(r.get("verification", {}).get("missing_count", 0) for r in results),
            "all_ok": all(r.get("verification", {}).get("ok", False) for r in results),
        }
        jobs.finish(
            job_id,
            "completed_with_errors" if had_errors else "completed",
            result={"summary": summary, "results": results},
        )
    except Exception as exc:
        logger.exception("Job %s fallo", job_id)
        jobs.finish(job_id, "failed", error=str(exc))
    finally:
        for csv_path in batch:
            release_file(output_folder, BINARY_STATE_FILE, os.path.basename(csv_path))


@app.post("/GetAttachmentBinary")
def get_attachment_binary(request: BinaryRequest):
    effective_workers = request.max_workers or config.get_max_workers()
    client = build_client(pool_size=effective_workers)
    os.makedirs(request.output_folder, exist_ok=True)
    storage = build_storage(request)  # valida credenciales/bucket antes de encolar
    if request.metadata_csv:
        if not os.path.isfile(request.metadata_csv):
            raise HTTPException(status_code=400, detail=f"El archivo de metadatos no existe: {request.metadata_csv}")
        batch = [request.metadata_csv]
        busy = reserve_explicit(batch, request.output_folder, BINARY_STATE_FILE)
        if busy:
            raise HTTPException(
                status_code=409,
                detail=f"Archivos en proceso por otro job: {', '.join(busy)}",
            )
        pending_after = 0
        track_state = False  # un archivo pedido explicitamente se procesa siempre
    else:
        if not os.path.isdir(request.metadata_folder):
            raise HTTPException(status_code=400, detail=f"La carpeta de metadatos no existe: {request.metadata_folder}")
        files = list_csv_files(request.metadata_folder)
        if not files:
            raise HTTPException(status_code=400, detail="La carpeta de metadatos no contiene archivos .csv")
        state = {} if request.force else load_state(request.output_folder, BINARY_STATE_FILE)
        batch, pending_after = reserve_batch(
            files, request.output_folder, BINARY_STATE_FILE, state, request.batch_size
        )
        track_state = True
        if not batch:
            return {
                "job_id": None,
                "message": "No hay archivos pendientes: todos estan registrados en "
                f"{BINARY_STATE_FILE} o en proceso por otro job (use force=true para reprocesar)",
                "total_files": len(files),
            }

    job = jobs.create("GetAttachmentBinary", request.model_dump())
    jobs.set_progress(
        job["job_id"],
        total_files=len(batch),
        processed_files=0,
        pending_after_batch=pending_after,
        downloaded=0,
        skipped_existing=0,
        download_errors=0,
    )
    start_job_thread(
        run_binary_job,
        (job["job_id"], client, batch, request.output_folder, request.overwrite, track_state, storage, effective_workers),
    )
    return {
        "job_id": job["job_id"],
        "status": "running",
        "files_in_batch": [os.path.basename(f) for f in batch],
        "pending_after_batch": pending_after,
        "status_url": f"/jobs/{job['job_id']}",
    }


# ---------------------------------------------------------------------------
# GetMetadataClobAndMessages
# ---------------------------------------------------------------------------

class ClobMessagesRequest(BaseModel):
    input_folder: str
    output_folder: str
    files: Optional[list[str]] = None
    batch_size: int = 10
    force: bool = False
    max_workers: Optional[int] = Field(default=None, ge=1, le=64)
    overwrite_html: bool = False  # true = volver a descargar HTMLs ya existentes


def process_clob_messages_file(
    client: OscClient,
    csv_path: str,
    output_folder: str,
    max_workers: int,
    job_id: str,
    force: bool,
    overwrite_html: bool,
    breaker: CircuitBreaker | None = None,
) -> dict:
    """Por cada SR: consulta CLOB + messages, escribe 2 CSV y baja los HTML.

    Escritura incremental con checkpoint por SR (igual que metadata): si se
    corta, la proxima corrida retoma solo los SR faltantes.
    """
    file_name = os.path.basename(csv_path)
    pairs = read_sr_numbers(csv_path)
    base = os.path.splitext(file_name)[0]
    clob_path = os.path.join(output_folder, f"{base}_clob.csv")
    messages_path = os.path.join(output_folder, f"{base}_messages.csv")
    progress_path = messages_path + ".progress"
    html_root = os.path.join(output_folder, MESSAGE_CONTENT_DIR)

    done: set[str] = set()
    if force:
        for path in (clob_path, messages_path, progress_path):
            if os.path.exists(path):
                os.remove(path)
    elif os.path.isfile(progress_path) and os.path.isfile(messages_path):
        with open(progress_path, encoding="utf-8") as fh:
            done = {line.strip() for line in fh if line.strip()}

    pending = [pair for pair in pairs if pair[1] not in done]
    resuming = bool(done)
    logger.info(
        "Procesando clob+messages %s (%d SR, %d ya hechos, %d pendientes)",
        file_name, len(pairs), len(pairs) - len(pending), len(pending),
    )
    jobs.set_progress(
        job_id, current_file=file_name, current_file_srs=len(pairs), current_file_pending=len(pending)
    )

    errors: list[dict] = []
    write_lock = threading.Lock()
    message_count = 0

    def process_sr(pair: tuple[str, str]):
        sr_id, sr = pair
        if shutdown_event.is_set() or (breaker and breaker.is_open()):
            raise CircuitOpen()
        data = client.get_sr_fields(sr, [*CLOB_FIELDS, "messages"])
        clob_row = {
            REFERENCE_COLUMN: sr,
            CLOB_FIELDS[0]: decode_clob(data.get(CLOB_FIELDS[0])),
            CLOB_FIELDS[1]: decode_clob(data.get(CLOB_FIELDS[1])),
        }
        message_rows: list[dict] = []
        for msg in data.get("messages") or []:
            row = {field: msg.get(field, "") for field in MESSAGE_FIELDS}
            message_id = str(msg.get("MessageId", "")).strip()
            if message_id:
                sr_dir = os.path.join(html_root, sanitize_filename(sr))
                os.makedirs(sr_dir, exist_ok=True)
                html_path = os.path.join(sr_dir, f"{sanitize_filename(message_id)}.html")
                if not (os.path.exists(html_path) and not overwrite_html):
                    client.download_binary(client.message_content_url(sr, message_id), html_path)
                row[MESSAGE_CONTENT_COLUMN] = os.path.relpath(html_path, output_folder)
            else:
                row[MESSAGE_CONTENT_COLUMN] = ""
            message_rows.append(row)
        return pair, clob_row, message_rows

    with open(clob_path, "a" if resuming else "w", newline="", encoding="utf-8-sig") as clob_fh, \
            open(messages_path, "a" if resuming else "w", newline="", encoding="utf-8-sig") as msg_fh, \
            open(progress_path, "a" if resuming else "w", encoding="utf-8") as prog_fh:
        clob_writer = csv.DictWriter(clob_fh, fieldnames=CLOB_OUTPUT_COLUMNS)
        msg_writer = csv.DictWriter(msg_fh, fieldnames=MESSAGE_OUTPUT_COLUMNS)
        if not resuming:
            clob_writer.writeheader()
            msg_writer.writeheader()
            clob_fh.flush()
            msg_fh.flush()

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(process_sr, pair): pair for pair in pending}
            for future in as_completed(futures):
                if shutdown_event.is_set() or (breaker and breaker.is_open()):
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                sr_id, sr = futures[future]
                try:
                    _, clob_row, message_rows = future.result()
                except CircuitOpen:
                    continue
                except Exception as exc:
                    logger.error("Error consultando clob/messages de SR %s: %s", sr, exc)
                    errors.append({"srNumber": sr, "error": str(exc)})
                    jobs.increment(job_id, "sr_errors")
                    if breaker and is_transient_error(exc):
                        breaker.record_failure()
                    continue
                if breaker:
                    breaker.record_success()
                with write_lock:
                    clob_writer.writerow(clob_row)
                    clob_fh.flush()
                    if message_rows:
                        msg_writer.writerows(message_rows)
                        msg_fh.flush()
                    prog_fh.write(sr + "\n")
                    prog_fh.flush()
                    message_count += len(message_rows)
                jobs.increment(job_id, "srs_consulted")

    if shutdown_event.is_set():
        raise ShutdownRequested()
    if breaker and breaker.is_open():
        raise CircuitOpen(f"{breaker.threshold} fallos transitorios consecutivos (5xx/429/red)")

    if not errors:
        os.remove(progress_path)

    consulted = len(pairs) - len(errors)
    verification = {
        "expected_srs": len(pairs),
        "consulted": consulted,
        "failed": len(errors),
        "ok": len(errors) == 0,
    }
    if errors:
        logger.error(
            "Verificacion %s: %d de %d SR fallaron y quedan pendientes de reintento",
            file_name, len(errors), len(pairs),
        )

    logger.info("Generado %s_clob.csv y %s_messages.csv (%d mensajes, %d errores)",
                base, base, message_count, len(errors))
    return {
        "input_file": file_name,
        "clob_file": clob_path,
        "messages_file": messages_path,
        "service_requests": len(pairs),
        "resumed_srs": len(pairs) - len(pending),
        "messages": message_count,
        "errors": errors,
        "verification": verification,
    }


def run_clob_messages_job(
    job_id: str,
    client: OscClient,
    batch: list[str],
    output_folder: str,
    force: bool,
    max_workers: int | None,
    overwrite_html: bool,
) -> None:
    max_workers = max_workers or config.get_max_workers()
    breaker = CircuitBreaker(config.get_circuit_threshold())
    results: list[dict] = []
    had_errors = False
    try:
        for index, csv_path in enumerate(batch, start=1):
            file_name = os.path.basename(csv_path)
            try:
                result = process_clob_messages_file(
                    client, csv_path, output_folder, max_workers, job_id, force, overwrite_html, breaker
                )
            except ShutdownRequested:
                logger.info("Job %s interrumpido por apagado del servidor en %s", job_id, file_name)
                jobs.finish(job_id, "interrupted", result={"results": results})
                return
            except CircuitOpen as exc:
                message = (
                    f"Circuit breaker abierto en {file_name}: {exc}. El servicio parece caido "
                    "(mantenimiento?); el avance quedo en el checkpoint. Relance el metodo "
                    "cuando el servicio se recupere (verifique con GET /health)."
                )
                logger.error("Job %s: %s", job_id, message)
                jobs.finish(job_id, "interrupted", result={"results": results}, error=message)
                return
            except Exception as exc:
                result = {"input_file": file_name, "error": str(exc)}
            finally:
                release_file(output_folder, CLOB_MESSAGES_STATE_FILE, file_name)
            results.append(result)

            file_failed = "error" in result or bool(result.get("errors"))
            had_errors = had_errors or file_failed
            if not file_failed:
                mark_processed(
                    output_folder,
                    CLOB_MESSAGES_STATE_FILE,
                    file_name,
                    {
                        "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "job_id": job_id,
                        "clob_file": result["clob_file"],
                        "messages_file": result["messages_file"],
                        "service_requests": result["service_requests"],
                        "messages": result["messages"],
                    },
                )
            jobs.set_progress(job_id, processed_files=index, current_file=None)
        summary = {
            "files": len(results),
            "expected_srs": sum(r.get("service_requests", 0) for r in results),
            "consulted": sum(r.get("verification", {}).get("consulted", 0) for r in results),
            "failed_srs": sum(len(r.get("errors", [])) for r in results),
            "messages": sum(r.get("messages", 0) for r in results),
            "all_ok": all(r.get("verification", {}).get("ok", False) for r in results),
        }
        jobs.finish(
            job_id,
            "completed_with_errors" if had_errors else "completed",
            result={"summary": summary, "results": results},
        )
    except Exception as exc:
        logger.exception("Job %s fallo", job_id)
        jobs.finish(job_id, "failed", error=str(exc))
    finally:
        for csv_path in batch:
            release_file(output_folder, CLOB_MESSAGES_STATE_FILE, os.path.basename(csv_path))


@app.post("/GetMetadataClobAndMessages")
def get_metadata_clob_and_messages(request: ClobMessagesRequest):
    if not os.path.isdir(request.input_folder):
        raise HTTPException(status_code=400, detail=f"La carpeta de entrada no existe: {request.input_folder}")

    effective_workers = request.max_workers or config.get_max_workers()
    client = build_client(pool_size=effective_workers)
    os.makedirs(request.output_folder, exist_ok=True)
    if request.files:
        batch = [os.path.join(request.input_folder, name) for name in request.files]
        missing = [name for name, path in zip(request.files, batch) if not os.path.isfile(path)]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Archivos no encontrados en {request.input_folder}: {', '.join(missing)}",
            )
        busy = reserve_explicit(batch, request.output_folder, CLOB_MESSAGES_STATE_FILE)
        if busy:
            raise HTTPException(status_code=409, detail=f"Archivos en proceso por otro job: {', '.join(busy)}")
        pending_after = 0
    else:
        input_files = list_csv_files(request.input_folder)
        if not input_files:
            raise HTTPException(status_code=400, detail="La carpeta de entrada no contiene archivos .csv")
        state = {} if request.force else load_state(request.output_folder, CLOB_MESSAGES_STATE_FILE)
        batch, pending_after = reserve_batch(
            input_files, request.output_folder, CLOB_MESSAGES_STATE_FILE, state, request.batch_size
        )
        if not batch:
            return {
                "job_id": None,
                "message": "No hay archivos pendientes: todos estan registrados en "
                f"{CLOB_MESSAGES_STATE_FILE} o en proceso por otro job (use force=true para reprocesar)",
                "total_files": len(input_files),
            }

    job = jobs.create("GetMetadataClobAndMessages", request.model_dump())
    jobs.set_progress(
        job["job_id"],
        total_files=len(batch),
        processed_files=0,
        pending_after_batch=pending_after,
        srs_consulted=0,
        sr_errors=0,
    )
    start_job_thread(
        run_clob_messages_job,
        (job["job_id"], client, batch, request.output_folder, request.force, effective_workers, request.overwrite_html),
    )
    return {
        "job_id": job["job_id"],
        "status": "running",
        "files_in_batch": [os.path.basename(f) for f in batch],
        "pending_after_batch": pending_after,
        "status_url": f"/jobs/{job['job_id']}",
    }


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Verifica con una llamada minima si el API de Oracle responde.

    Util para saber si un mantenimiento termino antes de relanzar los jobs.
    No usa reintentos: responde rapido con el estado actual del servicio.
    """
    client = build_client()
    start = time.monotonic()
    try:
        response = client.session.get(
            f"{client.base_url}/serviceRequests",
            params={"limit": 1, "fields": "SrNumber", "onlyData": "true"},
            timeout=15,
        )
        return {
            "oracle_ok": response.status_code == 200,
            "status_code": response.status_code,
            "elapsed_ms": int((time.monotonic() - start) * 1000),
        }
    except requests.RequestException as exc:
        return {
            "oracle_ok": False,
            "error": str(exc),
            "elapsed_ms": int((time.monotonic() - start) * 1000),
        }


# ---------------------------------------------------------------------------
# Consulta puntual de un SR (validaciones ad-hoc)
# ---------------------------------------------------------------------------

@app.get("/attachments/{sr_number}")
def get_attachments_by_sr(sr_number: str):
    """Consulta en vivo los adjuntos de un Reference Number, sin archivos.

    Pensado para validaciones puntuales: se envia el srNumber directamente y
    devuelve los mismos campos que el CSV de metadatos mas el FileContentsHref.
    No hace falta saber en que archivo esta el SR.
    """
    client = build_client()
    try:
        items = client.get_attachments(sr_number)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Error consultando SR {sr_number}: {exc}")
    attachments = []
    for item in items:
        row = {field: item.get(field, "") for field in METADATA_FIELDS}
        row[HREF_COLUMN] = get_file_contents_href(item)
        attachments.append(row)
    return {"srNumber": sr_number, "count": len(attachments), "attachments": attachments}


# ---------------------------------------------------------------------------
# Consulta de jobs
# ---------------------------------------------------------------------------

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"No existe el job {job_id}")
    return job


@app.get("/jobs")
def list_jobs(limit: int = 20):
    return {"jobs": jobs.list(limit=limit)}

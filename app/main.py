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

import csv
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, model_validator

from app import config
from app.jobs import JobManager
from app.osc_client import METADATA_FIELDS, OscClient, get_file_contents_href

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="down-attach-osc",
    description="Descarga de metadatos y binarios de adjuntos de solicitudes de servicio (Oracle Service Cloud)",
)

jobs = JobManager()

SR_ID_COLUMN = "Service Request ID"
REFERENCE_COLUMN = "Reference Number"
HREF_COLUMN = "FileContentsHref"
OUTPUT_COLUMNS = [SR_ID_COLUMN, REFERENCE_COLUMN, *METADATA_FIELDS, HREF_COLUMN]

METADATA_STATE_FILE = "_processed_files.json"
BINARY_STATE_FILE = "_downloaded_files.json"

INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

_state_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def build_client() -> OscClient:
    try:
        return OscClient(
            domain=config.get_domain(),
            username=config.get_username(),
            password=config.get_password(),
            timeout=config.get_timeout(),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


def sanitize_filename(name: str) -> str:
    return INVALID_FILENAME_CHARS.sub("_", name).strip() or "sin_nombre"


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


def select_batch(files: list[str], state: dict, batch_size: int) -> tuple[list[str], int]:
    """Filtra los archivos ya procesados y devuelve (lote, pendientes restantes)."""
    pending = [f for f in files if os.path.basename(f) not in state]
    batch = pending[: batch_size] if batch_size > 0 else pending
    return batch, len(pending) - len(batch)


# ---------------------------------------------------------------------------
# GetMetadataAttachments
# ---------------------------------------------------------------------------

class MetadataRequest(BaseModel):
    input_folder: str
    output_folder: str
    batch_size: int = 10  # 0 = procesar todos los pendientes
    force: bool = False  # true = reprocesar aunque esten en _processed_files.json


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
    client: OscClient, csv_path: str, output_folder: str, max_workers: int, job_id: str
) -> dict:
    """Consulta los adjuntos de cada SR del archivo y escribe el CSV de metadatos."""
    file_name = os.path.basename(csv_path)
    pairs = read_sr_numbers(csv_path)
    logger.info("Procesando %s (%d solicitudes)", file_name, len(pairs))
    jobs.set_progress(job_id, current_file=file_name, current_file_srs=len(pairs))

    rows: list[dict] = []
    errors: list[dict] = []

    def fetch(pair: tuple[str, str]) -> tuple[tuple[str, str], list[dict]]:
        return pair, client.get_attachments(pair[1])

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch, pair): pair for pair in pairs}
        for future in as_completed(futures):
            sr_id, reference = futures[future]
            try:
                _, items = future.result()
            except Exception as exc:
                logger.error("Error consultando adjuntos de SR %s: %s", reference, exc)
                errors.append({"srNumber": reference, "error": str(exc)})
                jobs.increment(job_id, "sr_errors")
                continue
            jobs.increment(job_id, "srs_consulted")
            for item in items:
                row = {SR_ID_COLUMN: sr_id, REFERENCE_COLUMN: reference}
                for field in METADATA_FIELDS:
                    row[field] = item.get(field, "")
                row[HREF_COLUMN] = get_file_contents_href(item)
                rows.append(row)

    output_name = f"{os.path.splitext(file_name)[0]}_attachments.csv"
    output_path = os.path.join(output_folder, output_name)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Generado %s (%d adjuntos, %d errores)", output_name, len(rows), len(errors))
    return {
        "input_file": file_name,
        "output_file": output_path,
        "service_requests": len(pairs),
        "attachments": len(rows),
        "errors": errors,
    }


def run_metadata_job(job_id: str, client: OscClient, batch: list[str], output_folder: str) -> None:
    max_workers = config.get_max_workers()
    results: list[dict] = []
    had_errors = False
    try:
        for index, csv_path in enumerate(batch, start=1):
            file_name = os.path.basename(csv_path)
            try:
                result = process_input_file(client, csv_path, output_folder, max_workers, job_id)
            except Exception as exc:
                result = {"input_file": file_name, "error": str(exc)}
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
        jobs.finish(
            job_id,
            "completed_with_errors" if had_errors else "completed",
            result={"results": results},
        )
    except Exception as exc:
        logger.exception("Job %s fallo", job_id)
        jobs.finish(job_id, "failed", error=str(exc))


@app.post("/GetMetadataAttachments")
def get_metadata_attachments(request: MetadataRequest):
    if not os.path.isdir(request.input_folder):
        raise HTTPException(status_code=400, detail=f"La carpeta de entrada no existe: {request.input_folder}")
    input_files = list_csv_files(request.input_folder)
    if not input_files:
        raise HTTPException(status_code=400, detail="La carpeta de entrada no contiene archivos .csv")

    os.makedirs(request.output_folder, exist_ok=True)
    state = {} if request.force else load_state(request.output_folder, METADATA_STATE_FILE)
    batch, pending_after = select_batch(input_files, state, request.batch_size)
    if not batch:
        return {
            "job_id": None,
            "message": "No hay archivos pendientes: todos estan registrados en "
            f"{METADATA_STATE_FILE} (use force=true para reprocesar)",
            "total_files": len(input_files),
        }

    client = build_client()
    job = jobs.create("GetMetadataAttachments", request.model_dump())
    jobs.set_progress(
        job["job_id"],
        total_files=len(batch),
        processed_files=0,
        pending_after_batch=pending_after,
        srs_consulted=0,
        sr_errors=0,
    )
    threading.Thread(
        target=run_metadata_job,
        args=(job["job_id"], client, batch, request.output_folder),
        daemon=True,
    ).start()
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
    output_folder: str
    batch_size: int = 10  # solo aplica con metadata_folder; 0 = todos
    overwrite: bool = False
    force: bool = False  # true = ignorar _downloaded_files.json

    @model_validator(mode="after")
    def _exactly_one_source(self):
        if bool(self.metadata_csv) == bool(self.metadata_folder):
            raise ValueError("Debe indicar metadata_csv o metadata_folder (solo uno)")
        return self


def download_metadata_file(
    client: OscClient, csv_path: str, output_folder: str, overwrite: bool, job_id: str
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

    logger.info("Descargando %s (%d adjuntos)", file_name, len(entries))
    jobs.set_progress(job_id, current_file=file_name, current_file_rows=len(entries))

    skipped = 0
    downloaded = 0
    errors: list[dict] = []
    counters_lock = threading.Lock()

    def download(row: dict) -> None:
        nonlocal skipped, downloaded
        reference = (row.get(REFERENCE_COLUMN) or "").strip()
        name = sanitize_filename(row.get("FileName") or row.get("Title") or "adjunto")
        # Cada SR tiene su propia subcarpeta para evitar colisiones de nombres
        target_dir = os.path.join(output_folder, sanitize_filename(reference) or "sin_sr")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, name)
        if os.path.exists(target_path) and not overwrite:
            with counters_lock:
                skipped += 1
            jobs.increment(job_id, "skipped_existing")
            return
        client.download_binary(row[HREF_COLUMN].strip(), target_path)
        with counters_lock:
            downloaded += 1
        jobs.increment(job_id, "downloaded")

    with ThreadPoolExecutor(max_workers=config.get_max_workers()) as pool:
        futures = {pool.submit(download, row): row for row in entries}
        for future in as_completed(futures):
            row = futures[future]
            try:
                future.result()
            except Exception as exc:
                reference = (row.get(REFERENCE_COLUMN) or "").strip()
                logger.error("Error descargando adjunto de SR %s (%s): %s", reference, row.get("FileName"), exc)
                errors.append({"srNumber": reference, "fileName": row.get("FileName"), "error": str(exc)})
                jobs.increment(job_id, "download_errors")

    return {
        "metadata_file": file_name,
        "total_rows": len(entries),
        "downloaded": downloaded,
        "skipped_existing": skipped,
        "errors": errors,
    }


def run_binary_job(
    job_id: str,
    client: OscClient,
    batch: list[str],
    output_folder: str,
    overwrite: bool,
    track_state: bool,
) -> None:
    results: list[dict] = []
    had_errors = False
    try:
        for index, csv_path in enumerate(batch, start=1):
            file_name = os.path.basename(csv_path)
            try:
                result = download_metadata_file(client, csv_path, output_folder, overwrite, job_id)
            except Exception as exc:
                result = {"metadata_file": file_name, "error": str(exc)}
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
        jobs.finish(
            job_id,
            "completed_with_errors" if had_errors else "completed",
            result={"results": results},
        )
    except Exception as exc:
        logger.exception("Job %s fallo", job_id)
        jobs.finish(job_id, "failed", error=str(exc))


@app.post("/GetAttachmentBinary")
def get_attachment_binary(request: BinaryRequest):
    if request.metadata_csv:
        if not os.path.isfile(request.metadata_csv):
            raise HTTPException(status_code=400, detail=f"El archivo de metadatos no existe: {request.metadata_csv}")
        batch = [request.metadata_csv]
        pending_after = 0
        track_state = False  # un archivo pedido explicitamente se procesa siempre
    else:
        if not os.path.isdir(request.metadata_folder):
            raise HTTPException(status_code=400, detail=f"La carpeta de metadatos no existe: {request.metadata_folder}")
        files = list_csv_files(request.metadata_folder)
        if not files:
            raise HTTPException(status_code=400, detail="La carpeta de metadatos no contiene archivos .csv")
        os.makedirs(request.output_folder, exist_ok=True)
        state = {} if request.force else load_state(request.output_folder, BINARY_STATE_FILE)
        batch, pending_after = select_batch(files, state, request.batch_size)
        track_state = True
        if not batch:
            return {
                "job_id": None,
                "message": "No hay archivos pendientes: todos estan registrados en "
                f"{BINARY_STATE_FILE} (use force=true para reprocesar)",
                "total_files": len(files),
            }

    os.makedirs(request.output_folder, exist_ok=True)
    client = build_client()
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
    threading.Thread(
        target=run_binary_job,
        args=(job["job_id"], client, batch, request.output_folder, request.overwrite, track_state),
        daemon=True,
    ).start()
    return {
        "job_id": job["job_id"],
        "status": "running",
        "files_in_batch": [os.path.basename(f) for f in batch],
        "pending_after_batch": pending_after,
        "status_url": f"/jobs/{job['job_id']}",
    }


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

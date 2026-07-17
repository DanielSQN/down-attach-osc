"""API para descargar metadatos y binarios de adjuntos de Oracle Service Cloud.

Metodos:
- POST /GetMetadataAttachments: lee una carpeta con archivos ServiceRequest_*.csv
  (columnas "Service Request ID","Reference Number"), consulta los adjuntos de
  cada Reference Number y genera un CSV de metadatos por archivo de entrada.
- POST /GetAttachmentBinary: lee un CSV de metadatos generado por el metodo
  anterior y descarga el binario de cada adjunto usando el href de FileContents.
"""

import csv
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app import config
from app.osc_client import METADATA_FIELDS, OscClient, get_file_contents_href

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="down-attach-osc",
    description="Descarga de metadatos y binarios de adjuntos de solicitudes de servicio (Oracle Service Cloud)",
)

SR_ID_COLUMN = "Service Request ID"
REFERENCE_COLUMN = "Reference Number"
HREF_COLUMN = "FileContentsHref"
OUTPUT_COLUMNS = [SR_ID_COLUMN, REFERENCE_COLUMN, *METADATA_FIELDS, HREF_COLUMN]

INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


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


class MetadataRequest(BaseModel):
    input_folder: str
    output_folder: str


class BinaryRequest(BaseModel):
    metadata_csv: str
    output_folder: str
    overwrite: bool = False


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


def process_input_file(client: OscClient, csv_path: str, output_folder: str, max_workers: int) -> dict:
    """Consulta los adjuntos de cada SR del archivo y escribe el CSV de metadatos."""
    file_name = os.path.basename(csv_path)
    pairs = read_sr_numbers(csv_path)
    logger.info("Procesando %s (%d solicitudes)", file_name, len(pairs))

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
                continue
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


@app.post("/GetMetadataAttachments")
def get_metadata_attachments(request: MetadataRequest):
    if not os.path.isdir(request.input_folder):
        raise HTTPException(status_code=400, detail=f"La carpeta de entrada no existe: {request.input_folder}")
    input_files = sorted(
        os.path.join(request.input_folder, name)
        for name in os.listdir(request.input_folder)
        if name.lower().endswith(".csv")
    )
    if not input_files:
        raise HTTPException(status_code=400, detail="La carpeta de entrada no contiene archivos .csv")

    os.makedirs(request.output_folder, exist_ok=True)
    client = build_client()
    max_workers = config.get_max_workers()

    results = []
    for csv_path in input_files:
        try:
            results.append(process_input_file(client, csv_path, request.output_folder, max_workers))
        except ValueError as exc:
            results.append({"input_file": os.path.basename(csv_path), "error": str(exc)})

    return {"processed_files": len(results), "results": results}


@app.post("/GetAttachmentBinary")
def get_attachment_binary(request: BinaryRequest):
    if not os.path.isfile(request.metadata_csv):
        raise HTTPException(status_code=400, detail=f"El archivo de metadatos no existe: {request.metadata_csv}")

    with open(request.metadata_csv, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or HREF_COLUMN not in reader.fieldnames:
            raise HTTPException(
                status_code=400,
                detail=f"El CSV no tiene la columna '{HREF_COLUMN}' (debe ser generado por GetMetadataAttachments)",
            )
        entries = [row for row in reader if (row.get(HREF_COLUMN) or "").strip()]

    os.makedirs(request.output_folder, exist_ok=True)
    client = build_client()

    downloaded: list[str] = []
    skipped: list[str] = []
    errors: list[dict] = []

    def download(row: dict) -> None:
        reference = (row.get(REFERENCE_COLUMN) or "").strip()
        file_name = sanitize_filename(row.get("FileName") or row.get("Title") or "adjunto")
        # Cada SR tiene su propia subcarpeta para evitar colisiones de nombres
        target_dir = os.path.join(request.output_folder, sanitize_filename(reference) or "sin_sr")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, file_name)
        if os.path.exists(target_path) and not request.overwrite:
            skipped.append(target_path)
            return
        client.download_binary(row[HREF_COLUMN].strip(), target_path)
        downloaded.append(target_path)
        logger.info("Descargado %s", target_path)

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

    return {
        "total_rows": len(entries),
        "downloaded": len(downloaded),
        "skipped_existing": len(skipped),
        "errors": errors,
    }

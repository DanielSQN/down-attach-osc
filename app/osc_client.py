"""Cliente REST para Oracle Service Cloud / Fusion CRM (autenticacion Basic)."""

import logging
import random
import threading
import time

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

API_PATH = "/crmRestApi/resources/11.13.18.05"

# Codigos que se consideran transitorios y se reintentan automaticamente.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Campos de metadatos que se guardan en el CSV de salida
METADATA_FIELDS = [
    "AttachedDocumentId",
    "DatatypeCode",
    "FileName",
    "DmDocumentId",
    "UploadedFileContentType",
    "UploadedFileLength",
    "Title",
    "CreationDate",
    "CreatedBy",
]


class OscClient:
    def __init__(
        self,
        domain: str,
        username: str,
        password: str,
        timeout: int = 60,
        max_retries: int = 4,
        backoff: float = 1.0,
        abort_event: threading.Event | None = None,
        pool_size: int = 10,
    ):
        self.base_url = f"https://{domain}{API_PATH}"
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.abort_event = abort_event
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.headers.update({"Accept": "application/json"})
        # Pool de conexiones dimensionado a los workers: evita abrir/cerrar
        # conexiones TLS constantemente cuando hay mas workers que el default (10)
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=max(pool_size, 10))
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # ------------------------------------------------------------------
    # Reintentos con backoff exponencial para errores transitorios
    # ------------------------------------------------------------------

    def _aborted(self) -> bool:
        return self.abort_event is not None and self.abort_event.is_set()

    def _sleep_backoff(self, attempt: int, retry_after: str | None) -> None:
        # Respeta Retry-After (segundos) si el servidor lo envia; si no, backoff
        # exponencial con jitter: 1s, 2s, 4s, 8s... + aleatorio.
        if retry_after and retry_after.isdigit():
            delay = float(retry_after)
        else:
            delay = self.backoff * (2 ** attempt) + random.uniform(0, 0.5)
        # Duerme en tramos cortos para reaccionar rapido a un apagado (Ctrl+C)
        end = time.monotonic() + delay
        while time.monotonic() < end:
            if self._aborted():
                return
            time.sleep(min(0.2, end - time.monotonic()))

    def _get_with_retry(self, url: str, **kwargs) -> requests.Response:
        """GET reintentando ante 5xx/429 y errores de red/timeout."""
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            if self._aborted():
                raise RuntimeError("Cancelado por apagado del servidor")
            retry_after = None
            try:
                response = self.session.get(url, timeout=self.timeout, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
            else:
                if response.status_code not in RETRYABLE_STATUS:
                    response.raise_for_status()
                    return response
                last_exc = requests.HTTPError(
                    f"{response.status_code} Server Error para {url}", response=response
                )
                retry_after = response.headers.get("Retry-After")
                response.close()
            if attempt < self.max_retries:
                logger.warning(
                    "Reintento %d/%d tras error transitorio en %s: %s",
                    attempt + 1, self.max_retries, url, last_exc,
                )
                self._sleep_backoff(attempt, retry_after)
        raise last_exc

    # ------------------------------------------------------------------
    # Operaciones de negocio
    # ------------------------------------------------------------------

    def get_sr_fields(self, sr_number: str, fields: list[str]) -> dict:
        """Consulta campos puntuales de una solicitud (?fields=...&onlyData=true).

        Devuelve el objeto de la solicitud con esos campos (p. ej. los CLOB
        arin_comentarios_cifrado_c, col_tex_plantilla_c y el array messages).
        """
        url = f"{self.base_url}/serviceRequests/{sr_number}"
        params = {"fields": ",".join(fields), "onlyData": "true"}
        response = self._get_with_retry(url, params=params)
        return response.json()

    def message_content_url(self, sr_number: str, message_id: str) -> str:
        """URL del enclosure con el HTML del contenido de un mensaje."""
        return (
            f"{self.base_url}/serviceRequests/{sr_number}"
            f"/child/messages/{message_id}/enclosure/MessageContent"
        )

    def get_attachments(self, sr_number: str) -> list[dict]:
        """Devuelve todos los adjuntos de una solicitud, paginando con offset.

        No se envia limit (por defecto el API usa 25); mientras hasMore sea
        true se vuelve a llamar incrementando el offset. Cada pagina se
        reintenta automaticamente ante errores transitorios (5xx/429/red).
        """
        url = f"{self.base_url}/serviceRequests/{sr_number}/child/Attachment"
        items: list[dict] = []
        offset = 0
        while True:
            params = {"offset": offset} if offset else None
            response = self._get_with_retry(url, params=params)
            data = response.json()
            page_items = data.get("items", [])
            items.extend(page_items)
            if not data.get("hasMore"):
                break
            offset += len(page_items) if page_items else data.get("limit", 25)
        return items

    def stream_binary(self, href: str, sink) -> None:
        """Trae el binario del enclosure y se lo pasa a `sink(response)`.

        Se sobreescribe el Accept de la sesion (application/json) porque el
        enclosure devuelve un binario y Oracle responde 406 si se pide JSON.
        Se reintenta ante errores transitorios; cada intento reabre el stream
        desde cero (el sink debe reescribir el destino, no anexar).
        """
        headers = {"Accept": "*/*"}
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            if self._aborted():
                raise RuntimeError("Cancelado por apagado del servidor")
            retry_after = None
            try:
                with self.session.get(href, headers=headers, timeout=self.timeout, stream=True) as response:
                    if response.status_code in RETRYABLE_STATUS:
                        retry_after = response.headers.get("Retry-After")
                        last_exc = requests.HTTPError(
                            f"{response.status_code} Server Error para {href}", response=response
                        )
                    else:
                        response.raise_for_status()
                        sink(response)
                        return
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
            if attempt < self.max_retries:
                logger.warning(
                    "Reintento %d/%d tras error transitorio descargando %s: %s",
                    attempt + 1, self.max_retries, href, last_exc,
                )
                self._sleep_backoff(attempt, retry_after)
        raise last_exc

    def download_binary(self, href: str, target_path: str) -> None:
        """Descarga el binario del enclosure FileContents a un archivo local."""
        def to_file(response) -> None:
            with open(target_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=1024 * 256):
                    fh.write(chunk)
        self.stream_binary(href, to_file)


def get_file_contents_href(item: dict) -> str:
    """Filtra en links el objeto con rel=enclosure y name=FileContents."""
    for link in item.get("links", []):
        if link.get("rel") == "enclosure" and link.get("name") == "FileContents":
            return link.get("href", "")
    return ""

"""Cliente REST para Oracle Service Cloud / Fusion CRM (autenticacion Basic)."""

import logging

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

API_PATH = "/crmRestApi/resources/11.13.18.05"

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
    def __init__(self, domain: str, username: str, password: str, timeout: int = 60):
        self.base_url = f"https://{domain}{API_PATH}"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)
        self.session.headers.update({"Accept": "application/json"})

    def get_attachments(self, sr_number: str) -> list[dict]:
        """Devuelve todos los adjuntos de una solicitud, paginando con offset.

        No se envia limit (por defecto el API usa 25); mientras hasMore sea
        true se vuelve a llamar incrementando el offset.
        """
        url = f"{self.base_url}/serviceRequests/{sr_number}/child/Attachment"
        items: list[dict] = []
        offset = 0
        while True:
            params = {"offset": offset} if offset else None
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            page_items = data.get("items", [])
            items.extend(page_items)
            if not data.get("hasMore"):
                break
            offset += len(page_items) if page_items else data.get("limit", 25)
        return items

    def download_binary(self, href: str, target_path: str) -> None:
        """Descarga el binario del enclosure FileContents a target_path."""
        with self.session.get(href, timeout=self.timeout, stream=True) as response:
            response.raise_for_status()
            with open(target_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=1024 * 256):
                    fh.write(chunk)


def get_file_contents_href(item: dict) -> str:
    """Filtra en links el objeto con rel=enclosure y name=FileContents."""
    for link in item.get("links", []):
        if link.get("rel") == "enclosure" and link.get("name") == "FileContents":
            return link.get("href", "")
    return ""

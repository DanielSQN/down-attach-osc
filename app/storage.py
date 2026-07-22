"""Backends de almacenamiento para los binarios de adjuntos: disco local o GCP.

Cada backend expone la misma interfaz para que GetAttachmentBinary sea agnostico
del destino:
- exists(rel): si el objeto ya esta (para omitir descargas ya hechas)
- sink(rel): callable que recibe la respuesta HTTP en streaming y la guarda
- location(rel): ruta/URI legible del objeto (para reportes)
- preload(): (opcional) precarga el listado de objetos ya existentes

`rel` es la ruta relativa del objeto, p. ej. "0002859140/documento.pdf".
"""

import logging
import os

logger = logging.getLogger(__name__)

CHUNK = 1024 * 256


class LocalStorage:
    """Guarda los binarios en disco, bajo output_folder."""

    kind = "local"

    def __init__(self, output_folder: str):
        self.root = output_folder

    def _full(self, rel: str) -> str:
        return os.path.join(self.root, *rel.split("/"))

    def preload(self) -> None:
        pass

    def exists(self, rel: str) -> bool:
        return os.path.exists(self._full(rel))

    def sink(self, rel: str):
        path = self._full(rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        def _write(response) -> None:
            with open(path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=CHUNK):
                    fh.write(chunk)

        return _write

    def location(self, rel: str) -> str:
        return self._full(rel)


class GcpStorage:
    """Sube los binarios a un bucket de GCP usando una cuenta de servicio."""

    kind = "gcp"

    def __init__(self, bucket: str, prefix: str = "", credentials_file: str = ""):
        # Import perezoso: solo se necesita google-cloud-storage si se usa GCP
        from google.cloud import storage as gcs

        if credentials_file:
            self.client = gcs.Client.from_service_account_json(credentials_file)
        else:
            # Credenciales por defecto (GOOGLE_APPLICATION_CREDENTIALS / ADC)
            self.client = gcs.Client()
        self.bucket = self.client.bucket(bucket)
        self.prefix = (prefix or "").strip("/")
        self._existing: set[str] | None = None

    def _name(self, rel: str) -> str:
        rel = rel.replace("\\", "/")
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def preload(self) -> None:
        # Un solo listado del prefijo (paginado) en vez de un HEAD por objeto;
        # sirve para omitir en la reanudacion lo que ya esta en el bucket.
        names = set()
        for blob in self.client.list_blobs(self.bucket, prefix=self.prefix or None):
            names.add(blob.name)
        self._existing = names
        logger.info("GCP: %d objetos ya presentes bajo gs://%s/%s", len(names), self.bucket.name, self.prefix)

    def exists(self, rel: str) -> bool:
        name = self._name(rel)
        if self._existing is not None:
            return name in self._existing
        return self.bucket.blob(name).exists()

    def sink(self, rel: str):
        blob = self.bucket.blob(self._name(rel))

        def _upload(response) -> None:
            response.raw.decode_content = True
            blob.upload_from_file(response.raw, content_type=response.headers.get("Content-Type"))

        return _upload

    def location(self, rel: str) -> str:
        return f"gs://{self.bucket.name}/{self._name(rel)}"

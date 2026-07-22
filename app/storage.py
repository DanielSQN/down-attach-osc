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
import tempfile

logger = logging.getLogger(__name__)

CHUNK = 1024 * 256
# Buffer de subida a GCP: hasta este tamaño va en memoria; por encima, a disco temporal
SPOOL_MAX = 16 * 1024 * 1024


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
            # Se vuelca el binario a un buffer con tamaño conocido y se sube con
            # size explicito. NO se streamea response.raw: Oracle puede mandar el
            # binario comprimido (gzip), y subir el stream crudo produce un
            # desajuste de tamano (Content-Range) que GCS rechaza con 400.
            # iter_content descomprime como lo haria requests.
            with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX) as buf:
                for chunk in response.iter_content(chunk_size=CHUNK):
                    buf.write(chunk)
                size = buf.tell()
                buf.seek(0)
                blob.upload_from_file(
                    buf, size=size, content_type=response.headers.get("Content-Type")
                )

        return _upload

    def location(self, rel: str) -> str:
        return f"gs://{self.bucket.name}/{self._name(rel)}"

    def check(self, write_test: bool = True) -> dict:
        """Prueba la conexion: autenticacion + listar (+ subir/borrar de prueba).

        Devuelve que verificaciones pasaron y el error donde se detuvo. No lanza.
        """
        import time

        checks: dict = {"auth_list": False, "write": None, "delete": None}
        errors: dict = {}
        try:
            # Listar valida autenticacion, acceso al bucket y permiso de listado
            next(iter(self.client.list_blobs(self.bucket, prefix=self.prefix or None, max_results=1)), None)
            checks["auth_list"] = True
        except Exception as exc:
            errors["auth_list"] = str(exc)
            return {"checks": checks, "errors": errors}
        if write_test:
            name = self._name(f"_healthcheck_{int(time.time() * 1000)}.txt")
            try:
                blob = self.bucket.blob(name)
                blob.upload_from_string(b"ok", content_type="text/plain")
                checks["write"] = True
                try:
                    blob.delete()
                    checks["delete"] = True
                except Exception as exc:
                    # El job no borra; falta de permiso de delete no lo invalida
                    checks["delete"] = False
                    errors["delete"] = str(exc)
            except Exception as exc:
                checks["write"] = False
                errors["write"] = str(exc)
        return {"checks": checks, "errors": errors}

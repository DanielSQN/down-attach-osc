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
import threading
import time

logger = logging.getLogger(__name__)

CHUNK = 1024 * 256
# Buffer de subida a GCP: hasta este tamaño va en memoria; por encima, a disco temporal
SPOOL_MAX = 16 * 1024 * 1024

# Listado de objetos ya presentes en el bucket, compartido por TODOS los jobs
# del proceso y por (bucket, prefix). Con millones de objetos el listado tarda
# minutos: pagarlo una vez por servidor (y no una vez por job) es la diferencia
# entre arrancar en segundos o esperar el listado completo en cada llamada.
_listing_cache: dict[tuple[str, str], set[str]] = {}
_listing_locks: dict[tuple[str, str], threading.Lock] = {}
_listing_guard = threading.Lock()


def _listing_lock_for(key: tuple[str, str]) -> threading.Lock:
    with _listing_guard:
        return _listing_locks.setdefault(key, threading.Lock())


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

    def sink(self, rel: str, content_type: str | None = None):
        path = self._full(rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.part"

        def _write(response) -> None:
            # Se escribe a un .part y se renombra al final: si la descarga se
            # corta, no queda un archivo final incompleto que la reanudacion
            # saltaria como "ya existente".
            try:
                with open(tmp_path, "wb") as fh:
                    for chunk in response.iter_content(chunk_size=CHUNK):
                        fh.write(chunk)
            except BaseException:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
                raise
            os.replace(tmp_path, path)

        return _write

    def location(self, rel: str) -> str:
        return self._full(rel)

    def upload_control(self, name: str, data: bytes, content_type: str = "text/csv") -> str | None:
        # En local los archivos de control ya quedan en output_folder; no se duplican
        return None

    def upload_index(self, name: str, data: bytes, content_type: str = "text/csv") -> str | None:
        # En local el indice ya queda en output_folder; no se duplica
        return None


class GcpStorage:
    """Sube los binarios a un bucket de GCP usando una cuenta de servicio."""

    kind = "gcp"

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        credentials_file: str = "",
        pool_size: int = 10,
        preload_listing: bool = True,
        refresh_listing: bool = False,
    ):
        # Import perezoso: solo se necesita google-cloud-storage si se usa GCP
        from google.cloud import storage as gcs

        if credentials_file:
            self.client = gcs.Client.from_service_account_json(credentials_file)
        else:
            # Credenciales por defecto (GOOGLE_APPLICATION_CREDENTIALS / ADC)
            self.client = gcs.Client()
        # El pool de conexiones de GCS es 10 por defecto; con mas workers en
        # paralelo se descartan conexiones (WARNING "Connection pool is full").
        # Se dimensiona al numero de workers para reutilizarlas.
        try:
            from requests.adapters import HTTPAdapter

            size = max(pool_size, 10)
            adapter = HTTPAdapter(pool_connections=size, pool_maxsize=size)
            self.client._http.mount("https://", adapter)
            self.client._http.mount("http://", adapter)
        except Exception:  # best-effort: si no se puede, solo son warnings de rendimiento
            pass
        self.bucket = self.client.bucket(bucket)
        self.prefix = (prefix or "").strip("/")
        self.preload_listing = preload_listing
        self.refresh_listing = refresh_listing
        self._existing: set[str] | None = None

    def _name(self, rel: str) -> str:
        rel = rel.replace("\\", "/")
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def preload(self) -> None:
        """Carga el listado de objetos ya presentes, para omitir lo ya subido.

        Un solo listado del prefijo (paginado) en vez de un HEAD por objeto, y
        compartido entre todos los jobs del proceso: con millones de objetos el
        listado tarda minutos y repetirlo por job bloquea el arranque. Las
        subidas propias se agregan al set en sink(), asi que sigue al dia.

        Con preload_listing=False no se lista: exists() hace un HEAD por objeto
        (util cuando el prefijo es enorme y el listado tarda mas que las
        comprobaciones puntuales, que ademas corren en paralelo).
        """
        if self._existing is not None:
            return
        if not self.preload_listing:
            logger.info(
                "GCP: sin listado previo (preload_listing=false); se verifica objeto por objeto"
            )
            return
        key = (self.bucket.name, self.prefix)
        # Un lock por (bucket, prefix): si varios jobs arrancan a la vez, solo
        # uno lista y los demas esperan y reutilizan ese mismo set.
        with _listing_lock_for(key):
            cached = _listing_cache.get(key)
            if cached is not None and not self.refresh_listing:
                self._existing = cached
                logger.info(
                    "GCP: %d objetos ya presentes bajo gs://%s/%s (listado en cache del proceso)",
                    len(cached), self.bucket.name, self.prefix,
                )
                return
            start = time.monotonic()
            logger.info(
                "GCP: listando objetos bajo gs://%s/%s (puede tardar varios minutos "
                "con millones de objetos)...", self.bucket.name, self.prefix,
            )
            names: set[str] = set()
            # fields limita la respuesta al nombre: sin esto el API devuelve toda
            # la metadata de cada objeto (tamano, hashes, fechas...), varias veces
            # mas bytes que parsear en un listado de millones.
            blobs = self.client.list_blobs(
                self.bucket,
                prefix=self.prefix or None,
                fields="items(name),nextPageToken",
                page_size=1000,
            )
            for blob in blobs:
                names.add(blob.name)
                if len(names) % 250_000 == 0:
                    logger.info(
                        "GCP: %d objetos listados (%.1f min)...",
                        len(names), (time.monotonic() - start) / 60,
                    )
            _listing_cache[key] = names
            self._existing = names
            logger.info(
                "GCP: %d objetos ya presentes bajo gs://%s/%s (listado en %.1f min)",
                len(names), self.bucket.name, self.prefix, (time.monotonic() - start) / 60,
            )

    def exists(self, rel: str) -> bool:
        name = self._name(rel)
        if self._existing is not None:
            return name in self._existing
        return self.bucket.blob(name).exists()

    def sink(self, rel: str, content_type: str | None = None):
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
                    buf,
                    size=size,
                    # content_type explicito (p. ej. text/html o text/plain para el
                    # contenido de mensajes); si no, el que declare la respuesta
                    content_type=content_type or response.headers.get("Content-Type"),
                )
            if self._existing is not None:
                self._existing.add(blob.name)

        return _upload

    def location(self, rel: str) -> str:
        return f"gs://{self.bucket.name}/{self._name(rel)}"

    def upload_control(self, name: str, data: bytes, content_type: str = "text/csv") -> str | None:
        """Sube un archivo de control al bucket, bajo <prefix>/_control/."""
        obj = f"{self.prefix}/_control/{name}" if self.prefix else f"_control/{name}"
        self.bucket.blob(obj).upload_from_string(data, content_type=content_type)
        if self._existing is not None:
            self._existing.add(obj)
        return f"gs://{self.bucket.name}/{obj}"

    def upload_index(self, name: str, data: bytes, content_type: str = "text/csv") -> str | None:
        """Sube el indice de busqueda (SR -> ruta) al bucket, bajo <prefix>/_index/."""
        obj = f"{self.prefix}/_index/{name}" if self.prefix else f"_index/{name}"
        self.bucket.blob(obj).upload_from_string(data, content_type=content_type)
        if self._existing is not None:
            self._existing.add(obj)
        return f"gs://{self.bucket.name}/{obj}"

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

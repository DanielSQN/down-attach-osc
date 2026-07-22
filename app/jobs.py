"""Registro de trabajos (jobs) en segundo plano, persistido en disco.

Cada job se guarda como jobs/<job_id>.json, por lo que el estatus se puede
consultar incluso despues de reiniciar el servidor.
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class JobManager:
    def __init__(self, jobs_dir: str | None = None):
        self.jobs_dir = jobs_dir or os.getenv("JOBS_DIR", "jobs")
        self._lock = threading.RLock()
        self._jobs: dict[str, dict] = {}
        self._pending_writes: dict[str, int] = {}

    def _path(self, job_id: str) -> str:
        return os.path.join(self.jobs_dir, f"{job_id}.json")

    def _persist(self, job: dict) -> None:
        os.makedirs(self.jobs_dir, exist_ok=True)
        path = self._path(job["job_id"])
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(job, fh, ensure_ascii=False, indent=2)
        # En Windows os.replace falla con "Acceso denegado" (WinError 5) si el
        # destino .json esta abierto por otro hilo (un GET /jobs concurrente) o
        # por el antivirus. Se reintenta unas veces; si aun asi falla, se registra
        # y NO se propaga: persistir el estado es bookkeeping y jamas debe marcar
        # una descarga como fallida.
        for _ in range(10):
            try:
                os.replace(tmp, path)
                return
            except PermissionError:
                time.sleep(0.05)
        try:
            os.replace(tmp, path)
        except OSError as exc:
            logger.warning("No se pudo persistir el job %s: %s", job.get("job_id"), exc)
            try:
                os.remove(tmp)
            except OSError:
                pass

    def create(self, job_type: str, params: dict) -> dict:
        job = {
            "job_id": uuid.uuid4().hex[:12],
            "type": job_type,
            "status": "running",
            "created_at": _now(),
            "finished_at": None,
            "params": params,
            "progress": {},
            "result": None,
            "error": None,
        }
        with self._lock:
            self._jobs[job["job_id"]] = job
            self._persist(job)
        return job

    def set_progress(self, job_id: str, **fields) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["progress"].update(fields)
            self._persist(job)

    def increment(self, job_id: str, field: str, persist_every: int = 25) -> None:
        """Incrementa un contador de progreso; escribe a disco cada persist_every."""
        with self._lock:
            job = self._jobs[job_id]
            job["progress"][field] = job["progress"].get(field, 0) + 1
            count = self._pending_writes.get(job_id, 0) + 1
            if count >= persist_every:
                self._persist(job)
                count = 0
            self._pending_writes[job_id] = count

    def finish(self, job_id: str, status: str, result: dict | None = None, error: str | None = None) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["status"] = status
            job["finished_at"] = _now()
            job["result"] = result
            job["error"] = error
            self._persist(job)

    def _mark_if_stale(self, job: dict) -> dict:
        # Un job "running" que no esta en memoria quedo cortado por un reinicio
        with self._lock:
            in_memory = job.get("job_id") in self._jobs
        if job.get("status") == "running" and not in_memory:
            job["status"] = "interrupted"
        return job

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            if job_id in self._jobs:
                return dict(self._jobs[job_id])
        path = self._path(job_id)
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                return self._mark_if_stale(json.load(fh))
        return None

    def list(self, limit: int = 20) -> list[dict]:
        jobs_by_id: dict[str, dict] = {}
        if os.path.isdir(self.jobs_dir):
            for name in os.listdir(self.jobs_dir):
                if not name.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(self.jobs_dir, name), encoding="utf-8") as fh:
                        job = self._mark_if_stale(json.load(fh))
                    jobs_by_id[job["job_id"]] = job
                except (json.JSONDecodeError, KeyError, OSError):
                    continue
        with self._lock:
            jobs_by_id.update({k: dict(v) for k, v in self._jobs.items()})
        ordered = sorted(jobs_by_id.values(), key=lambda j: j.get("created_at", ""), reverse=True)
        return [
            {key: job.get(key) for key in ("job_id", "type", "status", "created_at", "finished_at", "progress")}
            for job in ordered[:limit]
        ]

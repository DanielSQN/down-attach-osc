import os

from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Falta la variable {name} en el archivo .env "
            "(copie .env.example a .env y complete los valores)"
        )
    return value


def get_domain() -> str:
    domain = _required("OSC_DOMAIN")
    # Acepta el dominio con o sin https://
    return domain.removeprefix("https://").removeprefix("http://").rstrip("/")


def get_username() -> str:
    return _required("OSC_USERNAME")


def get_password() -> str:
    return _required("OSC_PASSWORD")


def get_max_workers() -> int:
    return int(os.getenv("OSC_MAX_WORKERS", "5"))


def get_timeout() -> int:
    return int(os.getenv("OSC_TIMEOUT", "60"))


def get_error_log_file() -> str:
    return os.getenv("ERROR_LOG_FILE", "errors.log")


def get_max_retries() -> int:
    return int(os.getenv("OSC_MAX_RETRIES", "4"))


def get_retry_backoff() -> float:
    return float(os.getenv("OSC_RETRY_BACKOFF", "1.0"))


def get_circuit_threshold() -> int:
    """Fallos transitorios consecutivos que abren el circuit breaker (0 = desactivado)."""
    return int(os.getenv("OSC_CIRCUIT_THRESHOLD", "10"))


def get_gcp_service_account_file() -> str:
    """Ruta al JSON de la cuenta de servicio de GCP (vacio = credenciales por defecto)."""
    return os.getenv("GCP_SERVICE_ACCOUNT_FILE", "").strip()

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

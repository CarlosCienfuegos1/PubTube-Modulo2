import os
from dotenv import load_dotenv

# Cargar variables del archivo .env local (ignorado por git)
load_dotenv()


def _require(name: str) -> str:
    """Lee una variable de entorno obligatoria. Falla descriptivamente si no existe."""
    value = os.getenv(name)
    if not value:
        raise KeyError(
            f"La variable de entorno '{name}' no está definida.\n"
            f"Crea un archivo '.env' en la raíz del proyecto copiando '.env.example':\n"
            f"  cp .env.example .env\n"
            f"Luego define el valor de '{name}' en ese archivo."
        )
    return value.strip()


RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER: str = _require("RABBITMQ_USER")
RABBITMQ_PASS: str = _require("RABBITMQ_PASS")

POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB: str = _require("POSTGRES_DB")
POSTGRES_USER: str = _require("POSTGRES_USER")
POSTGRES_PASSWORD: str = _require("POSTGRES_PASSWORD")

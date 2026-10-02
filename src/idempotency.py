"""
idempotency.py — Repositorio de idempotencia atómica (US-B4a).

Mantiene la tabla `processed_events` en PostgreSQL para garantizar que
dos copias del mismo evento —incluso si llegan de forma concurrente—
no puedan registrarse dos veces para el mismo consumidor.

La garantía se apoya en:
  1. PRIMARY KEY (event_id, consumer_id) → restricción única en BD.
  2. INSERT ... ON CONFLICT DO NOTHING → operación atómica en una sola
     sentencia SQL; PostgreSQL adquiere el lock a nivel de fila antes
     de que otra transacción pueda interferir.
  3. Captura defensiva de UniqueViolation por si se usara inserción
     directa sin ON CONFLICT (compatibilidad con extensiones futuras).

Uso:
    from idempotency import init_idempotency_db, try_register_event

    init_idempotency_db()                   # una vez al arrancar
    new = try_register_event("q.m4.experience", "550e8400-...")
    if new:
        # procesar el evento
        ...
    else:
        # duplicado → hacer ack y descartar
        ...
"""
import logging

import psycopg
import psycopg.errors

logger = logging.getLogger(__name__)


# ─── DDL ─────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        TEXT        NOT NULL,
    consumer_id     TEXT        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_id)
);
"""
# Indice adicional para consultas solo por event_id (sin filtrar consumer_id).
_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_processed_events_event_id
    ON processed_events (event_id);
"""


# ─── Conexion ─────────────────────────────────────────────────────────────────

def _get_connection() -> psycopg.Connection:
    """Abre y retorna una conexion fresca a PostgreSQL."""
    from config import (  # import lazy: solo se evalua al conectar, no al importar el modulo
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
    )
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ─── Inicializacion ───────────────────────────────────────────────────────────

def init_idempotency_db() -> None:
    """
    Crea la tabla `processed_events` y sus indices si no existen.
    Idempotente: puede llamarse multiples veces sin efecto secundario.
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
            cur.execute(_CREATE_INDEX_SQL)
        conn.commit()
    logger.info("[idempotency] Tabla 'processed_events' lista.")


# ─── Repositorio ─────────────────────────────────────────────────────────────

_INSERT_SQL = """
INSERT INTO processed_events (event_id, consumer_id, processed_at)
VALUES (%s, %s, now())
ON CONFLICT (event_id, consumer_id) DO NOTHING;
"""


def try_register_event(consumer_id: str, event_id: str) -> bool:
    """
    Intenta registrar atomicamente el par (event_id, consumer_id).

    Utiliza INSERT ... ON CONFLICT DO NOTHING para que la restriccion de
    clave primaria de la base de datos sea el arbitro ante llamadas
    concurrentes: PostgreSQL garantiza que, de N hilos que arriben al
    mismo milisegundo, exactamente uno lograra insertar la fila;
    los demas obtendran rowcount == 0.

    Args:
        consumer_id: Identificador del consumidor/cola
                     (ej. "q.m4.experience", "q.m3.publish").
        event_id:    Identificador unico del evento (UUID como string).

    Returns:
        True  si el evento es nuevo para este consumidor; debe procesarse.
        False si el evento ya fue registrado; debe descartarse.

    Raises:
        ValueError: si consumer_id o event_id son vacios.
        psycopg.OperationalError: si no se puede conectar a PostgreSQL.
    """
    if not consumer_id or not event_id:
        raise ValueError("consumer_id y event_id no pueden ser vacios.")

    try:
        with _get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_INSERT_SQL, (event_id, consumer_id))
                inserted = cur.rowcount > 0
            conn.commit()
        return inserted

    except psycopg.errors.UniqueViolation:
        # Ruta defensiva: captura limpiamente la violacion de clave unica
        # si se llegara a ejecutar sin ON CONFLICT y retorna False.
        logger.debug(
            "[idempotency] UniqueViolation capturada para "
            "event_id=%s consumer_id=%s — descartando duplicado.",
            event_id, consumer_id,
        )
        return False

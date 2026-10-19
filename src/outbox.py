"""
outbox.py — Patrón Outbox Transaccional: persistencia atómica de eventos (US-B5a).

Este módulo resuelve el problema de la "Doble Escritura" (Dual-Write Problem)
en arquitecturas orientadas a eventos:
    Si una aplicación guarda un cambio de negocio en la base de datos y luego
    intenta publicar el evento en RabbitMQ por separado, una caída de red o del
    proceso puede dejar la base de datos con el cambio pero el evento nunca emitido
    (o viceversa si se publica antes).

Solución (Transactional Outbox):
    El evento a emitir se persiste en la tabla `outbox` dentro de la MISMA
    transacción ACID que el cambio de negocio que lo origina:
        BEGIN;
            -- 1. Operación de negocio (ej: INSERT INTO videos ...)
            -- 2. Registro del evento en la outbox (save_to_outbox)
        COMMIT;

    - Si la transacción tiene éxito: el cambio de negocio y el evento pendiente
      quedan confirmados juntos.
    - Si la transacción falla o se hace rollback: el evento pendiente se revierte
      automáticamente, garantizando que NUNCA queden eventos huérfanos.
    - En una etapa posterior (US-B5b), un worker/relay leerá los eventos pendientes
      de la tabla `outbox` y los publicará de forma confiable a RabbitMQ.
"""

from contextlib import contextmanager
import logging
from typing import Any, Dict, Generator, List, Optional
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from envelope import EventEnvelope

logger = logging.getLogger(__name__)

# ─── DDL de la tabla outbox ───────────────────────────────────────────────────

CREATE_OUTBOX_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    routing_key     TEXT NOT NULL,
    correlation_id  TEXT NOT NULL,
    causation_id    TEXT,
    source          TEXT NOT NULL DEFAULT 'module-2',
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created
    ON outbox (status, created_at)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_outbox_correlation_id
    ON outbox (correlation_id);
"""


# ─── Conexión a PostgreSQL ───────────────────────────────────────────────────

def get_connection() -> psycopg.Connection:
    """
    Abre y retorna una nueva conexión a PostgreSQL usando las credenciales del .env.
    """
    from config import (
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
        connect_timeout=2,
    )


# ─── Inicialización de esquema ───────────────────────────────────────────────

def init_outbox_db(conn: Optional[psycopg.Connection] = None) -> None:
    """
    Crea la tabla `outbox` y sus índices si no existen.
    Idempotente: puede ejecutarse múltiples veces sin errores.

    Args:
        conn: Conexión existente opcional. Si no se provee, se abre una temporal.
    """
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(CREATE_OUTBOX_TABLE_SQL)
        conn.commit()
    else:
        with get_connection() as local_conn:
            with local_conn.cursor() as cur:
                cur.execute(CREATE_OUTBOX_TABLE_SQL)
            local_conn.commit()
    logger.info("[outbox] Tabla 'outbox' e índices listos.")


# ─── Persistencia transaccional de eventos ───────────────────────────────────

_INSERT_OUTBOX_SQL = """
INSERT INTO outbox (
    id,
    event_type,
    routing_key,
    correlation_id,
    causation_id,
    source,
    payload,
    status
) VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
ON CONFLICT (id) DO NOTHING;
"""


def save_to_outbox(
    conn: psycopg.Connection,
    envelope: EventEnvelope,
    routing_key: Optional[str] = None,
) -> str:
    """
    Persiste un EventEnvelope en la tabla `outbox` usando la conexión activa del llamador.

    REGLA ARQUITECTÓNICA FUNDAMENTAL (US-B5a):
        Esta función NUNCA hace `conn.commit()`.
        Se ejecuta dentro de la transacción abierta del llamador (junto con el cambio
        de negocio). El llamador es el único responsable de hacer `commit()` o `rollback()`.

    Args:
        conn:        Conexión activa a PostgreSQL que ya tiene una transacción abierta
                     con el cambio de negocio.
        envelope:    El EventEnvelope a persistir en la outbox.
        routing_key: (Opcional) La routing key para RabbitMQ. Si se omite, se usa `envelope.type`.

    Returns:
        El ID del evento persistido (str).

    Raises:
        ValueError: Si `conn` o `envelope` son nulos, o si el envelope no tiene ID.
    """
    if conn is None:
        raise ValueError("Se requiere una conexión activa a la base de datos para escribir en el outbox.")
    if envelope is None or not isinstance(envelope, EventEnvelope):
        raise ValueError("Se requiere una instancia válida de EventEnvelope.")
    if not envelope.id:
        raise ValueError("El EventEnvelope debe tener un 'id' no vacío.")

    rk = routing_key.strip() if (routing_key and routing_key.strip()) else envelope.type
    event_id = str(envelope.id)

    with conn.cursor() as cur:
        cur.execute(
            _INSERT_OUTBOX_SQL,
            (
                event_id,
                envelope.type,
                rk,
                envelope.correlationId,
                envelope.causationId,
                envelope.source,
                Jsonb(envelope.payload),
            ),
        )

    logger.debug(
        "[outbox] Evento '%s' (id=%s) preparado en outbox dentro de la transacción activa.",
        envelope.type,
        event_id,
    )
    return event_id


# ─── Helper de contexto transaccional ────────────────────────────────────────

@contextmanager
def transactional_scope(
    conn: Optional[psycopg.Connection] = None,
) -> Generator[psycopg.Connection, None, None]:
    """
    Context manager para operaciones transaccionales atómicas (negocio + outbox).

    Si no se provee una conexión:
        Abre una conexión nueva, ejecuta un bloque transaccional:
        - Si el bloque finaliza sin errores ➔ hace `commit()`.
        - Si ocurre alguna excepción ➔ hace `rollback()` automático.
        - Cierra la conexión al finalizar.

    Si se provee una conexión:
        Maneja un bloque transaccional / savepoint sobre la conexión existente,
        haciendo commit o rollback según corresponda.

    Ejemplo de uso:
        with transactional_scope() as conn:
            # 1. Operación de negocio
            with conn.cursor() as cur:
                cur.execute("INSERT INTO videos (id, title) VALUES (%s, %s)", (vid, "Mi Video"))

            # 2. Persistencia en outbox dentro de la misma transacción
            save_to_outbox(conn, envelope, routing_key="m1.video.uploaded")

            # Si algo falla aquí adentro, nada se guarda (cero eventos huérfanos).
    """
    if conn is not None:
        # Usamos la conexión existente
        with conn.transaction():
            yield conn
    else:
        # Abrimos una conexión dedicada
        managed_conn = get_connection()
        try:
            with managed_conn.transaction():
                yield managed_conn
        finally:
            managed_conn.close()


# ─── Consultas auxiliares para verificación y relay (US-B5b) ─────────────────

def get_outbox_event(
    event_id: str,
    conn: Optional[psycopg.Connection] = None,
) -> Optional[Dict[str, Any]]:
    """
    Obtiene un registro de la tabla outbox por su event_id.

    Returns:
        Diccionario con las columnas de la fila o None si no existe.
    """
    sql = "SELECT * FROM outbox WHERE id = %s;"

    def _query(c: psycopg.Connection) -> Optional[Dict[str, Any]]:
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (event_id,))
            return cur.fetchone()

    if conn is not None:
        return _query(conn)
    with get_connection() as local_conn:
        return _query(local_conn)


def get_pending_events(
    limit: int = 100,
    conn: Optional[psycopg.Connection] = None,
) -> List[Dict[str, Any]]:
    """
    Retorna los eventos con status = 'PENDING' ordenados cronológicamente por created_at.
    """
    sql = """
    SELECT * FROM outbox
    WHERE status = 'PENDING'
    ORDER BY created_at ASC
    LIMIT %s;
    """

    def _query(c: psycopg.Connection) -> List[Dict[str, Any]]:
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (limit,))
            return list(cur.fetchall())

    if conn is not None:
        return _query(conn)
    with get_connection() as local_conn:
        return _query(local_conn)


def mark_as_published(
    event_id: str,
    conn: Optional[psycopg.Connection] = None,
) -> bool:
    """
    Marca un evento de la outbox como 'PUBLISHED' y registra la fecha de publicación.

    Returns:
        True si se actualizó el registro, False si no existía.
    """
    sql = """
    UPDATE outbox
    SET status = 'PUBLISHED', published_at = now()
    WHERE id = %s;
    """

    def _update(c: psycopg.Connection) -> bool:
        with c.cursor() as cur:
            cur.execute(sql, (event_id,))
            updated = cur.rowcount > 0
        c.commit()
        return updated

    if conn is not None:
        return _update(conn)
    with get_connection() as local_conn:
        return _update(local_conn)

"""
Event Store v1 — persistencia de eventos en PostgreSQL.

Cada evento que pasa por el exchange 'pubtube.events' (todos, gracias al
binding '#' de la cola q.m2.event_store) se guarda aquí para auditoría,
trazabilidad por correlationId y eventual reproducción/replay.
"""
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from envelope import EventEnvelope


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY,
    type            TEXT NOT NULL,
    version         INTEGER NOT NULL,
    timestamp       TEXT NOT NULL,
    correlation_id  UUID NOT NULL,
    causation_id    UUID,
    source          TEXT,
    routing_key     TEXT NOT NULL,
    payload         JSONB NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events (correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (type);
"""


def init_db() -> None:
    """Crea la tabla 'events' y sus índices si no existen. Idempotente."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()


def insert_event(envelope: EventEnvelope, routing_key: str) -> bool:
    """
    Persiste un evento en el event store.
    Retorna True si se insertó una fila nueva, False si ya existía (duplicado).
    """
    sql = """
    INSERT INTO events (id, type, version, timestamp, correlation_id, causation_id, source, routing_key, payload)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                envelope.id,
                envelope.type,
                envelope.version,
                envelope.timestamp,
                envelope.correlationId,
                envelope.causationId,
                envelope.source,
                routing_key,
                Jsonb(envelope.payload),
            ))
            inserted = cur.rowcount > 0
        conn.commit()
    return inserted


def get_events_by_correlation_id(correlation_id: str) -> list[dict]:
    """
    Recupera todos los eventos de un flujo/saga, ordenados cronológicamente.
    Sirve de base para el endpoint GET /events/{correlationId} que pide el proyecto.
    """
    sql = """
    SELECT id, type, version, timestamp, correlation_id, causation_id, source, routing_key, payload, received_at
    FROM events
    WHERE correlation_id = %s
    ORDER BY received_at ASC
    """
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (correlation_id,))
            return cur.fetchall()
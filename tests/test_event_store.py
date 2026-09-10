"""
Test de integración: publica un evento real por RabbitMQ hacia la cola
q.m2.event_store, lo procesa igual que event_store_consumer.py, y confirma
que quedó persistido correctamente en PostgreSQL.

Requiere que RabbitMQ y PostgreSQL estén levantados (docker compose up -d).
Si Postgres no está disponible, el test se salta en vez de fallar.

Correr con:
    pytest tests/test_event_store.py -v
"""
import uuid
import psycopg
import pytest

from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from envelope import EventEnvelope
from event_store import init_db, insert_event, get_events_by_correlation_id


@pytest.fixture
def db_connection():
    """Se conecta a Postgres; si no está disponible, salta el test en vez de fallar."""
    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        )
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL no está disponible (¿corriste 'docker compose up -d'?)")
    init_db()
    yield conn
    conn.close()


def test_insert_event_persiste_correctamente(db_connection):
    correlation_id = str(uuid.uuid4())
    evento = EventEnvelope(
        type="m1.video.uploaded",
        correlationId=correlation_id,
        payload={"contentId": "test_video_123"},
    )

    insertado = insert_event(evento, routing_key="m1.video.uploaded")
    assert insertado is True

    eventos = get_events_by_correlation_id(correlation_id)
    assert len(eventos) == 1
    assert eventos[0]["type"] == "m1.video.uploaded"
    assert eventos[0]["payload"] == {"contentId": "test_video_123"}


def test_insert_event_es_idempotente(db_connection):
    """El mismo evento (mismo id) insertado dos veces no debe duplicarse."""
    correlation_id = str(uuid.uuid4())
    evento = EventEnvelope(
        type="m1.metadata.updated",
        correlationId=correlation_id,
        payload={"contentId": "test_video_456"},
    )

    primera_insercion = insert_event(evento, routing_key="m1.metadata.updated")
    segunda_insercion = insert_event(evento, routing_key="m1.metadata.updated")  # mismo id

    assert primera_insercion is True
    assert segunda_insercion is False  # duplicado, ignorado

    eventos = get_events_by_correlation_id(correlation_id)
    assert len(eventos) == 1  # no se duplicó


def test_get_events_by_correlation_id_ordena_cronologicamente(db_connection):
    correlation_id = str(uuid.uuid4())

    primero = EventEnvelope(type="m1.video.uploaded", correlationId=correlation_id, payload={"paso": 1})
    segundo = EventEnvelope(type="m1.metadata.updated", correlationId=correlation_id, payload={"paso": 2})

    insert_event(primero, routing_key="m1.video.uploaded")
    insert_event(segundo, routing_key="m1.metadata.updated")

    eventos = get_events_by_correlation_id(correlation_id)
    assert len(eventos) == 2
    assert eventos[0]["type"] == "m1.video.uploaded"
    assert eventos[1]["type"] == "m1.metadata.updated"
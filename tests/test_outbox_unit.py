"""
test_outbox_unit.py — Pruebas unitarias de la persistencia transaccional en Outbox (US-B5a).

Verifica el comportamiento de `src/outbox.py` de forma aislada (offline)
usando mocks, sin requerir PostgreSQL ni RabbitMQ corriendo.

Casos verificados:
  1. `save_to_outbox` ejecuta el INSERT con todos los campos del EventEnvelope.
  2. `save_to_outbox` NUNCA llama a `commit()` (regla transaccional de US-B5a).
  3. `routing_key` por defecto es `envelope.type` o el valor explícito si se provee.
  4. Validaciones defensivas (conexión nula, envelope nulo, envelope sin ID).
  5. `transactional_scope` gestiona el bloque transaccional correctamente.
  6. Funciones de consulta (`get_outbox_event`, `get_pending_events`, `mark_as_published`).
"""

from unittest.mock import MagicMock, patch
import pytest

from envelope import EventEnvelope
from outbox import (
    init_outbox_db,
    save_to_outbox,
    transactional_scope,
    get_outbox_event,
    get_pending_events,
    mark_as_published,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

from typing import Optional

def _make_envelope(
    event_id: str = "evt-outbox-001",
    event_type: str = "m1.video.uploaded",
    correlation_id: str = "saga-123",
    causation_id: str = "evt-parent-000",
    payload: Optional[dict] = None,
) -> EventEnvelope:
    env = EventEnvelope(
        type=event_type,
        correlationId=correlation_id,
        causationId=causation_id,
        source="m1-uploader",
        payload=payload or {"contentId": "video_abc123"},
    )
    object.__setattr__(env, "id", event_id)
    return env


# ─── Tests de save_to_outbox ──────────────────────────────────────────────────

class TestSaveToOutbox:
    """Pruebas unitarias de save_to_outbox."""

    def test_save_to_outbox_llama_insert_con_parametros_correctos(self):
        """Verifica que los datos del envelope se mapeen correctamente al SQL."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        envelope = _make_envelope(
            event_id="evt-456",
            event_type="m1.video.uploaded",
            correlation_id="saga-789",
            causation_id="evt-parent-001",
            payload={"videoId": "v123", "size": 1024},
        )

        returned_id = save_to_outbox(mock_conn, envelope)

        assert returned_id == "evt-456"
        mock_cur.execute.assert_called_once()
        sql, params = mock_cur.execute.call_args[0]
        assert "INSERT INTO outbox" in sql
        assert params[0] == "evt-456"
        assert params[1] == "m1.video.uploaded"
        assert params[2] == "m1.video.uploaded"  # routing_key default = type
        assert params[3] == "saga-789"
        assert params[4] == "evt-parent-001"
        assert params[5] == "m1-uploader"

    def test_save_to_outbox_no_hace_commit(self):
        """
        CRITERIO ARQUITECTÓNICO FUNDAMENTAL:
        save_to_outbox NUNCA debe llamar a commit(). La transacción debe
        permanecer abierta para que el llamador decida si confirma o revierte.
        """
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        envelope = _make_envelope()
        save_to_outbox(mock_conn, envelope)

        mock_conn.commit.assert_not_called()

    def test_save_to_outbox_usa_routing_key_personalizada(self):
        """Si se especifica una routing key diferente al type, debe usarse."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        envelope = _make_envelope(event_type="video.event")
        save_to_outbox(mock_conn, envelope, routing_key="custom.routing.key")

        _, params = mock_cur.execute.call_args[0]
        assert params[1] == "video.event"           # event_type
        assert params[2] == "custom.routing.key"    # routing_key

    def test_save_to_outbox_falla_si_conn_es_none(self):
        """Debe lanzar ValueError si la conexión no está presente."""
        envelope = _make_envelope()
        with pytest.raises(ValueError, match="conexión activa"):
            save_to_outbox(None, envelope)  # type: ignore

    def test_save_to_outbox_falla_si_envelope_es_invalido(self):
        """Debe lanzar ValueError si el objeto no es un EventEnvelope válido."""
        mock_conn = MagicMock()
        with pytest.raises(ValueError, match="instancia válida de EventEnvelope"):
            save_to_outbox(mock_conn, "no-es-un-envelope")  # type: ignore

    def test_save_to_outbox_falla_si_envelope_id_es_vacio(self):
        """Debe lanzar ValueError si el ID del envelope está vacío."""
        mock_conn = MagicMock()
        envelope = _make_envelope()
        object.__setattr__(envelope, "id", "")

        with pytest.raises(ValueError, match="no vacío"):
            save_to_outbox(mock_conn, envelope)


# ─── Tests de transactional_scope ────────────────────────────────────────────

class TestTransactionalScope:
    """Pruebas del context manager de transacción."""

    def test_scope_con_conexion_existente_usa_transaction(self):
        """Al pasar una conexión existente, gestiona el bloque con with conn.transaction()."""
        mock_conn = MagicMock()
        mock_tx = MagicMock()
        mock_conn.transaction.return_value = mock_tx

        with transactional_scope(mock_conn) as conn:
            assert conn is mock_conn

        mock_conn.transaction.assert_called_once()
        mock_tx.__enter__.assert_called_once()
        mock_tx.__exit__.assert_called_once()

    def test_scope_propaga_excepcion_al_llamador(self):
        """Si el bloque lanza una excepción, debe propagarla."""
        mock_conn = MagicMock()

        with pytest.raises(RuntimeError, match="Error simulado en negocio"):
            with transactional_scope(mock_conn):
                raise RuntimeError("Error simulado en negocio")


# ─── Tests de DDL y consultas auxiliares ─────────────────────────────────────

class TestConsultasYDDL:
    """Pruebas de inicialización de esquema y helpers."""

    def test_init_outbox_db_ejecuta_ddl_con_conn(self):
        """init_outbox_db ejecuta CREATE TABLE y commit en la conexión dada."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        init_outbox_db(mock_conn)

        mock_cur.execute.assert_called_once()
        sql = mock_cur.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS outbox" in sql
        mock_conn.commit.assert_called_once()

    def test_get_outbox_event_con_conn(self):
        """get_outbox_event ejecuta SELECT por id."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchone.return_value = {"id": "evt-001", "status": "PENDING"}

        result = get_outbox_event("evt-001", conn=mock_conn)

        assert result is not None
        assert result["id"] == "evt-001"
        assert result["status"] == "PENDING"
        mock_cur.execute.assert_called_once()

    def test_get_pending_events_con_conn(self):
        """get_pending_events consulta eventos con status = PENDING."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchall.return_value = [{"id": "evt-001"}, {"id": "evt-002"}]

        result = get_pending_events(limit=50, conn=mock_conn)

        assert len(result) == 2
        mock_cur.execute.assert_called_once()
        sql, params = mock_cur.execute.call_args[0]
        assert "WHERE status = 'PENDING'" in sql
        assert params == (50,)

    def test_mark_as_published_con_conn(self):
        """mark_as_published actualiza el registro a PUBLISHED y hace commit."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.rowcount = 1

        exito = mark_as_published("evt-001", conn=mock_conn)

        assert exito is True
        mock_cur.execute.assert_called_once()
        sql, params = mock_cur.execute.call_args[0]
        assert "SET status = 'PUBLISHED'" in sql
        assert params == ("evt-001",)
        mock_conn.commit.assert_called_once()

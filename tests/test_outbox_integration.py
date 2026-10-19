"""
test_outbox_integration.py — Pruebas de integración transaccional de Outbox con PostgreSQL real (US-B5a).

Verifica los criterios de aceptación centrales de la US-B5a:
  1. "El evento se escribe en la tabla outbox dentro de la misma transacción que el cambio de negocio."
  2. "Si la transacción falla, no queda ningún evento huérfano en el outbox."
  3. Pruebas de atomicidad ante fallos simulados (rollback completo).

Requiere PostgreSQL activo (docker compose up -d).
Si PostgreSQL no está disponible, los tests se saltan automáticamente.
"""

from typing import Any, Optional
import uuid
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pytest

from envelope import EventEnvelope
from outbox import (
    get_connection,
    init_outbox_db,
    save_to_outbox,
    transactional_scope,
    get_outbox_event,
    get_pending_events,
    mark_as_published,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

_CREATE_DUMMY_BUSINESS_TABLE = """
CREATE TABLE IF NOT EXISTS dummy_business_items (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


@pytest.fixture(scope="module")
def db_ready():
    """
    Verifica que PostgreSQL esté disponible e inicializa la tabla outbox
    y una tabla de negocio simulada (dummy_business_items).
    """
    try:
        from config import (
            POSTGRES_HOST,
            POSTGRES_PORT,
            POSTGRES_DB,
            POSTGRES_USER,
            POSTGRES_PASSWORD,
        )
    except KeyError as exc:
        pytest.skip(f"Variables de entorno no definidas ({exc})")

    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=2,
        )
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL no está disponible (ejecuta 'docker compose up -d')")

    with conn:
        init_outbox_db(conn)
        with conn.cursor() as cur:
            cur.execute(_CREATE_DUMMY_BUSINESS_TABLE)

    conn.close()


@pytest.fixture(autouse=True)
def limpiar_tablas(db_ready):
    """Limpia las tablas outbox y dummy_business_items antes de cada prueba."""
    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE outbox;")
            cur.execute("TRUNCATE TABLE dummy_business_items;")
    conn.close()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_envelope(
    event_id: Optional[str] = None,
    event_type: str = "m1.video.uploaded",
    correlation_id: str = "saga-outbox-001",
    payload: Optional[dict] = None,
) -> EventEnvelope:
    env = EventEnvelope(
        type=event_type,
        correlationId=correlation_id,
        source="m1-uploader",
        payload=payload or {"videoId": "vid_xyz", "title": "Tutorial PubTube"},
    )
    if event_id:
        object.__setattr__(env, "id", event_id)
    return env


def _contar_filas(tabla: str) -> int:
    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(tabla)))
            row = cur.fetchone()
            res = int(row[0]) if row else 0
    conn.close()
    return res


# ─── Pruebas de Atomicidad y Criterios de Aceptación ──────────────────────────

class TestOutboxPersistenciaTransaccional:
    """Verifica los criterios de aceptación de US-B5a con transacciones reales."""

    def test_escritura_transaccional_exitosa_persiste_negocio_y_outbox(self):
        """
        Criterio 1: El evento se escribe en la tabla outbox dentro de la misma
        transacción que el cambio de negocio.
        Ambos registros se confirman juntos al hacer commit.
        """
        item_id = f"item-{uuid.uuid4()}"
        envelope = _make_envelope()

        conn = get_connection()
        # Iniciamos la transacción que agrupa negocio + outbox
        with conn.transaction():
            # 1. Cambio de negocio
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dummy_business_items (id, name) VALUES (%s, %s);",
                    (item_id, "Video Test Publicado"),
                )

            # 2. Persistencia en outbox dentro de la misma transacción
            event_id = save_to_outbox(conn, envelope, routing_key="m1.video.uploaded")

        conn.close()

        # Verificación post-commit: ambos existen en la base de datos
        assert _contar_filas("dummy_business_items") == 1
        assert _contar_filas("outbox") == 1

        registro_outbox = get_outbox_event(event_id)
        assert registro_outbox is not None
        assert registro_outbox["id"] == envelope.id
        assert registro_outbox["event_type"] == "m1.video.uploaded"
        assert registro_outbox["routing_key"] == "m1.video.uploaded"
        assert registro_outbox["correlation_id"] == "saga-outbox-001"
        assert registro_outbox["status"] == "PENDING"
        assert registro_outbox["payload"]["title"] == "Tutorial PubTube"
        assert registro_outbox["published_at"] is None

    def test_fallo_en_negocio_hace_rollback_y_no_deja_evento_huerfano(self):
        """
        Criterio 2 (Principal): Si la transacción falla, no queda ningún
        evento huérfano en el outbox.
        Simula un error después de save_to_outbox antes del commit.
        """
        item_id = f"item-{uuid.uuid4()}"
        envelope = _make_envelope()

        conn = get_connection()

        # La transacción debe revertirse completamente ante la excepción
        with pytest.raises(RuntimeError, match="Error inesperado de negocio"):
            with conn.transaction():
                # 1. Cambio de negocio
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO dummy_business_items (id, name) VALUES (%s, %s);",
                        (item_id, "Video que fallará"),
                    )

                # 2. Se prepara el evento en outbox
                save_to_outbox(conn, envelope)

                # 3. Simulamos un fallo crítico de negocio antes del commit
                raise RuntimeError("Error inesperado de negocio tras guardar")

        conn.close()

        # Verificación post-rollback:
        # Ni el cambio de negocio ni el evento en outbox existen.
        assert _contar_filas("dummy_business_items") == 0
        assert _contar_filas("outbox") == 0, "No debe quedar NINGÚN evento huérfano en la tabla outbox."
        assert get_outbox_event(envelope.id) is None

    def test_fallo_en_outbox_revierte_cambio_de_negocio(self):
        """
        Si la inserción en outbox falla (ej: violación de clave o dato inválido),
        el cambio de negocio también debe revertirse completamente.
        """
        item_id = f"item-{uuid.uuid4()}"
        conn = get_connection()

        with pytest.raises(ValueError, match="instancia válida de EventEnvelope"):
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO dummy_business_items (id, name) VALUES (%s, %s);",
                        (item_id, "Video Huérfano Evitado"),
                    )

                # Pasamos un envelope inválido para forzar error en outbox
                save_to_outbox(conn, None)  # type: ignore

        conn.close()

        # El cambio de negocio no se confirmó
        assert _contar_filas("dummy_business_items") == 0
        assert _contar_filas("outbox") == 0

    def test_rollback_explicito_no_deja_evento_en_outbox(self):
        """
        Si la aplicación decide explícitamente llamar a conn.rollback(),
        el evento no persiste en el outbox.
        """
        envelope = _make_envelope()
        conn = get_connection()

        # Sin context manager, con rollback explícito
        save_to_outbox(conn, envelope)
        conn.rollback()
        conn.close()

        assert _contar_filas("outbox") == 0
        assert get_outbox_event(envelope.id) is None

    def test_multiples_eventos_en_misma_transaccion(self):
        """
        Una operación de negocio compleja puede generar múltiples eventos
        (ej: video.uploaded y video.notification.requested) en la misma transacción.
        """
        envelope_1 = _make_envelope(event_id="evt-multi-1", event_type="m1.video.uploaded")
        envelope_2 = _make_envelope(event_id="evt-multi-2", event_type="m1.notification.requested")

        conn = get_connection()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dummy_business_items (id, name) VALUES (%s, %s);",
                    ("item-multi", "Video con dos eventos"),
                )
            save_to_outbox(conn, envelope_1)
            save_to_outbox(conn, envelope_2)
        conn.close()

        assert _contar_filas("dummy_business_items") == 1
        assert _contar_filas("outbox") == 2

        pending = get_pending_events(limit=10)
        assert len(pending) == 2
        ids = [p["id"] for p in pending]
        assert "evt-multi-1" in ids
        assert "evt-multi-2" in ids

    def test_transactional_scope_context_manager_exitoso(self):
        """Prueba del helper transactional_scope con commit exitoso."""
        envelope = _make_envelope(event_id="evt-scope-ok")

        with transactional_scope() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dummy_business_items (id, name) VALUES (%s, %s);",
                    ("item-scope", "Prueba Scope"),
                )
            save_to_outbox(conn, envelope)

        assert _contar_filas("dummy_business_items") == 1
        assert _contar_filas("outbox") == 1
        assert get_outbox_event("evt-scope-ok") is not None

    def test_transactional_scope_context_manager_con_rollback(self):
        """Prueba del helper transactional_scope revirtiendo todo ante error."""
        envelope = _make_envelope(event_id="evt-scope-fail")

        with pytest.raises(ValueError, match="Fallo de prueba en scope"):
            with transactional_scope() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO dummy_business_items (id, name) VALUES (%s, %s);",
                        ("item-scope-fail", "Prueba Scope Fallida"),
                    )
                save_to_outbox(conn, envelope)
                raise ValueError("Fallo de prueba en scope")

        assert _contar_filas("dummy_business_items") == 0
        assert _contar_filas("outbox") == 0
        assert get_outbox_event("evt-scope-fail") is None

    def test_marcar_como_publicado_actualiza_estado(self):
        """Verifica que mark_as_published actualice el estado a PUBLISHED y la fecha."""
        envelope = _make_envelope(event_id="evt-publish-test")

        with transactional_scope() as conn:
            save_to_outbox(conn, envelope)

        # Antes de publicar
        evento_antes = get_outbox_event("evt-publish-test")
        assert evento_antes is not None
        assert evento_antes["status"] == "PENDING"
        assert evento_antes["published_at"] is None

        # Marcamos como publicado
        actualizado = mark_as_published("evt-publish-test")
        assert actualizado is True

        # Después de publicar
        evento_despues = get_outbox_event("evt-publish-test")
        assert evento_despues is not None
        assert evento_despues["status"] == "PUBLISHED"
        assert evento_despues["published_at"] is not None

        # Ya no debe aparecer en los eventos pendientes
        pendientes = get_pending_events()
        assert len(pendientes) == 0

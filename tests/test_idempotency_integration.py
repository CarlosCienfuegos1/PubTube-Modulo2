"""
test_idempotency_integration.py — Pruebas de integracion y concurrencia real
del repositorio de idempotencia (US-B4a).

Estas pruebas requieren que PostgreSQL este levantado (docker compose up -d).
Si Postgres no esta disponible, los tests se saltan automaticamente en vez
de fallar, siguiendo el mismo patron de test_event_store.py.

Incluye:
  - Pruebas funcionales sequenciales (nuevo / duplicado / multi-consumidor).
  - Prueba de race condition real: N hilos atacan con el mismo (consumer_id,
    event_id) al mismo tiempo usando threading.Barrier y
    concurrent.futures.ThreadPoolExecutor para garantizar que exactamente
    UNA llamada retorne True y el resto retornen False.

Correr con:
    pytest tests/test_idempotency_integration.py -v
"""
import uuid
import threading
import concurrent.futures
from typing import Optional, cast
from unittest.mock import MagicMock

import psycopg
import pytest

from envelope import EventEnvelope
from idempotency import (
    init_idempotency_db,
    try_register_event,
    make_idempotent_middleware,
    idempotent_handler,
)


# ─── Fixture de conexion ──────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def db_ready():
    """
    Verifica que PostgreSQL este disponible e inicializa el esquema.
    Scope 'module': se ejecuta una vez para todos los tests de este archivo.
    Se salta si falta el .env o si Postgres no esta corriendo.
    """
    try:
        from config import (
            POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
            POSTGRES_USER, POSTGRES_PASSWORD,
        )
    except KeyError as exc:
        pytest.skip(f"Variables de entorno no definidas — crea el archivo .env ({exc})")

    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=2,
        )
        conn.close()
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL no esta disponible (corre 'docker compose up -d')")

    init_idempotency_db()


@pytest.fixture(autouse=True)
def limpiar_tabla(db_ready):
    """
    Limpia la tabla processed_events antes de cada test para garantizar
    aislamiento entre pruebas. 'autouse=True' la aplica automaticamente.
    """
    from config import (
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
        POSTGRES_USER, POSTGRES_PASSWORD,
    )
    conn = psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=2,
    )
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE processed_events;")
    conn.commit()
    conn.close()


# ─── Helper de consulta directa ───────────────────────────────────────────────

def _contar_registros(event_id: str, consumer_id: str) -> int:
    """Cuenta cuantos registros existen para el par (event_id, consumer_id)."""
    from config import (
        POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
        POSTGRES_USER, POSTGRES_PASSWORD,
    )
    conn = psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=2,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM processed_events "
            "WHERE event_id = %s AND consumer_id = %s",
            (event_id, consumer_id),
        )
        row = cur.fetchone()
        assert row is not None
        count = row[0]
    conn.close()
    return count


# ─── Tests funcionales sequenciales ──────────────────────────────────────────

class TestRegistroSecuencial:
    """Pruebas de comportamiento basico sin concurrencia."""

    def test_primer_registro_retorna_true(self):
        """El primer intento de registrar un evento debe retornar True."""
        event_id = str(uuid.uuid4())
        resultado = try_register_event("q.m4.experience", event_id)
        assert resultado is True

    def test_segundo_registro_mismo_par_retorna_false(self):
        """El mismo (consumer_id, event_id) registrado dos veces -> False."""
        event_id = str(uuid.uuid4())
        consumer_id = "q.m4.experience"

        primera = try_register_event(consumer_id, event_id)
        segunda = try_register_event(consumer_id, event_id)

        assert primera is True
        assert segunda is False

    def test_segundo_registro_no_duplica_fila_en_db(self):
        """Tras dos llamadas identicas, solo debe existir 1 fila en la BD."""
        event_id = str(uuid.uuid4())
        consumer_id = "q.m4.experience"

        try_register_event(consumer_id, event_id)
        try_register_event(consumer_id, event_id)

        assert _contar_registros(event_id, consumer_id) == 1

    def test_mismo_evento_distintos_consumidores_ambos_son_nuevos(self):
        """
        El mismo event_id con distintos consumer_id debe permitir que
        ambos consumidores registren exitosamente (True, True).
        """
        event_id = str(uuid.uuid4())

        r1 = try_register_event("q.m4.experience", event_id)
        r2 = try_register_event("q.m3.publish", event_id)

        assert r1 is True
        assert r2 is True
        assert _contar_registros(event_id, "q.m4.experience") == 1
        assert _contar_registros(event_id, "q.m3.publish") == 1

    def test_mismo_consumidor_distintos_eventos_ambos_son_nuevos(self):
        """Dos eventos distintos para el mismo consumidor -> True, True."""
        consumer_id = "q.m4.experience"
        event_id_1 = str(uuid.uuid4())
        event_id_2 = str(uuid.uuid4())

        r1 = try_register_event(consumer_id, event_id_1)
        r2 = try_register_event(consumer_id, event_id_2)

        assert r1 is True
        assert r2 is True

    def test_processed_at_se_almacena(self):
        """El campo processed_at no debe ser NULL."""
        from config import (
            POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
            POSTGRES_USER, POSTGRES_PASSWORD,
        )
        event_id = str(uuid.uuid4())
        consumer_id = "q.m4.experience"
        try_register_event(consumer_id, event_id)

        conn = psycopg.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT processed_at FROM processed_events "
                "WHERE event_id = %s AND consumer_id = %s",
                (event_id, consumer_id),
            )
            row = cur.fetchone()
        conn.close()

        assert row is not None
        assert row[0] is not None  # processed_at no es NULL


# ─── Tests de concurrencia real ───────────────────────────────────────────────

class TestConcurrenciaReal:
    """
    Pruebas de race condition contra PostgreSQL real.

    Criterio de aceptacion: ante N llamadas concurrentes con el mismo
    (consumer_id, event_id), exactamente UNA retorna True y las demas
    retornan False, y la BD contiene exactamente 1 fila.
    """

    def _lanzar_concurrente(self, n_hilos: int, consumer_id: str, event_id: str) -> list[bool]:
        """
        Lanza n_hilos que llaman a try_register_event con el mismo par,
        usando threading.Barrier para disparar al mismo instante.
        Retorna la lista de resultados.
        """
        resultados: list[Optional[bool]] = [None] * n_hilos
        barrier = threading.Barrier(n_hilos)

        def worker(idx: int):
            barrier.wait()  # todos esperan aqui hasta que el ultimo llega
            resultados[idx] = try_register_event(consumer_id, event_id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_hilos) as executor:
            futures = [executor.submit(worker, i) for i in range(n_hilos)]
            concurrent.futures.wait(futures)

        # Todos los workers ya terminaron: cada elemento es bool, nunca None.
        return cast(list[bool], resultados)

    def test_10_hilos_concurrentes_solo_uno_exitoso(self):
        """Con 10 hilos simulando race condition: exactamente 1 True, 9 False."""
        N = 10
        event_id = str(uuid.uuid4())
        consumer_id = "q.m4.experience"

        resultados = self._lanzar_concurrente(N, consumer_id, event_id)

        assert resultados.count(True) == 1, (
            f"Se esperaba exactamente 1 True, se obtuvo: {resultados}"
        )
        assert resultados.count(False) == N - 1
        assert _contar_registros(event_id, consumer_id) == 1

    def test_20_hilos_concurrentes_solo_uno_exitoso(self):
        """Con 20 hilos: exactamente 1 True, 19 False y 1 fila en BD."""
        N = 20
        event_id = str(uuid.uuid4())
        consumer_id = "q.m3.publish"

        resultados = self._lanzar_concurrente(N, consumer_id, event_id)

        assert resultados.count(True) == 1, (
            f"Se esperaba exactamente 1 True, se obtuvo: {resultados}"
        )
        assert resultados.count(False) == N - 1
        assert _contar_registros(event_id, consumer_id) == 1

    def test_concurrencia_distintos_consumidores_mismo_evento(self):
        """
        2 grupos de N hilos, cada grupo con un consumer_id distinto,
        todos usando el mismo event_id.
        Resultado esperado: exactamente 1 True por grupo (2 filas en BD total).
        """
        N = 10
        event_id = str(uuid.uuid4())
        consumer_m4 = "q.m4.experience"
        consumer_m3 = "q.m3.publish"

        # Lanzamos ambos grupos en paralelo usando ThreadPoolExecutor anidado.
        resultados_m4: list[Optional[bool]] = [None] * N
        resultados_m3: list[Optional[bool]] = [None] * N
        barrier = threading.Barrier(N * 2)  # todos juntos

        def worker_m4(idx):
            barrier.wait()
            resultados_m4[idx] = try_register_event(consumer_m4, event_id)

        def worker_m3(idx):
            barrier.wait()
            resultados_m3[idx] = try_register_event(consumer_m3, event_id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=N * 2) as ex:
            futures = (
                [ex.submit(worker_m4, i) for i in range(N)]
                + [ex.submit(worker_m3, i) for i in range(N)]
            )
            concurrent.futures.wait(futures)

        # Todos los workers terminaron: cada elemento es bool, nunca None.
        res_m4 = cast(list[bool], resultados_m4)
        res_m3 = cast(list[bool], resultados_m3)

        assert res_m4.count(True) == 1
        assert res_m3.count(True) == 1
        assert _contar_registros(event_id, consumer_m4) == 1
        assert _contar_registros(event_id, consumer_m3) == 1

    def test_no_hay_filas_huerfanas_tras_concurrencia(self):
        """
        La tabla debe tener exactamente 1 fila tras N llamadas concurrentes,
        sin registros corruptos ni parciales.
        """
        N = 15
        event_id = str(uuid.uuid4())
        consumer_id = "q.m4.experience"

        self._lanzar_concurrente(N, consumer_id, event_id)

        total = _contar_registros(event_id, consumer_id)
        assert total == 1, (
            f"Se esperaba 1 fila en BD, se encontraron {total}"
        )


# ─── Pruebas de integracion del Middleware con reentrega forzada ─────────────

class TestMiddlewareIntegracionReentrega:
    """
    Pruebas de integracion del middleware transparente (Opción 2) con PostgreSQL real:
    Verifica los criterios de aceptación centrales de US-B4:
      - 'Reentregar un evento no produce efectos duplicados'
      - 'Se registra la clave de idempotencia de cada evento procesado'
    """

    def test_reentrega_forzada_no_produce_efectos_duplicados(self):
        """
        Al reentregar forzadamente el mismo evento:
        1. Primera entrega: ejecuta el handler de negocio, registra en BD y hace ACK.
        2. Segunda entrega: OMITE el handler de negocio y hace ACK directo a RabbitMQ.
        """
        event_id = str(uuid.uuid4())
        consumer_id = "q.m4.experience"
        ejecuciones = []

        def mi_handler(envelope):
            ejecuciones.append(envelope.id)

        wrapped = make_idempotent_middleware(consumer_id, mi_handler)

        envelope = EventEnvelope(
            type="m1.video.uploaded",
            correlationId="saga-integ-001",
            payload={"contentId": "video_123"},
        )
        object.__setattr__(envelope, "id", event_id)

        channel = MagicMock()
        method_1 = MagicMock()
        method_1.delivery_tag = 101

        # ── Entrega 1: Evento nuevo ──
        wrapped(envelope, channel, method_1)

        assert len(ejecuciones) == 1
        assert ejecuciones[0] == event_id
        assert _contar_registros(event_id, consumer_id) == 1
        channel.basic_ack.assert_called_once_with(delivery_tag=101)

        # ── Entrega 2: Reentrega forzada del mismo evento ──
        channel.reset_mock()
        method_2 = MagicMock()
        method_2.delivery_tag = 102

        wrapped(envelope, channel, method_2)

        # Criterio clave: NO se ejecutó el handler por segunda vez
        assert len(ejecuciones) == 1
        # Se envió ACK directo a RabbitMQ para evitar reencolar
        channel.basic_ack.assert_called_once_with(delivery_tag=102)
        # La BD sigue teniendo exactamente 1 registro
        assert _contar_registros(event_id, consumer_id) == 1

    def test_reentrega_forzada_con_decorador_idempotent_handler(self):
        """
        Verifica el mismo comportamiento usando @idempotent_handler con PostgreSQL real.
        """
        event_id = str(uuid.uuid4())
        consumer_id = "q.m3.publish"
        contador = 0

        @idempotent_handler(consumer_id)
        def mi_handler(envelope):
            nonlocal contador
            contador += 1

        envelope = EventEnvelope(
            type="m1.video.uploaded",
            correlationId="saga-integ-002",
            payload={"contentId": "video_456"},
        )
        object.__setattr__(envelope, "id", event_id)

        channel = MagicMock()
        method = MagicMock()
        method.delivery_tag = 200

        # Entrega 1
        mi_handler(envelope, channel, method)
        assert contador == 1
        assert _contar_registros(event_id, consumer_id) == 1

        # Entrega 2 (reentrega forzada)
        mi_handler(envelope, channel, method)
        assert contador == 1  # No subió el contador!
        assert _contar_registros(event_id, consumer_id) == 1


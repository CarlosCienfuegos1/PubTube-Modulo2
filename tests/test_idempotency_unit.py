"""
test_idempotency_unit.py — Pruebas unitarias del repositorio de idempotencia
(US-B4a).

Estas pruebas verifican la logica de try_register_event() SIN necesitar
PostgreSQL corriendo. Usan unittest.mock.patch para sustituir psycopg.connect
por un objeto falso controlado, de modo que podamos forzar cualquier
comportamiento (insercion exitosa, rowcount=0, UniqueViolation, etc.).

Se incluye ademas una prueba de concurrencia a nivel de threads que
verifica que no existan variables compartidas mutables que rompan la
atomicidad esperada cuando la BD la garantiza.

Correr con:
    pytest tests/test_idempotency_unit.py -v
"""
import threading
from typing import Optional
from unittest.mock import MagicMock, patch

import psycopg
import psycopg.errors
import pytest

from idempotency import try_register_event, init_idempotency_db


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_mock_conn(rowcount: int = 1, raise_exc: Optional[Exception] = None):
    """
    Construye un mock de conexion psycopg que simula:
      - rowcount: numero de filas afectadas por el INSERT.
      - raise_exc: excepcion que lanzara cur.execute() si no es None.
    """
    mock_cur = MagicMock()
    mock_cur.__enter__ = MagicMock(return_value=mock_cur)
    mock_cur.__exit__ = MagicMock(return_value=False)
    mock_cur.rowcount = rowcount
    if raise_exc is not None:
        mock_cur.execute.side_effect = raise_exc

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


# ─── Tests de try_register_event ─────────────────────────────────────────────

class TestTryRegisterEvent:
    """Pruebas unitarias de la funcion try_register_event."""

    @patch("idempotency._get_connection")
    def test_retorna_true_en_insercion_exitosa(self, mock_get_conn):
        """Cuando rowcount == 1, el evento es nuevo -> debe retornar True."""
        mock_conn, _ = _make_mock_conn(rowcount=1)
        mock_get_conn.return_value = mock_conn

        resultado = try_register_event("q.m4.experience", "evt-001")

        assert resultado is True

    @patch("idempotency._get_connection")
    def test_retorna_false_si_ya_existia(self, mock_get_conn):
        """Cuando rowcount == 0 (ON CONFLICT DO NOTHING), retorna False."""
        mock_conn, _ = _make_mock_conn(rowcount=0)
        mock_get_conn.return_value = mock_conn

        resultado = try_register_event("q.m4.experience", "evt-001")

        assert resultado is False

    @patch("idempotency._get_connection")
    def test_captura_unique_violation_y_retorna_false(self, mock_get_conn):
        """
        Si psycopg lanza UniqueViolation (ruta defensiva sin ON CONFLICT),
        la funcion la captura limpiamente y retorna False.
        """
        exc = psycopg.errors.UniqueViolation("duplicate key value violates unique constraint")
        mock_conn, _ = _make_mock_conn(raise_exc=exc)
        mock_get_conn.return_value = mock_conn

        resultado = try_register_event("q.m3.publish", "evt-999")

        assert resultado is False

    @patch("idempotency._get_connection")
    def test_llama_insert_con_parametros_correctos(self, mock_get_conn):
        """El INSERT debe enviarse con (event_id, consumer_id) en ese orden."""
        mock_conn, mock_cur = _make_mock_conn(rowcount=1)
        mock_get_conn.return_value = mock_conn

        try_register_event("q.m4.experience", "evt-abc-123")

        # Verificamos que execute fue llamado con los argumentos correctos.
        # El primer argumento es la sentencia SQL (ignorado aqui),
        # el segundo es la tupla (event_id, consumer_id).
        args, _ = mock_cur.execute.call_args
        assert args[1] == ("evt-abc-123", "q.m4.experience")

    @patch("idempotency._get_connection")
    def test_hace_commit_despues_de_insert(self, mock_get_conn):
        """Debe llamarse conn.commit() para persistir la transaccion."""
        mock_conn, _ = _make_mock_conn(rowcount=1)
        mock_get_conn.return_value = mock_conn

        try_register_event("q.m3.publish", "evt-xyz")

        mock_conn.commit.assert_called_once()

    def test_lanza_valueerror_si_consumer_id_vacio(self):
        """consumer_id vacio debe levantar ValueError antes de tocar la BD."""
        with pytest.raises(ValueError, match="consumer_id"):
            try_register_event("", "evt-001")

    def test_lanza_valueerror_si_event_id_vacio(self):
        """event_id vacio debe levantar ValueError antes de tocar la BD."""
        with pytest.raises(ValueError, match="event_id"):
            try_register_event("q.m4.experience", "")

    @patch("idempotency._get_connection")
    def test_mismo_evento_distintos_consumidores(self, mock_get_conn):
        """
        El mismo event_id con distintos consumer_id son registros
        independientes; cada llamada debe ser exitosa (True).
        """
        mock_conn_1, _ = _make_mock_conn(rowcount=1)
        mock_conn_2, _ = _make_mock_conn(rowcount=1)
        mock_get_conn.side_effect = [mock_conn_1, mock_conn_2]

        r1 = try_register_event("q.m4.experience", "evt-shared")
        r2 = try_register_event("q.m3.publish", "evt-shared")

        assert r1 is True
        assert r2 is True


# ─── Test de concurrencia con Mocks ──────────────────────────────────────────

class TestConcurrenciaMock:
    """
    Simula acceso concurrente a try_register_event usando threading.

    Como el mock es el arbitro (no la BD real), este test valida que:
      1. La funcion no tiene estado compartido (variables globales/mutables)
         que pueda corromperse entre hilos.
      2. Cada hilo recibe el resultado que le corresponde segun el rowcount
         devuelto por su propio mock.
    """

    @patch("idempotency._get_connection")
    def test_hilos_concurrentes_no_comparten_estado(self, mock_get_conn):
        """
        N hilos llaman try_register_event simultaneamente.
        Se simula que: hilo 0 inserta con exito (rowcount=1),
                       hilos 1..N-1 encuentran conflicto (rowcount=0).
        Verifica que exactamente un resultado sea True.
        """
        N = 20
        resultados: list[Optional[bool]] = [None] * N
        barrier = threading.Barrier(N)  # sincroniza el disparo simultaneo

        # Primer hilo exitoso, el resto con conflicto.
        mocks = [_make_mock_conn(rowcount=(1 if i == 0 else 0))[0] for i in range(N)]
        mock_get_conn.side_effect = mocks

        def worker(idx: int):
            barrier.wait()  # espera a que TODOS esten listos
            resultados[idx] = try_register_event("q.m4.experience", "evt-concurrent")

        hilos = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()

        assert resultados.count(True) == 1
        assert resultados.count(False) == N - 1


# ─── Tests de init_idempotency_db ────────────────────────────────────────────

class TestInitIdempotencyDb:
    """Verifica que init_idempotency_db ejecuta el DDL esperado."""

    @patch("idempotency._get_connection")
    def test_ejecuta_create_table_y_create_index(self, mock_get_conn):
        """Debe ejecutar exactamente dos sentencias DDL (tabla + indice)."""
        mock_conn, mock_cur = _make_mock_conn(rowcount=0)
        mock_get_conn.return_value = mock_conn

        init_idempotency_db()

        assert mock_cur.execute.call_count == 2
        # Primera llamada: CREATE TABLE
        first_sql = mock_cur.execute.call_args_list[0][0][0]
        assert "processed_events" in first_sql
        # Segunda llamada: CREATE INDEX
        second_sql = mock_cur.execute.call_args_list[1][0][0]
        assert "idx_processed_events_event_id" in second_sql

    @patch("idempotency._get_connection")
    def test_hace_commit_al_finalizar(self, mock_get_conn):
        """El commit debe llamarse tras ejecutar el DDL."""
        mock_conn, _ = _make_mock_conn(rowcount=0)
        mock_get_conn.return_value = mock_conn

        init_idempotency_db()

        mock_conn.commit.assert_called_once()

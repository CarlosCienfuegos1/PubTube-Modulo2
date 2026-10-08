"""
idempotency.py — Repositorio de idempotencia atómica y middleware (US-B4a / US-B4b).

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

Uso básico (US-B4a):
    from idempotency import init_idempotency_db, try_register_event

    init_idempotency_db()                   # una vez al arrancar
    new = try_register_event("q.m4.experience", "550e8400-...")
    if new:
        # procesar el evento
        ...
    else:
        # duplicado → hacer ack y descartar
        ...

Uso recomendado — middleware automático en subscribe_events (US-B4b):
    from event_bus import subscribe_events

    def mi_handler(envelope):
        # Firma limpia: solo el sobre del evento.
        # El SDK gestiona el ACK/NACK y la idempotencia de forma transparente.
        print(f"Procesando: {envelope.payload['contentId']}")

    subscribe_events(
        queue="q.m4.experience",
        routing_keys=["m1.video.uploaded"],
        on_event=mi_handler,
        idempotent=True,  # activa el middleware (True por defecto)
    )
"""
import functools
import inspect
import logging
from typing import Callable

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
        connect_timeout=2,
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


# ─── Middleware de idempotencia (US-B4b) ─────────────────────────────────────

def make_idempotent_middleware(consumer_id: str, handler_fn: Callable) -> Callable:
    """
    Envuelve un handler de negocio con el flujo de decision de idempotencia.

    Esta es la funcion central del middleware transparente. Es invocada
    internamente por `subscribe_events` cuando `idempotent=True` (el valor
    por defecto). El handler de negocio recibe SOLO el envelope; el SDK se
    ocupa del ACK/NACK y de consultar la capa de persistencia.

    Flujo de decision para cada mensaje recibido:
        1. Extraer el event_id del EventEnvelope.
        2. Consultar atomicamente la capa de persistencia (try_register_event).
        3. Si el evento es DUPLICADO: enviar ACK inmediato y no ejecutar el handler.
        4. Si el evento es NUEVO: ejecutar el handler, y solo si tiene exito, ACK.
           Si el handler lanza una excepcion, enviar NACK sin requeue.

    Args:
        consumer_id: Identificador unico del consumidor/cola,
                     ej: "q.m4.experience". Permite que el mismo event_id
                     sea procesado una vez por cada consumidor distinto.
        handler_fn:  Funcion de negocio con firma: handler(envelope) -> None.
                     No necesita conocer nada de RabbitMQ.

    Returns:
        Un callable interno con firma (envelope, channel, method) listo para
        ser usado como callback de `subscribe_events`.

    Raises:
        ValueError: si consumer_id es vacio.
    """
    if not consumer_id or not consumer_id.strip():
        raise ValueError(
            "consumer_id no puede ser vacio. "
            "Usa el nombre de la cola, ej: 'q.m4.experience'."
        )

    try:
        n_params = len(inspect.signature(handler_fn).parameters)
    except (ValueError, TypeError):
        n_params = 1

    @functools.wraps(handler_fn)
    def _middleware(envelope, channel, method) -> None:
        """
        Wrapper interno del middleware: recibe los tres argumentos de bajo nivel
        de pika pero expone la firma requerida al handler de negocio.
        """
        event_id: str = str(envelope.id)

        # ── Paso 1: Consulta atomica a la capa de persistencia ────────────────
        # try_register_event intenta insertar (event_id, consumer_id) en BD.
        # Retorna True si era nuevo (insertado), False si ya existia (duplicado).
        is_new = try_register_event(consumer_id, event_id)

        if not is_new:
            # ── Ruta DUPLICADO ────────────────────────────────────────────────
            # El evento ya fue procesado anteriormente por este consumidor.
            # Omitimos la logica de negocio y enviamos ACK inmediato para
            # que RabbitMQ elimine el mensaje de la cola sin reencolar.
            logger.info(
                "[idempotency] Evento duplicado detectado — omitiendo handler. "
                "event_id=%s consumer_id=%s",
                event_id, consumer_id,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # ── Ruta NUEVO ────────────────────────────────────────────────────────
        # El evento es nuevo para este consumidor. Ejecutamos la logica de
        # negocio. Si el handler lanza una excepcion, enviamos NACK sin
        # requeue para evitar un loop infinito de reintentos.
        try:
            logger.info(
                "[idempotency] Evento nuevo — ejecutando handler. "
                "event_id=%s consumer_id=%s handler=%s",
                event_id, consumer_id, handler_fn.__name__,
            )
            if n_params == 1:
                handler_fn(envelope)
            elif n_params == 2:
                handler_fn(envelope, channel)
            else:
                handler_fn(envelope, channel, method)

            # ── ACK solo tras exito del handler ───────────────────────────────
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as exc:
            # El handler de negocio fallo. Enviamos NACK sin requeue para
            # evitar que el mensaje quede atrapado en un loop de error
            # (el evento ya esta registrado como procesado en la BD).
            logger.error(
                "[idempotency] Handler '%s' fallo para event_id=%s: %s. "
                "Enviando NACK sin requeue.",
                handler_fn.__name__, event_id, exc,
            )
            channel.basic_nack(
                delivery_tag=method.delivery_tag, requeue=False
            )

    return _middleware


def idempotent_handler(consumer_id: str) -> Callable[[Callable], Callable]:
    """
    Decorador explicito opcional (US-B4b) para envolver handlers con idempotencia.

    Permite el uso como decorador tradicional:
        @idempotent_handler("q.m4.experience")
        def mi_handler(envelope):
            ...

    Internamente delega en `make_idempotent_middleware(consumer_id, fn)`.
    """
    def decorator(fn: Callable) -> Callable:
        return make_idempotent_middleware(consumer_id, fn)
    return decorator

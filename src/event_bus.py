"""
event_bus.py — SDK interno de publicación/suscripción de eventos (US-B1b).

Este módulo es la librería cliente central de PubTube-Modulo2. Cualquier módulo
del sistema (M1, M3, M4, etc.) lo importa para publicar y consumir eventos del
exchange 'pubtube.events' sin necesidad de escribir código de bajo nivel de pika
ni gestionar la reconexión manualmente.

Diseño:
    - publish_event(): publica un evento validado al exchange topic.
    - subscribe_events(): bloquea el hilo y consume eventos de una cola,
      reconectándose automáticamente si el broker cae.

Ambas funciones manejan la reconexión con retroceso exponencial, lo que significa
que si el broker RabbitMQ está caído, el cliente esperará 1s, 2s, 4s... antes de
reintentar, evitando saturar la red con reintentos agresivos.
"""

import time
import logging
from typing import Any, Callable, Dict, List, Optional

import pika
import pika.exceptions
from pydantic import ValidationError

from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
from envelope import EventEnvelope

# ─── Constantes de configuración ─────────────────────────────────────────────

EXCHANGE_NAME = "pubtube.events"
EXCHANGE_TYPE = "topic"

# Número máximo de reintentos antes de rendirse
MAX_RETRIES = 10

# Tiempo inicial de espera entre reintentos (en segundos)
INITIAL_BACKOFF = 1.0

# Factor multiplicador del backoff exponencial (cada intento espera el doble)
BACKOFF_FACTOR = 2.0

# Techo máximo de espera entre reintentos (evita esperar minutos enteros)
MAX_BACKOFF = 30.0

logger = logging.getLogger(__name__)


# ─── Función interna: crear conexión pika ────────────────────────────────────

def _build_connection_params() -> pika.ConnectionParameters:
    """
    Construye los parámetros de conexión a RabbitMQ usando la config del .env.

    Retorna un objeto ConnectionParameters listo para pasarle a
    pika.BlockingConnection(). Centralizar esto aquí garantiza que toda la
    librería use siempre las mismas credenciales y host definidos en el .env.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        # heartbeat: RabbitMQ envía pings cada N segundos para detectar
        # conexiones muertas. 60s es un valor seguro para LANs.
        heartbeat=60,
        # blocked_connection_timeout: si el broker bloquea la conexión porque
        # está saturado de memoria, lanza una excepción tras este tiempo.
        blocked_connection_timeout=300,
    )


def _connect_with_retry(max_retries: int = MAX_RETRIES) -> pika.BlockingConnection:
    """
    Intenta conectarse al broker con retroceso exponencial.

    Si la conexión falla (broker caído, credenciales incorrectas en ese momento,
    red inestable), espera INITIAL_BACKOFF segundos, luego el doble, el doble
    de eso, etc., hasta MAX_BACKOFF o hasta MAX_RETRIES intentos.

    Args:
        max_retries: número de intentos antes de lanzar la excepción.

    Returns:
        Una pika.BlockingConnection activa.

    Raises:
        pika.exceptions.AMQPConnectionError: si se agotaron los reintentos.
    """
    backoff = INITIAL_BACKOFF
    last_error: Exception = pika.exceptions.AMQPConnectionError("No se pudo conectar.")

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Intentando conectar a RabbitMQ en %s:%s (intento %d/%d)…",
                RABBITMQ_HOST, RABBITMQ_PORT, attempt, max_retries,
            )
            conn = pika.BlockingConnection(_build_connection_params())
            logger.info("Conexión a RabbitMQ establecida.")
            return conn
        except pika.exceptions.AMQPConnectionError as exc:
            last_error = exc
            wait = min(backoff, MAX_BACKOFF)
            logger.warning(
                "Conexión fallida (intento %d/%d). Reintentando en %.1fs…",
                attempt, max_retries, wait,
            )
            time.sleep(wait)
            backoff *= BACKOFF_FACTOR

    raise last_error


# ─── API pública ──────────────────────────────────────────────────────────────

def publish_event(
    routing_key: str,
    payload: Dict[str, Any],
    correlation_id: str,
    causation_id: Optional[str] = None,
    source: str = "module-2",
) -> EventEnvelope:
    """
    Publica un evento al exchange 'pubtube.events'.

    Esta función construye un EventEnvelope validado (US-B2a) y lo serializa
    a JSON antes de publicarlo. Si el broker no está disponible, reintenta
    automáticamente con retroceso exponencial.

    Args:
        routing_key:    La routing key del evento según la convención
                        '<módulo>.<entidad>.<evento>', ej: 'm1.video.uploaded'.
        payload:        Diccionario con los datos del evento (body del mensaje).
        correlation_id: ID que une todos los eventos de un mismo flujo de negocio
                        (por ejemplo, el ID de la Saga de publicación de un video).
        causation_id:   (Opcional) ID del evento que provocó este evento. Permite
                        trazar la cadena causal entre eventos.
        source:         Identificador del módulo que emite el evento.

    Returns:
        El EventEnvelope que se publicó (útil para logging o pruebas).

    Raises:
        pydantic.ValidationError: si el payload no pasa la validación del esquema.
        pika.exceptions.AMQPConnectionError: si se agotaron los reintentos.

    Ejemplo de uso:
        from event_bus import publish_event

        envelope = publish_event(
            routing_key="m1.video.uploaded",
            payload={"contentId": "abc123", "storageUrl": "s3://..."},
            correlation_id="saga-xyz-001",
        )
        print(f"Evento publicado: {envelope.id}")
    """
    # Validamos el envelope antes de intentar publicar.
    # Si el payload es inválido, EventEnvelope lanza ValidationError aquí,
    # lo que cumple el criterio: "un evento malformado se rechaza antes de
    # publicarse" (US-B2a).
    if not correlation_id or not correlation_id.strip():
        raise ValueError(
            "'correlation_id' es obligatorio y no puede estar vacío. "
            "Debe ser un UUID o un identificador único del flujo de negocio."
        )
    if not routing_key or not routing_key.strip():
        raise ValueError("'routing_key' es obligatoria y no puede estar vacía.")

    envelope = EventEnvelope(
        type=routing_key,
        correlationId=correlation_id,
        causationId=causation_id,
        source=source,
        payload=payload,
    )
    body = envelope.serialize()

    # _connect_with_retry ya maneja los reintentos de conexión con backoff.
    # Si el canal o la publicación falla después de conectar (error de red),
    # propagamos directamente ya que es un error puntual tras una conexión exitosa.
    conn = _connect_with_retry()
    channel = conn.channel()
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,           # persistente: sobrevive a reinicios del broker
            content_type="application/json",
            correlation_id=correlation_id,
        ),
    )
    conn.close()
    logger.info("Evento '%s' publicado. ID: %s", routing_key, envelope.id)
    return envelope


def subscribe_events(
    queue: str,
    routing_keys: List[str],
    on_event: Callable[[EventEnvelope, Any], None],
    auto_ack: bool = False,
) -> None:
    """
    Suscribe a una o más routing keys y comienza a consumir eventos de forma
    indefinida. Si el broker cae, reconecta automáticamente con retroceso
    exponencial y retoma el consumo.

    Args:
        queue:        Nombre de la cola durable a consumir (ej: 'q.m4.experience').
                      Debe existir ya en el broker (definida en definitions.json)
                      o se declarará idempotentemente en la primera conexión.
        routing_keys: Lista de routing keys a vincular a la cola, ej:
                      ['m1.video.uploaded', 'm3.publish.completed'].
        on_event:     Función callback que se llama por cada evento recibido.
                      Firma: on_event(envelope: EventEnvelope, channel: Any) -> None.
                      El canal se pasa para que el callback pueda hacer ACK manual.
        auto_ack:     Si True, el broker marca el mensaje como procesado apenas lo
                      entrega (at-most-once). Si False (default), el callback debe
                      hacer ch.basic_ack(method.delivery_tag) manualmente
                      (at-least-once, más seguro en producción).

    Raises:
        Exception: propaga cualquier excepción no recuperable del callback.

    Ejemplo de uso:
        from event_bus import subscribe_events
        from envelope import EventEnvelope

        def manejar_video(envelope: EventEnvelope, channel) -> None:
            print(f"Video subido: {envelope.payload['contentId']}")
            channel.basic_ack(delivery_tag=???)  # viene del method_frame

        subscribe_events(
            queue="q.m4.experience",
            routing_keys=["m1.video.uploaded", "m3.publish.completed"],
            on_event=manejar_video,
        )
    """
    while True:
        try:
            conn = _connect_with_retry()
            channel = conn.channel()

            # Declaramos la cola idempotentemente (si ya existe, no hace nada)
            channel.queue_declare(queue=queue, durable=True)

            # Vinculamos cada routing key a la cola
            channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True,
            )
            for rk in routing_keys:
                channel.queue_bind(
                    exchange=EXCHANGE_NAME,
                    queue=queue,
                    routing_key=rk,
                )
                logger.info("Cola '%s' vinculada a routing key '%s'.", queue, rk)

            # prefetch_count=1: RabbitMQ solo entrega UN mensaje a la vez.
            # El siguiente mensaje no se entrega hasta que el actual tenga ACK.
            # Esto garantiza que el consumidor no se sature si procesa lento.
            channel.basic_qos(prefetch_count=1)

            def _on_message(ch, method, properties, body):
                """
                Callback interno que intercepta cada mensaje raw de pika,
                lo deserializa y valida como EventEnvelope, y llama al
                callback del usuario (on_event).
                """
                try:
                    envelope = EventEnvelope.deserialize(body.decode("utf-8"))
                except (ValidationError, ValueError) as exc:
                    # Si el mensaje no es un EventEnvelope válido, lo descartamos
                    # con NACK sin requeue para no bloquear la cola con basura.
                    logger.error(
                        "Mensaje inválido descartado (routing_key=%s): %s",
                        method.routing_key, exc,
                    )
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    return

                logger.info(
                    "Evento recibido: type='%s', id='%s'",
                    envelope.type, envelope.id,
                )
                # Pasamos el canal para que el callback haga ACK manual si auto_ack=False
                on_event(envelope, ch)

                if auto_ack:
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue=queue,
                on_message_callback=_on_message,
                auto_ack=False,  # siempre False internamente; el ACK lo controla _on_message
            )

            logger.info(
                "Suscriptor iniciado. Escuchando en cola '%s'. Presiona Ctrl+C para salir.",
                queue,
            )
            channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("Suscriptor detenido por el usuario (Ctrl+C).")
            break

        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError,
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPHeartbeatTimeout,
        ) as exc:
            # Error de red o broker caído: reconectamos automáticamente
            logger.warning(
                "Conexión perdida con el broker: %s. Reconectando en %.1fs…",
                exc, INITIAL_BACKOFF,
            )
            time.sleep(INITIAL_BACKOFF)
            # El bucle while True se encarga de reintentar

        except Exception as exc:
            # Error inesperado en el callback del usuario: lo propagamos
            logger.error("Error inesperado en el consumidor: %s", exc)
            raise

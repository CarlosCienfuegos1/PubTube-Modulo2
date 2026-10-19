"""Primitivas compartidas para reintentos, backoff y dead-letter queues."""

import logging
from typing import Any, Optional

import pika

EXCHANGE_NAME = "pubtube.events"
RETRY_EXCHANGE = "pubtube.events.retry"
DLQ_EXCHANGE = "pubtube.events.dlq"
MAX_DELIVERY_ATTEMPTS = 3
RETRY_INITIAL_BACKOFF_MS = 1000
RETRY_BACKOFF_FACTOR = 2
RETRY_MAX_BACKOFF_MS = 30000

logger = logging.getLogger(__name__)


def retry_queue_name(queue: str) -> str:
    return f"{queue}.retry"


def dlq_name(queue: str) -> str:
    return f"{queue}.dlq"


def delivery_attempt(properties: Any, queue: str) -> int:
    """Devuelve el número de entrega actual usando el header AMQP x-death."""
    headers = getattr(properties, "headers", None) or {}
    deaths = headers.get("x-death", [])
    retries = sum(
        int(death.get("count", 0))
        for death in deaths
        if death.get("queue") == queue
    )
    return retries + 1


def retry_delay_ms(attempt: int) -> int:
    delay = RETRY_INITIAL_BACKOFF_MS * (RETRY_BACKOFF_FACTOR ** max(attempt - 1, 0))
    return min(delay, RETRY_MAX_BACKOFF_MS)


def _copy_properties(properties: Any, *, headers: Optional[dict] = None, expiration: Optional[str] = None):
    return pika.BasicProperties(
        content_type=getattr(properties, "content_type", "application/json"),
        content_encoding=getattr(properties, "content_encoding", None),
        delivery_mode=getattr(properties, "delivery_mode", 2),
        headers=headers if headers is not None else (getattr(properties, "headers", None) or {}),
        correlation_id=getattr(properties, "correlation_id", None),
        message_id=getattr(properties, "message_id", None),
        type=getattr(properties, "type", None),
        app_id=getattr(properties, "app_id", None),
        expiration=expiration,
    )


def route_failed_message(channel: Any, queue: str, method: Any, properties: Any, body: bytes, error: Exception) -> bool:
    """Envía un fallo al retry queue o a la DLQ y confirma el original.

    Retorna True cuando se agotó el límite y el mensaje terminó en la DLQ.
    Si publicar la copia falla, la excepción se propaga y RabbitMQ podrá
    redeliverar el mensaje al cerrarse la conexión.
    """
    attempt = delivery_attempt(properties, queue)
    if attempt >= MAX_DELIVERY_ATTEMPTS:
        destination = DLQ_EXCHANGE
        routing_key = queue
        expiration = None
        exhausted = True
        logger.error("Mensaje enviado a DLQ tras %d intentos (cola=%s): %s", attempt, queue, error)
    else:
        destination = RETRY_EXCHANGE
        routing_key = queue
        expiration = str(retry_delay_ms(attempt))
        exhausted = False
        logger.warning("Reintentando mensaje %d/%d en %sms (cola=%s): %s", attempt, MAX_DELIVERY_ATTEMPTS, expiration, queue, error)

    channel.basic_publish(
        exchange=destination,
        routing_key=routing_key,
        body=body,
        properties=_copy_properties(properties, expiration=expiration),
    )
    channel.basic_ack(delivery_tag=method.delivery_tag)
    return exhausted

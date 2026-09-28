"""Consumidor de inspección para registrar y reprocesar fallos de una DLQ.

Uso: ``python src/dlq_inspector.py q.m4.experience``
Añade ``--reprocess`` para publicar el mensaje nuevamente al exchange principal.
"""

import argparse
import logging
from typing import Any

import pika

from event_bus import _build_connection_params
from reliability import EXCHANGE_NAME, dlq_name

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def consume_dlq(queue: str, reprocess: bool = False) -> None:
    connection = pika.BlockingConnection(_build_connection_params())
    channel = connection.channel()
    dead_letter_queue = dlq_name(queue)
    channel.queue_declare(queue=dead_letter_queue, durable=True)

    def on_message(ch: Any, method: Any, properties: Any, body: bytes) -> None:
        logger.error(
            "Fallo crítico en DLQ %s (routing_key=%s, headers=%s, body=%s)",
            dead_letter_queue,
            method.routing_key,
            getattr(properties, "headers", None),
            body.decode("utf-8", errors="replace"),
        )
        if reprocess:
            ch.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=method.routing_key,
                body=body,
                properties=properties,
            )
            logger.warning("Mensaje reprocesado desde %s", dead_letter_queue)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=dead_letter_queue, on_message_callback=on_message, auto_ack=False)
    logger.info("Inspeccionando %s. Ctrl+C para salir.", dead_letter_queue)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspecciona una dead-letter queue")
    parser.add_argument("queue", help="Cola original, por ejemplo q.m4.experience")
    parser.add_argument("--reprocess", action="store_true", help="Republicar mensajes al exchange principal")
    args = parser.parse_args()
    consume_dlq(args.queue, args.reprocess)
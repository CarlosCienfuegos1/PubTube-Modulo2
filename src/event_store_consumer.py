"""
Consumidor del Event Store v1.

Escucha la cola q.m2.event_store (bindeada a '#', recibe TODOS los eventos
que pasan por el exchange pubtube.events) y persiste cada uno en PostgreSQL
para auditoría y trazabilidad.

Correr con:
    python src/event_store_consumer.py
"""
import pika

from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
from envelope import EventEnvelope
from event_store import init_db, insert_event

QUEUE_NAME = "q.m2.event_store"


def callback(ch, method, properties, body):
    try:
        envelope = EventEnvelope.model_validate_json(body)
        insertado = insert_event(envelope, method.routing_key)

        if insertado:
            print(f"[event_store] Persistido: {envelope.type} (id={envelope.id}, "
                  f"correlationId={envelope.correlationId})")
        else:
            print(f"[event_store] Duplicado ignorado: {envelope.type} (id={envelope.id})")

        # Ack solo después de persistir con éxito (o confirmar que ya existía).
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as exc:
        # Si falla la validación o la persistencia,
        # vuelve a la cola para reintentar en vez de perderse silenciosamente.
        print(f"[event_store] ERROR al procesar evento (routing_key={method.routing_key}): {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    print("[event_store] Inicializando esquema de base de datos...")
    init_db()
    print("[event_store] Esquema listo.")

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    print(f"[event_store] Escuchando TODOS los eventos en '{QUEUE_NAME}'. Presiona CTRL+C para salir.")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=False)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[event_store] Saliendo...")
        channel.stop_consuming()

    connection.close()


if __name__ == "__main__":
    main()  
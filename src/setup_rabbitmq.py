import pika
from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
from reliability import (
    DLQ_EXCHANGE,
    EXCHANGE_NAME,
    RETRY_EXCHANGE,
    RETRY_MAX_BACKOFF_MS,
    dlq_name,
    retry_queue_name,
)


def setup():
    # Configurar credenciales
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )

    print(f"Conectando a RabbitMQ en {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # Crear el Exchange principal
    exchange_name = EXCHANGE_NAME
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type='topic',
        durable=True  # Durable para que sobreviva a reinicios
    )
    channel.exchange_declare(exchange=RETRY_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLQ_EXCHANGE, exchange_type="direct", durable=True)
    print(f"Exchange '{exchange_name}' de tipo 'topic' creado o verificado exitosamente.")


    # Declarar colas y bindings por consumidor oficial (coincidente con definitions.json)
    queues_bindings = [
        ("q.m1.content", ["m3.publish.completed"]),
        ("q.m3.publish", ["m1.metadata.updated"]),
        ("q.m4.experience", [
            "m1.video.uploaded",
            "m1.metadata.updated",
            "m3.publish.scheduled",
            "m3.publish.completed",
            "m3.publish.failed",
            "m4.team.notified"
        ]),
        ("q.m2.event_store", ["#"])
    ]

    for queue_name, routing_keys in queues_bindings:
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": RETRY_EXCHANGE,
                "x-dead-letter-routing-key": queue_name,
            },
        )
        for rk in routing_keys:
            channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=rk)
        print(f"Cola '{queue_name}' configurada con bindings: {routing_keys}")

        channel.queue_declare(
            queue=retry_queue_name(queue_name),
            durable=True,
            arguments={
                "x-message-ttl": RETRY_MAX_BACKOFF_MS,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )
        channel.queue_bind(
            exchange=RETRY_EXCHANGE,
            queue=retry_queue_name(queue_name),
            routing_key=queue_name,
        )
        channel.queue_declare(queue=dlq_name(queue_name), durable=True)
        channel.queue_bind(
            exchange=DLQ_EXCHANGE,
            queue=dlq_name(queue_name),
            routing_key=queue_name,
        )

    connection.close()
    print("Configuración completada exitosamente.")


if __name__ == "__main__":
    setup()
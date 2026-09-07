import pika
from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS


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
    exchange_name = 'pubtube.events'
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type='topic',
        durable=True  # Durable para que sobreviva a reinicios
    )
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
        channel.queue_declare(queue=queue_name, durable=True)
        for rk in routing_keys:
            channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=rk)
        print(f"Cola '{queue_name}' configurada con bindings: {routing_keys}")

    connection.close()
    print("Configuración completada exitosamente.")


if __name__ == "__main__":
    setup()
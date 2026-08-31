import pika
import os
from dotenv import load_dotenv

load_dotenv()

# Fail-fast: si falta alguna variable, el script debe fallar explícitamente
# en vez de conectarse silenciosamente con credenciales por defecto.
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = int(os.environ["RABBITMQ_PORT"])
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASS = os.environ["RABBITMQ_PASS"]


def callback(ch, method, properties, body):
    print(f"\n[x] Evento recibido (Routing Key: {method.routing_key})")
    print(f"    Payload raw: {body.decode()}")
    print("    - Listo para ser deserializado y procesado -")

    # Ack manual: solo confirmamos el mensaje después de procesarlo con éxito.
    # Con auto_ack=True, RabbitMQ da el mensaje por entregado apenas lo envía,
    # y si el consumidor se cae antes de terminar de procesarlo, el mensaje
    # se pierde. Esto viola la garantía "at-least-once" que pide el proyecto.
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_events():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    queue_name = 'test_queue'
    print(f"[*] Esperando eventos en '{queue_name}'. Presiona CTRL+C para salir.")

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False  # ack manual, ver callback()
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nSaliendo...")
        channel.stop_consuming()

    connection.close()


if __name__ == "__main__":
    consume_events()

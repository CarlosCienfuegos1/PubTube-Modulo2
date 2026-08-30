import pika
import os
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

# Fail-fast: si falta alguna variable, el script debe fallar explícitamente
# en vez de conectarse silenciosamente con credenciales por defecto.
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = int(os.environ["RABBITMQ_PORT"])
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASS = os.environ["RABBITMQ_PASS"]


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


    # las colas por consumidor real
    # se implementan en Sprint 1.
    queue_name = 'test_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='#')
    print(f"Cola '{queue_name}' enlazada al exchange para escuchar todo ('#').")

    connection.close()
    print("Configuración completada.")


if __name__ == "__main__":
    setup()
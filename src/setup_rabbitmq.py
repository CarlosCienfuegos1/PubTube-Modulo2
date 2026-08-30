import pika
import os
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin123")

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

    # Opcionalmente, para pruebas rápidas podemos crear una cola genérica
    queue_name = 'test_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='#')
    print(f"Cola '{queue_name}' enlazada al exchange para escuchar todo ('#').")

    connection.close()
    print("Configuración completada.")

if __name__ == "__main__":
    setup()

import pika
import os
import uuid
from dotenv import load_dotenv
from envelope import EventEnvelope

load_dotenv()

# Fail-fast: si falta alguna variable, el script debe fallar explícitamente
# en vez de conectarse silenciosamente con credenciales por defecto.
RABBITMQ_HOST = os.environ["RABBITMQ_HOST"]
RABBITMQ_PORT = int(os.environ["RABBITMQ_PORT"])
RABBITMQ_USER = os.environ["RABBITMQ_USER"]
RABBITMQ_PASS = os.environ["RABBITMQ_PASS"]


def publish_event():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # 1. Crear el objeto con el Envelope acordado
    evento = EventEnvelope(
        type="video.uploaded",
        correlationId=str(uuid.uuid4()),  # Generamos un ID de flujo simulado
        payload={
            "contentId": "video_12345",
            "checksum": "a8f5f167f44f...",
            "storageUrl": "https://s3.demo.com/videos/video_12345.mp4"
        }
    )

    # 2. Publicar al Exchange usando el modelo convertido a JSON.
    # La routing key debe coincidir exactamente con el catálogo de eventos
    # acordado (sección 6.1 del documento del proyecto): "video.uploaded"
    routing_key = "video.uploaded"

    channel.basic_publish(
        exchange='pubtube.events',
        routing_key=routing_key,
        body=evento.model_dump_json(),  # Pydantic serializa a JSON directamente
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent: sobrevive a un reinicio del broker
            content_type='application/json'
        )
    )

    print(f"[x] Evento enviado a '{routing_key}' con correlationId: {evento.correlationId}")
    connection.close()


if __name__ == "__main__":
    publish_event()


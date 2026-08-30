import pika
import os
import uuid
from dotenv import load_dotenv
from envelope import EventEnvelope

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "admin123")

def publish_event():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # 1. Crear el objeto con el Envelope acordado
    evento = EventEnvelope(
        type="video.uploaded",
        correlationId=str(uuid.uuid4()), # Generamos un ID de flujo simulado
        payload={
            "contentId": "video_12345",
            "checksum": "a8f5f167f44f...",
            "storageUrl": "https://s3.demo.com/videos/video_12345.mp4"
        }
    )

    # 2. Publicar al Exchange usando el modelo convertido a JSON
    routing_key = "module1.video.uploaded"
    channel.basic_publish(
        exchange='pubtube.events',
        routing_key=routing_key,
        body=evento.model_dump_json() # Pydantic serializa a JSON directamente
    )
    print(f"[x] Evento enviado a '{routing_key}' con correlationId: {evento.correlationId}")

    connection.close()

if __name__ == "__main__":
    publish_event()

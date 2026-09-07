import uuid
import pika
from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
from envelope import EventEnvelope


def publish_event():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    # 1. Crear el objeto con el Envelope acordado
    evento = EventEnvelope(
        type="m1.video.uploaded",
        correlationId=str(uuid.uuid4()),  # Generamos un ID de flujo simulado
        payload={
            "contentId": "video_12345",
            "checksum": "a8f5f167f44f...",
            "storageUrl": "https://s3.demo.com/videos/video_12345.mp4"
        }
    )

    # 2. Publicar al Exchange usando el modelo convertido a JSON.
    # La routing key sigue la convención acordada: <módulo>.<entidad>.<evento>
    routing_key = "m1.video.uploaded"

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


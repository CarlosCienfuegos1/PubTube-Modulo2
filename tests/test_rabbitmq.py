"""
Test de integración: prueba el flujo real publish -> exchange -> binding -> consumer.

Requiere que RabbitMQ esté corriendo (docker compose up -d) y que el .env
tenga las credenciales correctas. Si el broker no está disponible, el test
se salta automáticamente en vez de fallar (útil para correr la suite en CI
sin necesitar un broker levantado, si así lo definen más adelante).

Correr con:
    pytest tests/test_integration_rabbitmq.py -v
"""
import uuid
import time
import pika
import pika.exceptions
import pytest

from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS
from envelope import EventEnvelope

EXCHANGE_NAME = "pubtube.events"


def _get_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials
    )
    return pika.BlockingConnection(parameters)


@pytest.fixture
def rabbitmq_channel():
    """Se conecta a RabbitMQ; si no está disponible, salta el test en vez de fallar."""
    try:
        connection = _get_connection()
    except pika.exceptions.AMQPConnectionError:
        pytest.skip("RabbitMQ no está disponible (¿corriste 'docker compose up -d'?)")
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    yield channel
    connection.close()


def test_exchange_pubtube_events_existe(rabbitmq_channel):
    """El exchange debe existir y ser declarable sin error (passive check)."""
    # exchange_declare con passive=True lanza excepción si el exchange no existe
    rabbitmq_channel.exchange_declare(exchange=EXCHANGE_NAME, passive=True)


def test_publish_y_consume_evento_end_to_end(rabbitmq_channel):
    """
    Publica un evento con routing key 'video.uploaded' a una cola de prueba
    exclusiva de este test, y confirma que se puede consumir de vuelta con
    el mismo contenido.
    """
    queue_name = f"test.integration.{uuid.uuid4().hex[:8]}"
    routing_key = "video.uploaded"

    # Cola exclusiva y auto-eliminable para no ensuciar el broker con cada corrida.
    # exclusive=True es obligatorio junto con durable=False en RabbitMQ 4.x:
    # las colas transitorias no-exclusivas ("transient_nonexcl_queues") fueron
    # deprecadas y ya no se permiten por defecto.
    rabbitmq_channel.queue_declare(queue=queue_name, durable=False, exclusive=True, auto_delete=True)
    rabbitmq_channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=routing_key)

    correlation_id = str(uuid.uuid4())
    evento = EventEnvelope(
        type="video.uploaded",
        correlationId=correlation_id,
        payload={"contentId": "test_video"}
    )

    rabbitmq_channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=evento.model_dump_json(),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )

    # Esperar un momento a que el mensaje quede disponible en la cola
    time.sleep(0.5)

    method_frame, header_frame, body = rabbitmq_channel.basic_get(queue=queue_name, auto_ack=True)

    assert method_frame is not None, "No llegó ningún mensaje a la cola de prueba"
    assert method_frame.routing_key == routing_key

    recibido = EventEnvelope.model_validate_json(body)
    assert recibido.correlationId == correlation_id
    assert recibido.type == "video.uploaded"
    assert recibido.payload == {"contentId": "test_video"}
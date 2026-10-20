"""E2E de Saga contra RabbitMQ y PostgreSQL reales.

El test se omite si las dependencias no están disponibles localmente. CI lo
ejecuta después de levantar los servicios Docker.
"""

import os
import time
import uuid

import pika
import psycopg
import pytest

from envelope import EventEnvelope


def _connections_or_skip():
    required = (
        "RABBITMQ_USER",
        "RABBITMQ_PASS",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    )
    if any(not os.getenv(name) for name in required):
        pytest.skip("E2E requiere variables de RabbitMQ y PostgreSQL.")
    from config import (
        POSTGRES_DB,
        POSTGRES_HOST,
        POSTGRES_PASSWORD,
        POSTGRES_PORT,
        POSTGRES_USER,
        RABBITMQ_HOST,
        RABBITMQ_PASS,
        RABBITMQ_PORT,
        RABBITMQ_USER,
    )

    try:
        db = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=2,
        )
        rabbit = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
                blocked_connection_timeout=2,
            )
        )
    except (psycopg.OperationalError, pika.exceptions.AMQPConnectionError) as exc:
        pytest.skip(f"Infraestructura E2E no disponible: {exc}")
    return db, rabbit


def _get_message(channel, queue, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        method, properties, body = channel.basic_get(queue, auto_ack=True)
        if body is not None:
            return EventEnvelope.deserialize(body)
        time.sleep(0.1)
    pytest.fail(f"No llegó un evento a {queue}")


def test_saga_happy_path_usa_rabbitmq_y_postgresql_reales():
    db, rabbit = _connections_or_skip()
    correlation_id = f"e2e-{uuid.uuid4()}"
    input_queue = f"e2e.saga.input.{uuid.uuid4()}"
    command_queue = f"e2e.saga.commands.{uuid.uuid4()}"
    try:
        input_channel = rabbit.channel()
        input_channel.exchange_declare(
            exchange="pubtube.events", exchange_type="topic", durable=True
        )
        for queue in (input_queue, command_queue):
            input_channel.queue_declare(queue=queue, exclusive=True)
        input_channel.queue_bind(
            exchange="pubtube.events", queue=input_queue, routing_key="m3.publish.scheduled"
        )
        input_channel.queue_bind(
            exchange="pubtube.events", queue=input_queue, routing_key="m3.publish.completed"
        )
        input_channel.queue_bind(
            exchange="pubtube.events", queue=input_queue, routing_key="m4.team.notified"
        )
        input_channel.queue_bind(
            exchange="pubtube.events", queue=command_queue, routing_key="m2.publish.execute"
        )
        input_channel.queue_bind(
            exchange="pubtube.events", queue=command_queue, routing_key="m2.notify.requested"
        )

        from event_bus import publish_event
        from saga import SagaState
        from saga_orchestrator import SagaOrchestrator
        from saga_store import SagaRepository

        repository = SagaRepository()
        repository.init_db()
        orchestrator = SagaOrchestrator(
            repository, timeout_seconds=30, max_step_retries=1
        )

        publish_event("m3.publish.scheduled", {"contentId": "e2e-video"}, correlation_id)
        scheduled = _get_message(input_channel, input_queue)
        orchestrator.handle(scheduled)
        command = _get_message(input_channel, command_queue)
        assert command.type == "m2.publish.execute"

        publish_event("m3.publish.completed", {"youtubeVideoId": "yt-e2e"}, correlation_id)
        orchestrator.handle(_get_message(input_channel, input_queue))
        notification = _get_message(input_channel, command_queue)
        assert notification.type == "m2.notify.requested"

        publish_event("m4.team.notified", {"channel": "e2e"}, correlation_id)
        orchestrator.handle(_get_message(input_channel, input_queue))
        assert repository.get(correlation_id).state == SagaState.COMPLETED
    finally:
        with db.cursor() as cursor:
            cursor.execute(
                "DELETE FROM saga_instances WHERE correlation_id = %s",
                (correlation_id,),
            )
        db.commit()
        db.close()
        rabbit.close()

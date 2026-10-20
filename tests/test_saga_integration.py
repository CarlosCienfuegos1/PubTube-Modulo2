"""Pruebas de integración del orquestador con un repositorio y bus en memoria.

El mismo contrato se utiliza en producción con PostgreSQL y RabbitMQ; estas
pruebas no requieren infraestructura externa y corren en cada push/PR.
"""

from datetime import datetime, timedelta, timezone

from envelope import EventEnvelope
from saga import SagaState
from saga_orchestrator import SagaOrchestrator


class MemoryRepository:
    def __init__(self):
        self.items = {}

    def get(self, correlation_id):
        return self.items.get(correlation_id)

    def save(self, saga):
        self.items[saga.correlation_id] = saga

    def due(self, now):
        return [
            saga for saga in self.items.values()
            if saga.deadline is not None and saga.deadline <= now
        ]


def envelope(event_type, correlation_id="integration-1"):
    return EventEnvelope(type=event_type, correlationId=correlation_id, payload={"contentId": "c-1"})


def test_orchestrator_happy_path():
    published = []
    orchestrator = SagaOrchestrator(
        MemoryRepository(),
        timeout_seconds=30,
        publisher=lambda **kwargs: published.append(kwargs),
    )
    orchestrator.handle(envelope("m3.publish.scheduled"))
    orchestrator.handle(envelope("m3.publish.completed"))
    orchestrator.handle(envelope("m4.team.notified"))

    saga = orchestrator.repository.get("integration-1")
    assert saga.state == SagaState.COMPLETED
    assert [item["routing_key"] for item in published] == [
        "m2.publish.execute",
        "m2.notify.requested",
    ]


def test_orchestrator_failure_emits_rollback():
    published = []
    orchestrator = SagaOrchestrator(
        MemoryRepository(),
        max_step_retries=0,
        publisher=lambda **kwargs: published.append(kwargs),
    )
    orchestrator.handle(envelope("publish.scheduled"))
    orchestrator.handle(envelope("publish.failed"))

    assert orchestrator.repository.get("integration-1").state == SagaState.FAILED
    assert published[-1]["routing_key"] == "m2.publish.rollback"


def test_orchestrator_timeout_emits_rollback():
    repository = MemoryRepository()
    published = []
    orchestrator = SagaOrchestrator(
        repository,
        timeout_seconds=30,
        max_step_retries=0,
        publisher=lambda **kwargs: published.append(kwargs),
    )
    orchestrator.handle(envelope("publish.scheduled"))
    saga = repository.get("integration-1")
    saga.deadline = datetime.now(timezone.utc) - timedelta(seconds=1)

    assert orchestrator.expire_due() == 1
    assert saga.state == SagaState.FAILED
    assert published[-1]["routing_key"] == "m2.publish.rollback"

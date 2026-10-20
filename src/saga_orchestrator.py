"""Orquestador de la Saga de publicación."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from envelope import EventEnvelope
from event_bus import publish_event
from saga import SagaInstance
from saga_store import SagaRepository

SAGA_QUEUE = "q.m2.saga"
SAGA_EVENTS = [
    "m3.publish.scheduled",
    "publish.scheduled",
    "m3.publish.completed",
    "publish.completed",
    "m3.publish.failed",
    "publish.failed",
    "m4.team.notified",
    "m4.notify.sent",
    "notify.sent",
    "m4.notify.failed",
    "notify.failed",
    "m4.team.notification.failed",
]


class SagaOrchestrator:
    def __init__(
        self,
        repository: SagaRepository,
        timeout_seconds: Optional[float] = None,
        max_step_retries: Optional[int] = None,
        publisher: Callable[..., EventEnvelope] = publish_event,
    ) -> None:
        self.repository = repository
        self.timeout_seconds = (
            float(os.getenv("SAGA_STEP_TIMEOUT_SECONDS", "300"))
            if timeout_seconds is None
            else timeout_seconds
        )
        self.max_step_retries = (
            int(os.getenv("SAGA_MAX_STEP_RETRIES", "2"))
            if max_step_retries is None
            else max_step_retries
        )
        if self.max_step_retries < 0:
            raise ValueError("max_step_retries no puede ser negativo.")
        self.publisher = publisher

    def handle(self, event: EventEnvelope) -> SagaInstance:
        saga = self.repository.get(event.correlationId)
        if saga is None:
            if event.type not in ("m3.publish.scheduled", "publish.scheduled"):
                raise ValueError(
                    f"No existe una Saga para {event.type} y no es un disparador válido."
                )
            saga = SagaInstance.start(
                event, self.timeout_seconds, self.max_step_retries
            )
            self._emit("m2.publish.execute", event.payload, event)
        else:
            saga.handle_event(
                event,
                self.timeout_seconds,
                self._emit,
                self.max_step_retries,
            )
        self.repository.save(saga)
        return saga

    def expire_due(self, now: Optional[datetime] = None) -> int:
        current = now or datetime.now(timezone.utc)
        count = 0
        for saga in self.repository.due(current):
            if saga.handle_timeout(
                current,
                self._emit,
                self.max_step_retries,
            ):
                self.repository.save(saga)
                count += 1
        return count

    def _emit(self, routing_key: str, payload: Dict[str, Any], causation: EventEnvelope) -> None:
        self.publisher(
            routing_key=routing_key,
            payload=payload,
            correlation_id=causation.correlationId,
            causation_id=causation.id,
        )

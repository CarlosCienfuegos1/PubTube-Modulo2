"""Máquina de estados y reglas de negocio de la Saga de publicación (US-B7)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from envelope import EventEnvelope


class SagaState(str, Enum):
    SCHEDULED = "SCHEDULED"
    PUBLISHING = "PUBLISHING"
    WAITING_NOTIFICATION = "WAITING_NOTIFICATION"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class SagaTransition:
    from_state: Optional[SagaState]
    to_state: SagaState
    reason: str
    at: str


@dataclass
class SagaInstance:
    correlation_id: str
    state: SagaState
    content_id: Optional[str] = None
    deadline: Optional[datetime] = None
    history: List[SagaTransition] = field(default_factory=list)

    @classmethod
    def start(cls, event: EventEnvelope, timeout_seconds: float) -> "SagaInstance":
        if not event.correlationId.strip():
            raise ValueError("La Saga requiere un correlationId no vacío.")
        instance = cls(
            correlation_id=event.correlationId,
            state=SagaState.SCHEDULED,
            content_id=event.payload.get("contentId"),
        )
        instance._transition(SagaState.SCHEDULED, "publish.scheduled")
        instance._move_to(SagaState.PUBLISHING, "publication requested")
        instance._set_deadline(timeout_seconds)
        return instance

    def handle_event(
        self,
        event: EventEnvelope,
        timeout_seconds: float,
        emit: Callable[[str, Dict[str, Any], EventEnvelope], None],
    ) -> None:
        self._ensure_correlation(event)
        if self.state in (SagaState.COMPLETED, SagaState.FAILED):
            return
        if event.type in ("m3.publish.completed", "publish.completed"):
            if self.state != SagaState.PUBLISHING:
                return
            self._move_to(SagaState.WAITING_NOTIFICATION, "publication completed")
            self._set_deadline(timeout_seconds)
            emit(
                "m2.notify.requested",
                {"contentId": self.content_id, "publication": event.payload},
                event,
            )
        elif event.type in ("m3.publish.failed", "publish.failed"):
            self._compensate("publication failed", event, emit)
        elif event.type in (
            "m4.notify.failed",
            "notify.failed",
            "m4.team.notification.failed",
        ):
            if self.state == SagaState.WAITING_NOTIFICATION:
                self._compensate("notification failed", event, emit)
        elif event.type in ("m4.team.notified", "m4.notify.sent", "notify.sent"):
            if self.state != SagaState.WAITING_NOTIFICATION:
                return
            self._move_to(SagaState.COMPLETED, "notification sent")
            self.deadline = None

    def handle_timeout(
        self,
        now: datetime,
        emit: Callable[[str, Dict[str, Any], EventEnvelope], None],
    ) -> bool:
        if (
            self.state in (SagaState.PUBLISHING, SagaState.WAITING_NOTIFICATION)
            and self.deadline is not None
            and now >= self.deadline
        ):
            synthetic = EventEnvelope(
                type="saga.timeout",
                correlationId=self.correlation_id,
                payload={"state": self.state.value},
            )
            self._compensate(f"timeout in {self.state.value}", synthetic, emit)
            return True
        return False

    def _compensate(
        self,
        reason: str,
        causation: EventEnvelope,
        emit: Callable[[str, Dict[str, Any], EventEnvelope], None],
    ) -> None:
        if self.state == SagaState.FAILED:
            return
        previous = self.state
        self._move_to(SagaState.FAILED, reason)
        self.deadline = None
        emit(
            "m2.publish.rollback",
            {
                "contentId": self.content_id,
                "failedState": previous.value,
                "reason": reason,
            },
            causation,
        )

    def _ensure_correlation(self, event: EventEnvelope) -> None:
        if event.correlationId != self.correlation_id:
            raise ValueError("El correlationId del evento no coincide con la Saga.")

    def _set_deadline(self, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds debe ser mayor que cero.")
        self.deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)

    def _move_to(self, state: SagaState, reason: str) -> None:
        self.state = state
        self._transition(state, reason)

    def _transition(self, state: SagaState, reason: str) -> None:
        self.history.append(
            SagaTransition(
                from_state=None if len(self.history) == 0 else self.history[-1].to_state,
                to_state=state,
                reason=reason,
                at=datetime.now(timezone.utc).isoformat(),
            )
        )

from datetime import datetime, timedelta, timezone

import pytest

from envelope import EventEnvelope
from saga import SagaInstance, SagaState


def event(event_type, correlation_id="saga-1", payload=None):
    return EventEnvelope(type=event_type, correlationId=correlation_id, payload=payload or {})


def test_saga_records_happy_path_history_and_transitions():
    scheduled = event("m3.publish.scheduled", payload={"contentId": "video-1"})
    saga = SagaInstance.start(scheduled, timeout_seconds=60)
    emitted = []

    saga.handle_event(
        event("m3.publish.completed", payload={"youtubeVideoId": "yt-1"}),
        60,
        lambda key, payload, causation: emitted.append((key, payload, causation.type)),
    )
    saga.handle_event(
        event("m4.team.notified"),
        60,
        lambda key, payload, causation: emitted.append((key, payload, causation.type)),
    )

    assert saga.state == SagaState.COMPLETED
    assert [item.to_state for item in saga.history] == [
        SagaState.SCHEDULED,
        SagaState.PUBLISHING,
        SagaState.WAITING_NOTIFICATION,
        SagaState.COMPLETED,
    ]
    assert emitted[0][0] == "m2.notify.requested"
    assert saga.deadline is None


def test_saga_compensates_publication_failure():
    saga = SagaInstance.start(event("publish.scheduled"), 60, max_step_retries=0)
    emitted = []
    saga.handle_event(
        event("m3.publish.failed", payload={"reason": "quota"}),
        60,
        lambda key, payload, causation: emitted.append((key, payload)),
    )

    assert saga.state == SagaState.FAILED
    assert emitted[0][0] == "m2.publish.rollback"
    assert emitted[0][1]["reason"] == "publication failed"


def test_saga_retries_publication_before_compensation():
    saga = SagaInstance.start(event("publish.scheduled"), 60, max_step_retries=1)
    emitted = []
    saga.handle_event(
        event("publish.failed", payload={"reason": "transient"}),
        60,
        lambda key, payload, causation: emitted.append((key, payload)),
        max_step_retries=1,
    )
    assert saga.state == SagaState.PUBLISHING
    assert emitted[-1][0] == "m2.publish.execute"
    assert emitted[-1][1]["attempt"] == 2

    saga.handle_event(
        event("publish.failed", payload={"reason": "permanent"}),
        60,
        lambda key, payload, causation: emitted.append((key, payload)),
        max_step_retries=1,
    )
    assert saga.state == SagaState.FAILED
    assert emitted[-1][0] == "m2.publish.rollback"


def test_saga_timeout_compensates_waiting_notification():
    saga = SagaInstance.start(event("publish.scheduled"), 60)
    saga.handle_event(
        event("publish.completed"),
        60,
        lambda *_: None,
    )
    emitted = []
    saga.deadline = datetime.now(timezone.utc) - timedelta(seconds=1)
    expired = saga.handle_timeout(
        datetime.now(timezone.utc),
        lambda key, payload, causation: emitted.append((key, payload)),
    )

    assert expired is True
    assert saga.state == SagaState.WAITING_NOTIFICATION
    saga.deadline = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert saga.handle_timeout(
        datetime.now(timezone.utc),
        lambda key, payload, causation: emitted.append((key, payload)),
    ) is True
    saga.deadline = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert saga.handle_timeout(
        datetime.now(timezone.utc),
        lambda key, payload, causation: emitted.append((key, payload)),
    ) is True
    assert saga.state == SagaState.FAILED
    assert emitted[-1][0] == "m2.publish.rollback"


def test_saga_compensates_notification_failure():
    saga = SagaInstance.start(event("publish.scheduled"), 60, max_step_retries=0)
    saga.handle_event(event("publish.completed"), 60, lambda *_: None)
    emitted = []

    saga.handle_event(
        event("notify.failed", payload={"reason": "provider unavailable"}),
        60,
        lambda key, payload, causation: emitted.append((key, payload)),
    )

    assert saga.state == SagaState.FAILED
    assert emitted[-1][0] == "m2.publish.rollback"
    assert emitted[-1][1]["reason"] == "notification failed"


def test_saga_rejects_event_from_other_correlation():
    saga = SagaInstance.start(event("publish.scheduled", "saga-1"), 60)
    with pytest.raises(ValueError, match="correlationId"):
        saga.handle_event(event("publish.completed", "saga-2"), 60, lambda *_: None)

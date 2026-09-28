"""Consulta de trazabilidad de eventos por correlationId."""

from typing import Any

from event_store import get_events_by_correlation_id


def get_traceability_by_correlation_id(correlation_id: str) -> list[dict[str, Any]]:
    """Devuelve los eventos asociados a un correlationId no vacío."""
    if not isinstance(correlation_id, str) or not correlation_id.strip():
        raise ValueError("'correlation_id' es obligatorio y no puede estar vacío.")

    return get_events_by_correlation_id(correlation_id.strip())
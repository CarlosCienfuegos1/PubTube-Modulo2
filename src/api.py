"""API HTTP para consultar la trazabilidad de una saga."""

from fastapi import FastAPI, HTTPException, Query

from traceability import get_traceability, get_traceability_by_correlation_id

app = FastAPI(title="PubTube Traceability API")


EVENT_RESPONSE = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "id": {"type": "string"},
        "type": {"type": "string"},
        "version": {"type": "integer"},
        "timestamp": {"type": "string", "format": "date-time"},
        "correlation_id": {"type": "string"},
        "causation_id": {"type": ["string", "null"]},
        "source": {"type": "string"},
        "routing_key": {"type": "string"},
        "payload": {"type": "object"},
        "received_at": {"type": "string", "format": "date-time"},
    },
}
EVENT_LIST_RESPONSE = {"type": "array", "items": EVENT_RESPONSE}

@app.get("/health")
@app.get("/api/events/health")
@app.get("/events/health")
def health_check():
    return {"status": "ok"}



@app.get(
    "/events",
    responses={
        200: {"description": "Eventos encontrados.", "content": {"application/json": {"schema": EVENT_LIST_RESPONSE}}},
        500: {"description": "Error interno al consultar el Event Store."},
    },
)
def query_events(
    correlation_id: str | None = Query(
        default=None,
        alias="correlationId",
        description="Filtra por correlationId.",
    )
) -> list[dict[str, object]]:
    """Consulta todos los eventos, opcionalmente filtrados por correlationId."""
    return get_traceability(correlation_id)


@app.get(
    "/events/{correlation_id}",
    responses={
        200: {
            "description": "Eventos encontrados.",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "required": ["correlationId", "events"],
                        "properties": {
                            "correlationId": {"type": "string"},
                            "events": EVENT_LIST_RESPONSE,
                        },
                    }
                }
            },
        },
        400: {"description": "El correlationId es obligatorio."},
        404: {"description": "No hay eventos para el correlationId indicado."},
        500: {"description": "Error interno al consultar el Event Store."},
    },
)
def query_traceability(correlation_id: str) -> dict[str, object]:
    """Consulta los eventos de un flujo por correlationId."""
    try:
        events = get_traceability_by_correlation_id(correlation_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not events:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron eventos para correlationId '{correlation_id.strip()}'.",
        )

    return {"correlationId": correlation_id.strip(), "events": events}
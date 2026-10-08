"""API HTTP para consultar la trazabilidad de una saga."""

from fastapi import FastAPI, HTTPException

from traceability import get_traceability_by_correlation_id

app = FastAPI(title="PubTube Traceability API")

@app.get("/health")
@app.get("/api/events/health")
@app.get("/events/health")
def health_check():
    return {"status": "ok"}



@app.get(
    "/events/{correlation_id}",
    responses={
        400: {"description": "El correlationId es obligatorio."},
        404: {"description": "No hay eventos para el correlationId indicado."},
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
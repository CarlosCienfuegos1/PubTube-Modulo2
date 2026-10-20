"""API HTTP para consultar la trazabilidad de una saga."""

from fastapi import FastAPI, HTTPException

from traceability import get_traceability_by_correlation_id

app = FastAPI(
    title="PubTube Traceability API",
    description="API HTTP del Módulo 2 para verificación de salud y consulta de trazabilidad histórica de eventos por correlationId.",
    version="1.0.0",
)

@app.get("/health", tags=["Health"])
@app.get("/api/events/health", tags=["Health"])
@app.get("/events/health", tags=["Health"])
def health_check():
    return {"status": "ok"}



@app.get(
    "/api/events/{correlation_id}",
    tags=["Traceability"],
    summary="Consultar trazabilidad por correlationId (vía Gateway)",
    responses={
        400: {"description": "El correlationId es obligatorio."},
        404: {"description": "No hay eventos para el correlationId indicado."},
    },
)
@app.get(
    "/events/{correlation_id}",
    tags=["Traceability"],
    summary="Consultar trazabilidad por correlationId",
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
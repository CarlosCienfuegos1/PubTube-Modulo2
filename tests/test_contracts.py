"""Pruebas de contrato del envelope y de los endpoints HTTP del Módulo 2."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from jsonschema import ValidationError

import api
from contract_validation import validate_event_contract


SCHEMA = json.loads(
    (Path(__file__).resolve().parent.parent / "schemas" / "event-envelope.json").read_text(
        encoding="utf-8"
    )
)
OPENAPI = json.loads(
    (Path(__file__).resolve().parent.parent / "docs" / "openapi.json").read_text(
        encoding="utf-8"
    )
)


def valid_event() -> dict:
    return {
        "id": "event-001",
        "type": "m1.video.uploaded",
        "version": 1,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "correlationId": "saga-001",
        "causationId": None,
        "source": "module-1",
        "payload": {"contentId": "video-001"},
    }


def test_evento_valido_cumple_el_contrato():
    evento = valid_event()

    validate_event_contract(evento)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda event: event.pop("correlationId"),
        lambda event: event.update(timestamp="not-a-timestamp"),
        lambda event: event.update(version=2),
    ],
)
def test_evento_invalido_rechaza_el_contrato(mutator):
    evento = valid_event()
    mutator(evento)

    with pytest.raises(ValidationError):
        validate_event_contract(evento)


def test_openapi_documenta_endpoints_parametros_y_estados():
    events_get = OPENAPI["paths"]["/events"]["get"]
    traceability_get = OPENAPI["paths"]["/events/{correlationId}"]["get"]

    assert events_get["parameters"][0]["name"] == "correlationId"
    assert {"200", "500"} <= set(events_get["responses"])
    assert {"200", "404", "500"} <= set(traceability_get["responses"])
    assert "EventRecord" in json.dumps(OPENAPI)


def test_get_events_responde_contrato(monkeypatch):
    events = [valid_event()]
    monkeypatch.setattr(api, "get_traceability", lambda _: events)
    client = TestClient(api.app)

    response = client.get("/events", params={"correlationId": "saga-001"})

    assert response.status_code == 200
    assert response.json() == events


def test_get_events_by_correlation_id_responde_contrato(monkeypatch):
    events = [valid_event()]
    monkeypatch.setattr(api, "get_traceability_by_correlation_id", lambda _: events)
    client = TestClient(api.app)

    response = client.get("/events/saga-001")

    assert response.status_code == 200
    assert response.json() == {"correlationId": "saga-001", "events": events}


def test_get_events_by_correlation_id_devuelve_404(monkeypatch):
    monkeypatch.setattr(api, "get_traceability_by_correlation_id", lambda _: [])
    client = TestClient(api.app)

    response = client.get("/events/saga-404")

    assert response.status_code == 404

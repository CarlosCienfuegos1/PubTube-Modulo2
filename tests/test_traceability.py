"""Pruebas unitarias de la consulta de trazabilidad."""

import pytest

import api
import traceability


def test_consulta_trazabilidad_delega_por_correlation_id(monkeypatch):
    expected = [{"id": "event-1", "type": "m1.video.uploaded"}]
    received = []

    def fake_query(correlation_id):
        received.append(correlation_id)
        return expected

    monkeypatch.setattr(traceability, "get_events_by_correlation_id", fake_query)

    result = traceability.get_traceability_by_correlation_id("  saga-001  ")

    assert result == expected
    assert received == ["saga-001"]


@pytest.mark.parametrize("correlation_id", ["", "   ", None])
def test_consulta_trazabilidad_rechaza_correlation_id_vacio(correlation_id):
    with pytest.raises(ValueError):
        traceability.get_traceability_by_correlation_id(correlation_id)


def test_endpoint_devuelve_trazabilidad(monkeypatch):
    events = [{"id": "event-1", "type": "m1.video.uploaded"}]
    monkeypatch.setattr(api, "get_traceability_by_correlation_id", lambda _: events)

    response = api.query_traceability("saga-001")

    assert response == {"correlationId": "saga-001", "events": events}


def test_endpoint_indica_ausencia_de_trazabilidad(monkeypatch):
    monkeypatch.setattr(api, "get_traceability_by_correlation_id", lambda _: [])

    with pytest.raises(api.HTTPException) as error:
        api.query_traceability("saga-001")

    assert error.value.status_code == 404
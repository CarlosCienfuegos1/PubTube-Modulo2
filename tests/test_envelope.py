"""
Tests unitarios del EventEnvelope.
No requieren que RabbitMQ esté corriendo: validan solo el modelo de datos.

Correr con:
    pytest tests/test_envelope.py -v
"""
import json
import pytest
from pydantic import ValidationError

from envelope import EventEnvelope


def test_envelope_genera_id_automaticamente():
    """El campo 'id' debe autogenerarse si no se provee."""
    evento = EventEnvelope(type="video.uploaded", correlationId="abc-123")
    assert evento.id  # no vacío
    assert isinstance(evento.id, str)


def test_envelope_genera_timestamp_iso8601():
    """El timestamp debe autogenerarse en formato ISO 8601."""
    evento = EventEnvelope(type="video.uploaded", correlationId="abc-123")
    # Un timestamp ISO 8601 válido siempre trae al menos un guión y una T
    assert "T" in evento.timestamp
    assert evento.timestamp.count("-") >= 2


def test_envelope_version_default_es_1():
    evento = EventEnvelope(type="video.uploaded", correlationId="abc-123")
    assert evento.version == 1


def test_envelope_causationId_es_opcional():
    """causationId debe poder omitirse (None) sin error."""
    evento = EventEnvelope(type="video.uploaded", correlationId="abc-123")
    assert evento.causationId is None


def test_envelope_requiere_type():
    """'type' es obligatorio: sin él, la validación debe fallar."""
    with pytest.raises(ValidationError):
        EventEnvelope(correlationId="abc-123")


def test_envelope_requiere_correlationId():
    """'correlationId' es obligatorio: sin él, la validación debe fallar."""
    with pytest.raises(ValidationError):
        EventEnvelope(type="video.uploaded")


def test_envelope_serializa_a_json_valido():
    """El envelope debe serializar a JSON parseable con todos los campos del contrato."""
    evento = EventEnvelope(
        type="video.uploaded",
        correlationId="abc-123",
        payload={"contentId": "video_1"}
    )
    data = json.loads(evento.model_dump_json())

    # Campos exigidos por el contrato acordado (sección 6.3 del documento del proyecto)
    for campo in ["id", "type", "version", "timestamp", "correlationId", "causationId", "payload"]:
        assert campo in data


def test_envelope_payload_default_es_diccionario_vacio():
    evento = EventEnvelope(type="video.uploaded", correlationId="abc-123")
    assert evento.payload == {}


def test_envelope_acepta_payload_anidado():
    payload = {"contentId": "video_1", "meta": {"size": 123, "format": "mp4"}}
    evento = EventEnvelope(type="video.uploaded", correlationId="abc-123", payload=payload)
    assert evento.payload == payload
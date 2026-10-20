"""Validación del contrato JSON Schema compartido por productores y consumidores."""

import json
from pathlib import Path
from typing import Any, Mapping
from datetime import datetime

from jsonschema import Draft202012Validator, FormatChecker, ValidationError


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "event-envelope.json"
_SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
_VALIDATOR = Draft202012Validator(_SCHEMA, format_checker=FormatChecker())


def validate_event_contract(event: Mapping[str, Any]) -> None:
    """Lanza jsonschema.ValidationError si el evento no cumple el contrato v1."""
    _VALIDATOR.validate(dict(event))
    try:
        datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError("'timestamp' debe tener formato ISO 8601.") from exc

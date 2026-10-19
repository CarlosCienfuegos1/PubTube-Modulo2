import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field

class EventEnvelope(BaseModel):
    # Identificador único del evento
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Tipo de evento, ej: 'video.uploaded'
    type: str
    
    # Versión del esquema del evento
    version: int = 1
    
    # Fecha y hora exacta de creación en formato ISO 8601 UTC
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    # ID de correlación para trazar todo un flujo o Saga
    correlationId: str
    
    # ID del evento que causó la emisión de este evento
    causationId: Optional[str] = None
    
    # Origen del evento
    source: str = "module-2"
    
    # El cuerpo del evento
    payload: Dict[str, Any] = Field(default_factory=dict)

    def serialize(self) -> str:
        """Serializa el evento a un string JSON."""
        return self.model_dump_json()

    @classmethod
    def deserialize(cls, json_str: str) -> 'EventEnvelope':
        """Deserializa y valida un string JSON convirtiéndolo en un EventEnvelope.
        Lanza ValidationError si el JSON no cumple con el esquema."""
        return cls.model_validate_json(json_str)

    @classmethod
    def create_child_from(cls, parent_event: 'EventEnvelope', event_type: str, payload: Dict[str, Any] = None) -> 'EventEnvelope':
        """
        Crea un nuevo evento como consecuencia de otro (propagación).
        - Conserva el mismo correlationId para trazar el flujo completo.
        - Asigna el id del evento padre como causationId de este nuevo evento.
        """
        return cls(
            type=event_type,
            correlationId=parent_event.correlationId,
            causationId=parent_event.id,
            payload=payload or {}
        )

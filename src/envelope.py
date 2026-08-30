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

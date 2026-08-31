"""
Agrega src/ al path de Python para que los tests puedan hacer
'from envelope import EventEnvelope' sin necesidad de convertir
el proyecto en un paquete instalable.

pytest carga este archivo automáticamente antes de correr los tests
en este directorio (y subdirectorios).
"""
import sys
from pathlib import Path

SRC_PATH = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_PATH))
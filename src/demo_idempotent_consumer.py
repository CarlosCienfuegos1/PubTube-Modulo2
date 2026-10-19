"""
demo_idempotent_consumer.py — Consumidor de prueba con idempotencia automática (US-B4b).

Demuestra el patrón de uso recomendado: el handler de negocio escribe SOLO
su lógica (recibiendo únicamente el EventEnvelope). El SDK de Módulo 2 se encarga
automáticamente de:
  - Consultar la tabla `processed_events` en PostgreSQL.
  - Filtrar eventos duplicados antes de llamar al handler.
  - Enviar ACK/NACK al broker según el resultado.
  - Inicializar la tabla de idempotencia al arrancar.

Flujo de demostración:
    1. Levanta RabbitMQ y PostgreSQL:
           docker compose up -d
    2. Ejecuta este consumidor en una terminal:
           python src/demo_idempotent_consumer.py
    3. Publica el MISMO evento dos veces desde otra terminal:
           python src/demo_publisher.py   # primera vez
           python src/demo_publisher.py   # segunda vez con el mismo correlationId

Resultado esperado:
    - Primera entrega: verás ">>> [NEGOCIO] Procesando video..." en consola.
    - Segunda entrega: el SDK detecta el duplicado y NO invoca el handler;
      solo verás el log "[idempotency] Evento duplicado detectado".
"""
import logging
import sys

# Permite correr directamente como: python src/demo_idempotent_consumer.py
sys.path.insert(0, "src")

from event_bus import subscribe_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)

# Contador visible en consola: si la idempotencia funciona, este número
# NO debe incrementarse al reenviar el mismo evento.
_veces_ejecutado: int = 0

QUEUE = "q.m4.experience"


def procesar_video(envelope) -> None:
    """
    Handler de negocio limpio — firma mínima: solo el EventEnvelope.

    El SDK (subscribe_events con idempotent=True) garantiza que esta función
    se ejecute EXACTAMENTE UNA VEZ por event_id, aunque RabbitMQ reentregue
    el mensaje. No necesitas importar nada de idempotency ni gestionar el ACK.
    """
    global _veces_ejecutado
    _veces_ejecutado += 1

    print(f"\n{'='*60}")
    print(f">>> [NEGOCIO] Ejecutando handler #{_veces_ejecutado}")
    print(f"    type:          {envelope.type}")
    print(f"    event_id:      {envelope.id}")
    print(f"    correlationId: {envelope.correlationId}")
    print(f"    payload:       {envelope.payload}")
    print(f"{'='*60}\n")


def main() -> None:
    print(
        f"[demo] Escuchando en '{QUEUE}' con idempotencia automática.\n"
        "Envía el mismo evento más de una vez para observar la idempotencia.\n"
        "Presiona Ctrl+C para salir.\n"
    )

    # El SDK se encarga de todo: inicializa la BD, filtra duplicados y gestiona ACK.
    subscribe_events(
        queue=QUEUE,
        routing_keys=["m1.video.uploaded", "m3.publish.completed"],
        on_event=procesar_video,
        idempotent=True,  # True es el valor por defecto; se puede omitir
    )


if __name__ == "__main__":
    main()

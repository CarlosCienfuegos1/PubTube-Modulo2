"""
demo_subscriber.py — Ejemplo de suscripción usando el SDK event_bus (US-B1b).

Muestra cómo cualquier módulo (M1, M3, M4...) se suscribe a eventos del exchange
'pubtube.events' usando la librería interna con reconexión automática.

Uso:
    python src/demo_subscriber.py
"""
import logging
from event_bus import subscribe_events
from envelope import EventEnvelope

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def on_event(envelope: EventEnvelope, channel) -> None:
    """
    Callback que se llama por cada evento recibido en la cola.

    Args:
        envelope: El evento ya deserializado y validado como EventEnvelope.
        channel:  El canal pika activo. Úsalo para hacer ACK manual.
    """
    print(f"\n[x] Evento recibido: type='{envelope.type}'")
    print(f"    id:            {envelope.id}")
    print(f"    correlationId: {envelope.correlationId}")
    print(f"    payload:       {envelope.payload}")
    print("    - Procesado exitosamente -")

    # ACK manual: le confirma al broker que procesamos el mensaje con éxito.
    # Si el proceso muere antes de llegar aquí, RabbitMQ reencola el mensaje.
    channel.basic_ack(delivery_tag=channel.last_delivery_tag if hasattr(channel, 'last_delivery_tag') else 0)


def _on_event_with_method(envelope: EventEnvelope, channel) -> None:
    """Wrapper que imprime el evento. El channel aquí es el canal pika."""
    print(f"\n[x] Evento recibido: type='{envelope.type}'")
    print(f"    id:            {envelope.id}")
    print(f"    correlationId: {envelope.correlationId}")
    print(f"    payload:       {envelope.payload}")
    print("    - Listo para ser procesado -")


def main() -> None:
    print("[*] Esperando eventos en 'q.m4.experience'. Presiona Ctrl+C para salir.")
    subscribe_events(
        queue="q.m4.experience",
        routing_keys=["m1.video.uploaded", "m3.publish.completed"],
        on_event=_on_event_with_method,
        auto_ack=True,  # auto_ack=True para el demo; en producción usar False + ACK manual
    )


if __name__ == "__main__":
    main()

"""
demo_publisher.py — Ejemplo de publicación usando el SDK event_bus (US-B1b).

Muestra cómo cualquier módulo (M1, M3, M4...) publica un evento al exchange
'pubtube.events' usando la librería interna, sin escribir código de pika.

Uso:
    python src/demo_publisher.py
"""
import uuid
import logging
from event_bus import publish_event

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main() -> None:
    envelope = publish_event(
        routing_key="m1.video.uploaded",
        correlation_id=str(uuid.uuid4()),
        payload={
            "contentId": "video_12345",
            "checksum": "a8f5f167f44f...",
            "storageUrl": "https://s3.demo.com/videos/video_12345.mp4",
        },
    )
    print(f"[OK] Evento publicado: type='{envelope.type}', id='{envelope.id}'")


if __name__ == "__main__":
    main()

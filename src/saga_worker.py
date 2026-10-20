"""Worker RabbitMQ de la Saga. Ejecutar con `python src/saga_worker.py`."""

from __future__ import annotations

import logging
import threading
import time

from event_bus import subscribe_events
from saga_orchestrator import SAGA_EVENTS, SAGA_QUEUE, SagaOrchestrator
from saga_store import SagaRepository

logging.basicConfig(level=logging.INFO)


def main() -> None:
    repository = SagaRepository()
    repository.init_db()
    orchestrator = SagaOrchestrator(repository)

    def timeout_loop() -> None:
        while True:
            try:
                orchestrator.expire_due()
            except Exception:
                logging.getLogger(__name__).exception("Error revisando timeouts de Saga")
            time.sleep(1)

    threading.Thread(target=timeout_loop, daemon=True).start()
    subscribe_events(
        queue=SAGA_QUEUE,
        routing_keys=SAGA_EVENTS,
        on_event=orchestrator.handle,
        idempotent=True,
    )


if __name__ == "__main__":
    main()

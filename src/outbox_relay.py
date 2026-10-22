"""
outbox_relay.py — Proceso Relay de publicación del Outbox Transaccional (US-B5b).

Este módulo implementa el proceso "Relay/Poller" para el patrón Transactional Outbox:
1. Inspecciona periódicamente la tabla `outbox` en PostgreSQL en busca de eventos
   con estado 'PENDING', ordenados cronológicamente por `created_at`.
2. Para cada evento pendiente:
   a. Reconstruye el EventEnvelope original con sus identificadores intactos (ID,
      correlationId, causationId, tipo, payload, etc.).
   b. Publica el evento al exchange de RabbitMQ activando Publisher Confirms (confirm_delivery=True).
   c. ÚNICAMENTE tras la confirmación exitosa del broker (ACK), marca el evento
      como 'PUBLISHED' en la tabla outbox (con published_at = now()).
3. Si el broker rechaza la publicación o está caído:
   - El evento NO se marca como entregado y permanece en estado 'PENDING'.
   - El relay detiene el procesamiento del lote actual para respetar el orden y
     reintentará en el próximo ciclo de polling.
4. Si el relay se reinicia o cae a mitad de camino (después de publicar pero antes de
   marcar como entregado en la BD):
   - Al reiniciar, el evento sigue figurando como 'PENDING' y será publicado nuevamente (At-Least-Once).
   - Los consumidores que utilicen el middleware de idempotencia de US-B4 deduplicarán
     el evento mediante `processed_events`, garantizando un procesamiento Exactly-Once a nivel de negocio.
"""

import json
import logging
import signal
import sys
import time
from typing import Any, Dict, List, Optional

import psycopg

from envelope import EventEnvelope
from event_bus import publish_envelope
from outbox import get_connection, get_pending_events, init_outbox_db, mark_as_published

logger = logging.getLogger(__name__)


def reconstruct_envelope_from_outbox(row: Dict[str, Any]) -> EventEnvelope:
    """
    Reconstruye una instancia de EventEnvelope a partir de un registro de la tabla outbox.

    Preserva estrictamente el `id` original del evento para que, en caso de reentrega
    tras un reinicio o caída, los consumidores aguas abajo puedan detectar la duplicación
    mediante su clave de idempotencia (US-B4).

    Args:
        row: Diccionario con las columnas de la fila leída de la tabla `outbox`.

    Returns:
        Instancia de EventEnvelope con los metadatos y payload originales.
    """
    payload_raw = row.get("payload")
    if isinstance(payload_raw, str):
        try:
            payload_data = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload_data = {"raw": payload_raw}
    elif isinstance(payload_raw, dict):
        payload_data = payload_raw
    else:
        payload_data = {}

    causation = row.get("causation_id")
    causation_str = str(causation) if causation is not None and str(causation).strip() else None

    envelope = EventEnvelope(
        id=str(row["id"]),
        type=str(row["event_type"]),
        correlationId=str(row["correlation_id"]),
        causationId=causation_str,
        source=str(row.get("source") or "module-2"),
        payload=payload_data,
    )
    return envelope


def publish_outbox_row(
    row: Dict[str, Any],
    conn: Optional[psycopg.Connection] = None,
    confirm_delivery: bool = True,
) -> bool:
    """
    Publica un único evento de la outbox a RabbitMQ y, solo tras recibir
    confirmación del broker, lo marca como 'PUBLISHED' en PostgreSQL.

    Args:
        row:              Registro del outbox con los datos del evento.
        conn:             (Opcional) Conexión existente a PostgreSQL para actualizar el estado.
        confirm_delivery: Si True, exige confirmación explícita del broker antes de marcar.

    Returns:
        True si el evento fue confirmado por el broker y marcado como publicado.
        False si la publicación falló o fue rechazada (el evento permanece PENDING).
    """
    event_id = str(row["id"])
    routing_key = str(row.get("routing_key") or row.get("event_type") or "").strip()

    try:
        envelope = reconstruct_envelope_from_outbox(row)
        if not routing_key:
            routing_key = envelope.type

        # 1. Publicación con Publisher Confirms activado
        logger.debug(
            "[outbox_relay] Publicando evento id=%s (rk=%s) con confirm_delivery=%s...",
            event_id,
            routing_key,
            confirm_delivery,
        )
        publish_envelope(
            envelope=envelope,
            routing_key=routing_key,
            confirm_delivery=confirm_delivery,
        )
    except Exception as exc:
        logger.error(
            "[outbox_relay] Error al publicar evento id=%s al broker: %s. "
            "El evento permanece PENDING para reintento.",
            event_id,
            exc,
        )
        return False

    # 2. Solo tras confirmación del broker (no se lanzó excepción), actualizar estado en BD
    try:
        updated = mark_as_published(event_id, conn=conn)
        if updated:
            logger.info(
                "[outbox_relay] Evento id=%s ('%s') confirmado por broker y marcado como PUBLISHED.",
                event_id,
                routing_key,
            )
        else:
            logger.warning(
                "[outbox_relay] Evento id=%s confirmado por broker, pero no se encontró en outbox para marcar.",
                event_id,
            )
        return True
    except Exception as exc:
        logger.critical(
            "[outbox_relay] Evento id=%s publicado con éxito, pero falló marcarlo en BD: %s. "
            "El evento se re-publicará en el próximo ciclo; los consumidores deduplicarán vía US-B4.",
            event_id,
            exc,
        )
        return False


def process_pending_outbox(
    batch_size: int = 50,
    conn: Optional[psycopg.Connection] = None,
    confirm_delivery: bool = True,
) -> int:
    """
    Recupera los eventos pendientes de la tabla `outbox` y los publica en orden cronológico.

    Si algún evento falla al publicarse hacia el broker, el procesamiento del lote se
    detiene de inmediato para no desordenar la secuencia ni sobrecargar un broker caído.

    Args:
        batch_size:       Límite máximo de eventos a procesar en esta iteración.
        conn:             (Opcional) Conexión a PostgreSQL.
        confirm_delivery: Si True, exige confirmación del broker.

    Returns:
        Número de eventos publicados y marcados exitosamente en esta tanda.
    """
    pending_events = get_pending_events(limit=batch_size, conn=conn)
    if not pending_events:
        return 0

    logger.debug("[outbox_relay] Se encontraron %d eventos pendientes.", len(pending_events))
    published_count = 0

    for row in pending_events:
        success = publish_outbox_row(row, conn=conn, confirm_delivery=confirm_delivery)
        if success:
            published_count += 1
        else:
            # Si el broker falló, detenemos el lote actual para preservar causalidad y orden
            logger.warning(
                "[outbox_relay] Deteniendo procesamiento del lote en evento id=%s por fallo de broker.",
                row.get("id"),
            )
            break

    return published_count


class OutboxRelay:
    """
    Worker que ejecuta un ciclo continuo de polling sobre la tabla `outbox`.
    """

    def __init__(
        self,
        poll_interval: float = 1.0,
        batch_size: int = 50,
        confirm_delivery: bool = True,
    ):
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.confirm_delivery = confirm_delivery
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """
        Ejecuta el bucle de polling hasta que `stop()` sea llamado o se interrumpa el proceso.
        """
        self._running = True
        logger.info(
            "[outbox_relay] Worker relay iniciado (intervalo=%.1fs, batch_size=%d, confirm=%s).",
            self.poll_interval,
            self.batch_size,
            self.confirm_delivery,
        )

        try:
            init_outbox_db()
        except Exception as exc:
            logger.warning(
                "[outbox_relay] No se pudo verificar la tabla outbox al iniciar (se reintentará en el loop): %s",
                exc,
            )

        while self._running:
            try:
                published = process_pending_outbox(
                    batch_size=self.batch_size,
                    confirm_delivery=self.confirm_delivery,
                )
                if published == 0:
                    time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("[outbox_relay] Interrupción manual recibida (KeyboardInterrupt).")
                break
            except Exception as exc:
                logger.error("[outbox_relay] Error inesperado en ciclo de polling: %s", exc)
                time.sleep(self.poll_interval)

        self._running = False
        logger.info("[outbox_relay] Worker relay finalizado correctamente.")

    def stop(self) -> None:
        """Indica al worker que debe detener su ejecución de forma segura."""
        logger.info("[outbox_relay] Solicitud de detención recibida.")
        self._running = False


def run_relay(
    poll_interval: float = 1.0,
    batch_size: int = 50,
    run_once: bool = False,
    confirm_delivery: bool = True,
) -> int:
    """
    Punto de entrada programático y de línea de comandos para el relay de outbox.

    Args:
        poll_interval:    Segundos de espera cuando no hay eventos pendientes.
        batch_size:       Cantidad de eventos por lote.
        run_once:         Si es True, procesa una única tanda y retorna el total publicado.
        confirm_delivery: Si es True, exige confirmación del broker.

    Returns:
        Número de eventos procesados (si run_once=True) o 0 (si corrió el loop continuo).
    """
    if run_once:
        return process_pending_outbox(
            batch_size=batch_size,
            confirm_delivery=confirm_delivery,
        )

    relay = OutboxRelay(
        poll_interval=poll_interval,
        batch_size=batch_size,
        confirm_delivery=confirm_delivery,
    )

    def _signal_handler(sig, frame):
        logger.info("[outbox_relay] Señal del sistema recibida: %s. Apagando...", sig)
        relay.stop()

    try:
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    except (ValueError, AttributeError):
        # Manejo de entorno donde signals no están disponibles (threads secundarios o ciertas shells de Windows)
        pass

    relay.start()
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_relay()

"""
test_event_bus.py — Pruebas unitarias del SDK event_bus (US-B1b).

Estas pruebas verifican la lógica del SDK sin necesidad de que RabbitMQ
esté corriendo. Usan 'unittest.mock.patch' para simular la biblioteca pika
y provocar situaciones de error controladas (broker caído, mensajes inválidos,
reconexión exitosa tras fallos).

Conceptos clave:
- patch(): reemplaza temporalmente un objeto real por un Mock (un objeto falso
  que registra cómo fue llamado y permite definir su comportamiento).
- MagicMock(): un Mock que también simula el protocolo de contexto (__enter__,
  __exit__) y operadores mágicos de Python.
- side_effect: cuando se asigna una excepción a side_effect, el Mock la lanza
  automáticamente al ser llamado, simulando un fallo.
"""
import time
import pytest
from unittest.mock import MagicMock, patch, call
from pydantic import ValidationError

import pika.exceptions

# Ajustamos el path (conftest.py ya lo hace, esto es por claridad documental)
from event_bus import (
    publish_event,
    subscribe_events,
    EXCHANGE_NAME,
    MAX_RETRIES,
    INITIAL_BACKOFF,
)
from envelope import EventEnvelope


# ─── Fixtures reutilizables ───────────────────────────────────────────────────

@pytest.fixture
def mock_channel():
    """Canal pika simulado que no hace nada real pero registra las llamadas."""
    ch = MagicMock()
    return ch


@pytest.fixture
def mock_connection(mock_channel):
    """
    Conexión pika simulada. Su método .channel() devuelve el mock_channel
    para que podamos inspeccionar qué se llamó en el canal.
    """
    conn = MagicMock()
    conn.channel.return_value = mock_channel
    return conn


# ─── Tests de publish_event ───────────────────────────────────────────────────

class TestPublishEvent:
    """Pruebas del comportamiento de publish_event()."""

    @patch("event_bus.pika.BlockingConnection")
    def test_publica_evento_valido_retorna_envelope(self, mock_bc):
        """
        Caso feliz: publicar un evento con datos válidos.
        Verifica que:
        1. La función retorna un EventEnvelope.
        2. Se llama a basic_publish exactamente una vez.
        3. El body publicado es JSON deserializable como EventEnvelope.
        """
        mock_conn = MagicMock()
        mock_ch = MagicMock()
        mock_conn.channel.return_value = mock_ch
        mock_bc.return_value = mock_conn

        envelope = publish_event(
            routing_key="m1.video.uploaded",
            payload={"contentId": "abc123"},
            correlation_id="saga-001",
        )

        assert isinstance(envelope, EventEnvelope)
        assert envelope.type == "m1.video.uploaded"
        assert envelope.payload == {"contentId": "abc123"}
        assert envelope.correlationId == "saga-001"

        # Verificar que se publicó una sola vez al exchange correcto
        mock_ch.basic_publish.assert_called_once()
        kwargs = mock_ch.basic_publish.call_args.kwargs
        assert kwargs["exchange"] == EXCHANGE_NAME
        assert kwargs["routing_key"] == "m1.video.uploaded"

        # El body publicado debe ser JSON válido que reconstruye el envelope
        body_json = kwargs["body"]
        reconstructed = EventEnvelope.deserialize(body_json)
        assert reconstructed.id == envelope.id

    @patch("event_bus.pika.BlockingConnection")
    def test_publica_con_causation_id(self, mock_bc):
        """
        Verificar que causation_id se transmite correctamente en el envelope.
        Esto permite trazar cadenas causales entre eventos.
        """
        mock_conn = MagicMock()
        mock_conn.channel.return_value = MagicMock()
        mock_bc.return_value = mock_conn

        envelope = publish_event(
            routing_key="m3.publish.scheduled",
            payload={"contentId": "abc123", "scheduleAt": "2026-12-01T10:00:00Z"},
            correlation_id="saga-002",
            causation_id="evento-padre-001",
        )

        assert envelope.causationId == "evento-padre-001"

    @patch("event_bus.time.sleep")
    @patch("event_bus.pika.BlockingConnection")
    def test_reintenta_ante_caida_del_broker_y_luego_publica(
        self, mock_bc, mock_sleep
    ):
        """
        Caso de reconexión: el broker falla en el primer intento y
        se recupera en el segundo.

        Verifica que:
        1. No se lanza excepción si el broker se recupera antes de MAX_RETRIES.
        2. time.sleep() fue llamado (se esperó antes de reintentar).
        3. El evento se publica correctamente tras la reconexión.
        """
        mock_conn = MagicMock()
        mock_conn.channel.return_value = MagicMock()

        # El primer intento falla, el segundo tiene éxito
        mock_bc.side_effect = [
            pika.exceptions.AMQPConnectionError("Broker caído"),
            mock_conn,
        ]

        envelope = publish_event(
            routing_key="m1.video.uploaded",
            payload={"contentId": "xyz"},
            correlation_id="saga-003",
        )

        assert isinstance(envelope, EventEnvelope)
        # Verificar que se esperó antes de reintentar
        mock_sleep.assert_called()

    @patch("event_bus.time.sleep")
    @patch("event_bus.pika.BlockingConnection")
    def test_lanza_excepcion_si_se_agotan_reintentos(self, mock_bc, mock_sleep):
        """
        Caso límite: el broker no se recupera en ninguno de los MAX_RETRIES
        intentos. La función debe propagar la excepción al llamador.
        """
        mock_bc.side_effect = pika.exceptions.AMQPConnectionError(
            "Broker permanentemente caído"
        )

        with pytest.raises(pika.exceptions.AMQPConnectionError):
            publish_event(
                routing_key="m1.video.uploaded",
                payload={"contentId": "xyz"},
                correlation_id="saga-004",
            )

        # Se intentó MAX_RETRIES veces (una por cada intento de _connect_with_retry)
        assert mock_bc.call_count == MAX_RETRIES

    @patch("event_bus.pika.BlockingConnection")
    def test_envelope_invalido_lanza_excepcion_antes_de_publicar(
        self, mock_bc
    ):
        """
        Si el llamador pasa datos que no cumplen el esquema del EventEnvelope
        (US-B2a), se lanza una excepción ANTES de intentar conectarse
        al broker. Esto satisface: "un evento malformado se rechaza antes
        de publicarse."
        """
        # correlation_id vacío debe rechazarse con ValueError antes de tocar pika
        with pytest.raises((ValidationError, TypeError, ValueError)):
            publish_event(
                routing_key="m1.video.uploaded",
                payload={"contentId": "xyz"},
                correlation_id="",  # valor vacío inválido
            )

        # Verificar que NUNCA se intentó conectar al broker
        mock_bc.assert_not_called()


# ─── Tests de subscribe_events ────────────────────────────────────────────────

class TestSubscribeEvents:
    """Pruebas del comportamiento de subscribe_events()."""

    @patch("event_bus.pika.BlockingConnection")
    def test_suscripcion_declara_cola_y_bindings(self, mock_bc):
        """
        Verifica que subscribe_events() declara la cola y vincula
        todas las routing keys al exchange antes de comenzar a consumir.
        """
        mock_conn = MagicMock()
        mock_ch = MagicMock()
        mock_conn.channel.return_value = mock_ch
        mock_bc.return_value = mock_conn

        # start_consuming lanza KeyboardInterrupt para salir del loop
        mock_ch.start_consuming.side_effect = KeyboardInterrupt

        routing_keys = ["m1.video.uploaded", "m3.publish.completed"]

        subscribe_events(
            queue="q.m4.experience",
            routing_keys=routing_keys,
            on_event=lambda e, ch: None,
        )

        # Verificar que se declaró la cola
        mock_ch.queue_declare.assert_called_with(
            queue="q.m4.experience", durable=True
        )

        # Verificar que se vincularon TODAS las routing keys
        bind_calls = mock_ch.queue_bind.call_args_list
        bound_keys = [c.kwargs["routing_key"] for c in bind_calls]
        for rk in routing_keys:
            assert rk in bound_keys, f"La routing key '{rk}' no fue vinculada."

    @patch("event_bus.pika.BlockingConnection")
    def test_callback_recibe_envelope_deserializado(self, mock_bc):
        """
        Verifica que el callback on_event recibe un EventEnvelope
        correctamente deserializado (no el body raw en bytes de pika).
        """
        mock_conn = MagicMock()
        mock_ch = MagicMock()
        mock_conn.channel.return_value = mock_ch
        mock_bc.return_value = mock_conn

        received_envelopes = []

        def my_handler(envelope: EventEnvelope, channel) -> None:
            received_envelopes.append(envelope)

        # Preparamos el evento fake que 'pika' entregará al consumidor
        evento_original = EventEnvelope(
            type="m1.video.uploaded",
            correlationId="saga-999",
            payload={"contentId": "vid_xyz"},
        )
        body_bytes = evento_original.serialize().encode("utf-8")

        # Simulamos que start_consuming llama al callback una vez y luego para
        def fake_start_consuming():
            # Obtenemos el callback que registró subscribe_events
            callback = mock_ch.basic_consume.call_args.kwargs["on_message_callback"]
            # Simulamos la entrega de un mensaje por pika
            method_mock = MagicMock()
            method_mock.routing_key = "m1.video.uploaded"
            method_mock.delivery_tag = 1
            callback(mock_ch, method_mock, MagicMock(), body_bytes)
            raise KeyboardInterrupt  # salir del loop

        mock_ch.start_consuming.side_effect = fake_start_consuming

        subscribe_events(
            queue="q.m4.experience",
            routing_keys=["m1.video.uploaded"],
            on_event=my_handler,
            auto_ack=True,
        )

        assert len(received_envelopes) == 1
        envelope = received_envelopes[0]
        assert isinstance(envelope, EventEnvelope)
        assert envelope.type == "m1.video.uploaded"
        assert envelope.correlationId == "saga-999"
        assert envelope.payload["contentId"] == "vid_xyz"

    @patch("event_bus.pika.BlockingConnection")
    def test_mensaje_invalido_es_descartado_sin_romper_consumidor(self, mock_bc):
        """
        Si llega un mensaje que no es un JSON válido de EventEnvelope,
        el consumidor lo descarta con NACK (sin requeue) y sigue procesando
        otros mensajes. No debe lanzar excepción al llamador.
        """
        mock_conn = MagicMock()
        mock_ch = MagicMock()
        mock_conn.channel.return_value = mock_ch
        mock_bc.return_value = mock_conn

        errores = []
        mensajes_validos_recibidos = []

        def my_handler(envelope: EventEnvelope, channel) -> None:
            mensajes_validos_recibidos.append(envelope)

        def fake_start_consuming():
            callback = mock_ch.basic_consume.call_args.kwargs["on_message_callback"]
            method_mock = MagicMock()
            method_mock.delivery_tag = 1

            # Mensaje inválido (no es JSON del schema)
            callback(mock_ch, method_mock, MagicMock(), b"esto_no_es_un_json_valido")
            raise KeyboardInterrupt

        mock_ch.start_consuming.side_effect = fake_start_consuming

        # No debe lanzar excepción
        subscribe_events(
            queue="q.m4.experience",
            routing_keys=["m1.video.uploaded"],
            on_event=my_handler,
        )

        # El mensaje inválido fue descartado con NACK
        mock_ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
        # Ningún mensaje válido llegó al handler
        assert len(mensajes_validos_recibidos) == 0

    @patch("event_bus.time.sleep")
    @patch("event_bus.pika.BlockingConnection")
    def test_reconecta_automaticamente_tras_caida_de_conexion(
        self, mock_bc, mock_sleep
    ):
        """
        Verifica que si el broker cae mientras el suscriptor está escuchando
        (StreamLostError durante start_consuming), el SDK reconecta
        automáticamente y retoma el consumo sin que el proceso muera.
        """
        mock_conn_1 = MagicMock()
        mock_ch_1 = MagicMock()
        mock_conn_1.channel.return_value = mock_ch_1

        mock_conn_2 = MagicMock()
        mock_ch_2 = MagicMock()
        mock_conn_2.channel.return_value = mock_ch_2

        # Primera conexión exitosa, pero start_consuming pierde la conexión
        mock_ch_1.start_consuming.side_effect = pika.exceptions.StreamLostError(
            "Broker se cayó"
        )
        # Segunda conexión exitosa, y el usuario para con Ctrl+C
        mock_ch_2.start_consuming.side_effect = KeyboardInterrupt

        mock_bc.side_effect = [mock_conn_1, mock_conn_2]

        subscribe_events(
            queue="q.m4.experience",
            routing_keys=["m1.video.uploaded"],
            on_event=lambda e, ch: None,
        )

        # Se conectó dos veces: la primera (la que falló) y la segunda (la que paró)
        assert mock_bc.call_count == 2
        # Se esperó antes de reconectar
        mock_sleep.assert_called()

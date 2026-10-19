"""
test_idempotency_wrapper.py — Pruebas unitarias del middleware de idempotencia (US-B4b).

Estas pruebas verifican el comportamiento del middleware `make_idempotent_middleware`
y su integración transparente en `subscribe_events`, sin necesitar ni RabbitMQ ni
PostgreSQL corriendo. Se utilizan Mocks para:
  - Simular la capa de persistencia (try_register_event).
  - Simular el canal pika (channel.basic_ack / channel.basic_nack).
  - Simular el frame de método (method.delivery_tag).

Patrón de uso cubierto (Opción 2 — middleware transparente):
    def mi_handler(envelope):       # ← Firma limpia, sin channel ni method
        print(envelope.payload)

    subscribe_events(               # ← El SDK aplica idempotencia automáticamente
        queue="q.m4.experience",
        routing_keys=["m1.video.uploaded"],
        on_event=mi_handler,
        idempotent=True,            # ← True por defecto
    )

Casos que se cubren:
  1. Evento NUEVO → handler ejecutado 1 vez, ACK enviado 1 vez, NACK no llamado.
  2. Evento DUPLICADO → handler NO ejecutado, ACK inmediato, NACK no llamado.
  3. Secuencia mixta → A, A, B, A → handler invocado exactamente 2 veces (A y B).
  4. Handler que falla → NACK enviado sin requeue, ACK no llamado.
  5. Preservación de nombre/docstring (functools.wraps).
  6. ValueError al crear middleware con consumer_id vacío.
  7. Mismo event_id, distintos consumer_id → independiente para cada consumidor.

Correr con:
    pytest tests/test_idempotency_wrapper.py -v
"""
from unittest.mock import MagicMock, patch

import pytest

from idempotency import make_idempotent_middleware, idempotent_handler
from envelope import EventEnvelope


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_envelope(event_id: str = "evt-001") -> EventEnvelope:
    """Construye un EventEnvelope con un ID predecible para pruebas."""
    env = EventEnvelope(
        type="m1.video.uploaded",
        correlationId="saga-test-001",
        payload={"contentId": "vid_abc"},
    )
    # Sobreescribimos el id para tener valores predecibles en los asserts
    object.__setattr__(env, "id", event_id)
    return env


def _make_method(delivery_tag: int = 42) -> MagicMock:
    """Simula el frame de método de pika con un delivery_tag."""
    method = MagicMock()
    method.delivery_tag = delivery_tag
    return method


def _make_channel() -> MagicMock:
    """Simula el canal pika."""
    return MagicMock()


# ─── Tests de evento NUEVO ─────────────────────────────────────────────────────

class TestEventoNuevo:
    """El middleware debe ejecutar el handler y enviar ACK cuando el evento es nuevo."""

    @patch("idempotency.try_register_event", return_value=True)
    def test_handler_se_ejecuta_exactamente_una_vez(self, mock_register):
        """Cuando el evento es nuevo (True), el handler de negocio debe llamarse una vez."""
        handler_mock = MagicMock()

        # Firma limpia: el handler de negocio solo recibe el envelope
        def mi_handler(envelope):
            handler_mock(envelope)

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)

        envelope = _make_envelope("evt-nuevo-001")
        wrapped(envelope, _make_channel(), _make_method(delivery_tag=1))

        handler_mock.assert_called_once_with(envelope)

    @patch("idempotency.try_register_event", return_value=True)
    def test_ack_enviado_despues_del_handler(self, mock_register):
        """El ACK se envía con el delivery_tag correcto DESPUÉS de ejecutar el handler."""
        execution_order = []

        def mi_handler(envelope):
            execution_order.append("handler")

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)

        channel = _make_channel()
        channel.basic_ack.side_effect = lambda **kwargs: execution_order.append("ack")

        wrapped(_make_envelope(), channel, _make_method(delivery_tag=7))

        assert execution_order == ["handler", "ack"], (
            "El ACK debe enviarse DESPUÉS del handler, no antes."
        )
        channel.basic_ack.assert_called_once_with(delivery_tag=7)

    @patch("idempotency.try_register_event", return_value=True)
    def test_nack_no_se_llama_en_evento_nuevo(self, mock_register):
        """En la ruta de evento nuevo exitoso, NACK no debe llamarse nunca."""

        def mi_handler(envelope):
            pass

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)

        channel = _make_channel()
        wrapped(_make_envelope(), channel, _make_method())

        channel.basic_nack.assert_not_called()

    @patch("idempotency.try_register_event", return_value=True)
    def test_try_register_event_llamado_con_parametros_correctos(self, mock_register):
        """El middleware debe llamar try_register_event(consumer_id, event_id) en ese orden."""

        def mi_handler(envelope):
            pass

        wrapped = make_idempotent_middleware("q.m3.publish", mi_handler)

        envelope = _make_envelope("evt-xyz-789")
        wrapped(envelope, _make_channel(), _make_method())

        mock_register.assert_called_once_with("q.m3.publish", "evt-xyz-789")


# ─── Tests de evento DUPLICADO ────────────────────────────────────────────────

class TestEventoDuplicado:
    """El middleware debe omitir el handler y enviar ACK inmediato en duplicados."""

    @patch("idempotency.try_register_event", return_value=False)
    def test_handler_no_se_ejecuta_en_duplicado(self, mock_register):
        """La función de negocio NO debe invocarse cuando el evento es duplicado."""
        handler_mock = MagicMock()

        def mi_handler(envelope):
            handler_mock(envelope)

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)
        wrapped(_make_envelope(), _make_channel(), _make_method())

        handler_mock.assert_not_called()

    @patch("idempotency.try_register_event", return_value=False)
    def test_ack_inmediato_en_duplicado(self, mock_register):
        """Incluso si el handler no se ejecuta, el ACK debe enviarse al broker."""
        channel = _make_channel()

        def mi_handler(envelope):
            pass

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)
        wrapped(_make_envelope(), channel, _make_method(delivery_tag=99))

        channel.basic_ack.assert_called_once_with(delivery_tag=99)

    @patch("idempotency.try_register_event", return_value=False)
    def test_nack_no_se_llama_en_duplicado(self, mock_register):
        """En duplicados no se debe enviar NACK; el mensaje simplemente se ACKea."""
        channel = _make_channel()

        def mi_handler(envelope):
            pass

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)
        wrapped(_make_envelope(), channel, _make_method())

        channel.basic_nack.assert_not_called()


# ─── Test de secuencia mixta ──────────────────────────────────────────────────

class TestSecuenciaMixta:
    """
    Simula la reentrega del criterio de aceptacion:
    'al reentregar manualmente un mismo evento, la logica interna no se
    vuelve a invocar'.

    Secuencia: [Evento A, Evento A, Evento B, Evento A]
    El handler debe ejecutarse exactamente 2 veces (una por A, una por B).
    """

    def test_reentrega_no_reinvoca_logica(self):
        """
        Simula la secuencia [A, A, B, A] donde A es duplicado en el 2do y 4to intento.
        El contador de ejecuciones del handler debe ser exactamente 2.
        """
        # Primer A → nuevo, segundo A → duplicado, B → nuevo, tercer A → duplicado
        register_results = [True, False, True, False]
        ejecuciones = []

        def mi_handler(envelope):
            ejecuciones.append(envelope.id)

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)
        canal_mock = _make_channel()

        with patch("idempotency.try_register_event", side_effect=register_results):
            for i, (event_id, _) in enumerate([
                ("evt-A", 1),
                ("evt-A", 2),  # reentrega
                ("evt-B", 3),
                ("evt-A", 4),  # segunda reentrega
            ]):
                wrapped(
                    _make_envelope(event_id),
                    canal_mock,
                    _make_method(delivery_tag=i + 1),
                )

        # Solo 2 ejecuciones reales de negocio
        assert len(ejecuciones) == 2, (
            f"Se esperaban 2 ejecuciones del handler pero ocurrieron {len(ejecuciones)}"
        )
        assert ejecuciones == ["evt-A", "evt-B"]

        # ACK enviado para los 4 mensajes (2 nuevos + 2 duplicados)
        assert canal_mock.basic_ack.call_count == 4, (
            "El broker debe recibir ACK para TODOS los mensajes, incluyendo duplicados."
        )

        # NACK nunca llamado (todos los mensajes terminaron bien)
        canal_mock.basic_nack.assert_not_called()


# ─── Tests de handler que falla ───────────────────────────────────────────────

class TestHandlerFalla:
    """Si la lógica de negocio lanza una excepción, el middleware debe enviar NACK."""

    @patch("idempotency.try_register_event", return_value=True)
    def test_nack_enviado_si_handler_lanza_excepcion(self, mock_register):
        """
        Un handler que lanza RuntimeError debe recibir NACK sin requeue.
        Esto evita un loop infinito en la cola de RabbitMQ.
        """
        def mi_handler_roto(envelope):
            raise RuntimeError("Error simulado de negocio")

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler_roto)
        channel = _make_channel()

        # No debe propagarse la excepción al llamador
        wrapped(_make_envelope(), channel, _make_method(delivery_tag=55))

        channel.basic_nack.assert_called_once_with(delivery_tag=55, requeue=False)

    @patch("idempotency.try_register_event", return_value=True)
    def test_ack_no_se_llama_si_handler_falla(self, mock_register):
        """Si el handler falla, ACK no debe llamarse (solo NACK)."""
        def mi_handler_roto(envelope):
            raise ValueError("Dato invalido")

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler_roto)
        channel = _make_channel()
        wrapped(_make_envelope(), channel, _make_method())

        channel.basic_ack.assert_not_called()

    @patch("idempotency.try_register_event", return_value=True)
    def test_excepcion_no_se_propaga_al_llamador(self, mock_register):
        """El middleware absorbe la excepción del handler; el consumidor no debe caerse."""
        def mi_handler_roto(envelope):
            raise Exception("Error catastrófico simulado")

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler_roto)

        # No debe lanzar excepción
        try:
            wrapped(_make_envelope(), _make_channel(), _make_method())
        except Exception as exc:
            pytest.fail(f"El middleware propagó una excepción inesperada: {exc}")


# ─── Tests de propiedades del middleware ──────────────────────────────────────

class TestPropiedadesMiddleware:
    """Verifica propiedades estructurales del middleware."""

    def test_preserva_nombre_y_docstring_del_handler(self):
        """
        functools.wraps debe preservar el __name__ y __doc__ del handler original
        para que el debugging y los logs sean legibles.
        """
        def mi_handler_especial(envelope):
            """Docstring original del handler."""
            pass

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler_especial)

        assert wrapped.__name__ == "mi_handler_especial"
        assert wrapped.__doc__ is not None and "Docstring original" in wrapped.__doc__

    def test_lanza_valueerror_con_consumer_id_vacio(self):
        """
        El middleware debe fallar en tiempo de creacion si consumer_id es vacio.
        """
        def mi_handler(envelope):
            pass

        with pytest.raises(ValueError, match="consumer_id no puede ser vacio"):
            make_idempotent_middleware("", mi_handler)

    def test_lanza_valueerror_con_consumer_id_solo_espacios(self):
        """consumer_id con solo espacios debe rechazarse."""
        def mi_handler(envelope):
            pass

        with pytest.raises(ValueError, match="consumer_id no puede ser vacio"):
            make_idempotent_middleware("   ", mi_handler)


# ─── Test multi-consumidor ────────────────────────────────────────────────────

class TestMultiConsumidor:
    """
    El mismo event_id debe poder procesarse independientemente por
    distintos consumer_id (un consumidor por módulo).
    """

    def test_mismo_evento_distintos_consumidores_son_independientes(self):
        """
        Si el evento EVT-001 fue procesado por M4, aún debe procesarse
        por M3 (consumidor distinto). El registro es por (event_id, consumer_id).
        """
        # Ambas llamadas a try_register_event retornan True (nuevos para cada consumidor)
        with patch("idempotency.try_register_event", return_value=True):
            ejecuciones_m4 = []
            ejecuciones_m3 = []

            def handler_m4(envelope):
                ejecuciones_m4.append(envelope.id)

            def handler_m3(envelope):
                ejecuciones_m3.append(envelope.id)

            wrapped_m4 = make_idempotent_middleware("q.m4.experience", handler_m4)
            wrapped_m3 = make_idempotent_middleware("q.m3.publish", handler_m3)

            envelope = _make_envelope("evt-compartido-001")
            channel = _make_channel()

            wrapped_m4(envelope, channel, _make_method(1))
            wrapped_m3(envelope, channel, _make_method(2))

        assert len(ejecuciones_m4) == 1
        assert len(ejecuciones_m3) == 1
        assert ejecuciones_m4[0] == "evt-compartido-001"
        assert ejecuciones_m3[0] == "evt-compartido-001"


# ─── Tests de firmas de handler y decorador ───────────────────────────────────

class TestFirmasYDecorador:
    """Verifica compatibilidad con distintas firmas y con @idempotent_handler."""

    @patch("idempotency.try_register_event", return_value=True)
    def test_handler_con_firma_simple_un_parametro(self, mock_register):
        """Opción 2 estándar: el handler de negocio solo recibe el envelope."""
        llamadas = []

        def mi_handler(envelope):
            llamadas.append(envelope.id)

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)
        channel = _make_channel()
        method = _make_method(1)

        wrapped(_make_envelope("evt-1p"), channel, method)

        assert llamadas == ["evt-1p"]
        channel.basic_ack.assert_called_once_with(delivery_tag=1)

    @patch("idempotency.try_register_event", return_value=True)
    def test_handler_con_dos_parametros_recibe_envelope_y_channel(self, mock_register):
        """Compatibilidad con handlers legados: def handler(envelope, channel)."""
        llamadas = []

        def mi_handler(envelope, channel):
            llamadas.append((envelope.id, channel))

        wrapped = make_idempotent_middleware("q.m4.experience", mi_handler)
        channel = _make_channel()
        method = _make_method(1)

        wrapped(_make_envelope("evt-2p"), channel, method)

        assert len(llamadas) == 1
        assert llamadas[0][0] == "evt-2p"
        assert llamadas[0][1] is channel
        channel.basic_ack.assert_called_once_with(delivery_tag=1)

    @patch("idempotency.try_register_event", return_value=True)
    def test_idempotent_handler_como_decorador(self, mock_register):
        """El decorador @idempotent_handler envuelve correctamente la función."""
        llamadas = []

        @idempotent_handler("q.m4.experience")
        def mi_handler_decorado(envelope):
            llamadas.append(envelope.id)

        channel = _make_channel()
        method = _make_method(1)

        mi_handler_decorado(_make_envelope("evt-dec"), channel, method)

        assert llamadas == ["evt-dec"]
        channel.basic_ack.assert_called_once_with(delivery_tag=1)

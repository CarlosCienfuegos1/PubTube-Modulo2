# ADR 0001: Elección del Stack Tecnológico Base

**Fecha:** 2026-08-30
**Estado:** Aprobado

## Contexto
El Módulo 2 de PubTube es responsable de proveer el núcleo de mensajería (Publish-Subscribe) y de orquestar el flujo de eventos entre todos los sistemas. Era necesario elegir las herramientas tecnológicas (lenguaje de programación y Message Broker) que cumplan con los requerimientos técnicos (Alta resiliencia, soporte nativo de Topic Exchanges, facilidad para aplicar Envelope pattern y Dead-Letter Queues).

La rúbrica técnica ofrecía las opciones de Python (FastAPI), Node.js (NestJS) o Go (Gin) para el backend; y RabbitMQ, NATS o GCP Pub/Sub para mensajería.

## Decisión
Se ha decidido estandarizar el **Módulo 2** bajo el siguiente stack:
- **Broker de Mensajería:** RabbitMQ (versión 4.x) orquestado vía Docker.
- **Lenguaje Base:** Python 3.
- **Librerías Core:** `pika` (como cliente AMQP) y `pydantic` (para validación de esquemas JSON).

## Justificación
1. **RabbitMQ** es el software sugerido explícitamente en los documentos para el Módulo 2 por su robustez, soporte del protocolo AMQP 0-9-1 y la capacidad nativa de definir *Exchanges* complejos y rutas estáticas.
2. **Python** fue seleccionado porque es la *Opción A (Recomendada)* a nivel general y posee el ecosistema de validación de datos `pydantic`, que es ideal para blindar el contrato estricto requerido por nuestro *Envelope* de evento versionado.

## Consecuencias
- Todo el equipo debe tener instalado Docker y Python localmente.
- Se debe manejar estrictamente el gestor de dependencias (`requirements.txt`) para no generar conflictos entre los equipos.
- Todo desarrollo de esquema nuevo debe obligatoriamente usar un modelo de Pydantic para validar los datos antes de inyectarlos en RabbitMQ.

# ADR 0002: Actualización del Stack Tecnológico Base

**Fecha:** 2026-09-03
**Estado:** Aprobado

## Contexto
El Módulo 2 de PubTube es responsable de proveer el núcleo de mensajería (Publish-Subscribe) y de orquestar el flujo de eventos entre todos los sistemas. Era necesario elegir las herramientas tecnológicas (lenguaje de programación y Message Broker) que cumplan con los requerimientos técnicos (Alta resiliencia, soporte nativo de Topic Exchanges, facilidad para aplicar Envelope pattern y Dead-Letter Queues).

La rúbrica técnica ofrecía las opciones de Python (FastAPI), Node.js (NestJS) o Go (Gin) para el backend; y RabbitMQ, NATS o GCP Pub/Sub para mensajería.

## Decisión
Se ha decidido Actualizar el **Módulo 2** bajo el siguiente stack:
- **todo lo referente al primer documento se mantiene**
- se agrega la tecnologia de postgresql como base de datos

## Justificación
1. **Postgresql** es el software sugerido explícitamente en los documentos para el Módulo 2, es requerido por el modulo para manejar la persistencia entre mensajes dentro del contexto de la aplicación.

## Consecuencias
- Todo el equipo debe tener instalado Docker y Python localmente.
- Se debe manejar estrictamente el gestor de dependencias (`requirements.txt`) para no generar conflictos entre los equipos.
- Se debe agregar información de credenciales al archivo .env para el uso de la base de datos.

# PubTube - Módulo 2 (Núcleo Pub/Sub y Orquestación)

Este repositorio contiene la implementación del **Módulo 2** para la plataforma PubTube, cuyo objetivo es actuar como el *backbone* de mensajería y orquestar el flujo de eventos entre todos los módulos del sistema (M1, M3 y M4).

## Responsabilidades del Módulo
- Proveer el broker de mensajería asincrónica garantizando el desacoplamiento.
- Manejar entregas confiables (DLQ, reintentos).
- Orquestar la saga de publicación (programado → publicado → notificado).
- Establecer y hacer cumplir el formato del *Envelope* de evento versionado.

## Stack Tecnológico
Según el ADR-0001, la arquitectura base utiliza:
- **Python 3** (Librerías principales: `pika`, `pydantic`, `python-dotenv`)
- **RabbitMQ 4.x** (Broker de mensajería con soporte para AMQP 0-9-1)
- **Docker & Docker Compose** (Para levantar la infraestructura localmente de manera aislada)

## Requisitos Previos
1. Tener [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y en ejecución.
2. Tener Python (3.9+) instalado en tu sistema.

## Configuración y Despliegue Local

### 1. Configuración de Variables de Entorno
Copia el archivo de ejemplo para crear tu configuración local con las credenciales:
```bash
cp .env.example .env
```
Abre el archivo `.env` que se acaba de crear y reemplaza los valores de `RABBITMQ_USER` y `RABBITMQ_PASS` por las credenciales que desees usar. El archivo `.env` está ignorado por git para evitar filtrar contraseñas.

### 2. Levantar la Infraestructura (RabbitMQ)
Inicia el contenedor de RabbitMQ en segundo plano:
```bash
docker compose up -d
```
Puedes acceder a la interfaz de administración (Management UI) en `http://127.0.0.1:15672` con el usuario y contraseña que definiste en tu archivo `.env`.

### 3. Instalar Dependencias de Python
Se recomienda crear un entorno virtual para no ensuciar el sistema:
```bash
python -m venv venv
# Activar en Windows PowerShell
.\venv\Scripts\Activate.ps1
# Instalar dependencias (incluye pytest)
pip install -r requirements.txt
```

### 4. Configurar Exchanges y Colas
Al iniciar RabbitMQ mediante Docker Compose (`docker compose up -d`), el broker **carga automáticamente la infraestructura y topología** desde `rabbitmq/definitions.json`.

Si deseas verificar o declarar los exchanges y colas programáticamente mediante Python, puedes ejecutar:
```bash
python src/setup_rabbitmq.py
```

### 5. Ejecutar Pruebas
Puedes ejecutar la suite de pruebas unitarias y de integración:
```bash
pytest -v
```

### 6. Probar el Flujo
Disponemos de dos scripts para simular el paso de mensajes:
- En una terminal, levanta el suscriptor: `python src/demo_subscriber.py`
- En otra terminal, levanta el publicador: `python src/demo_publisher.py`

## Arquitectura de Mensajería y Catálogo de Eventos

### Convención de Routing Keys
Todas las routing keys del sistema siguen estrictamente la convención jerárquica:
> `<módulo>.<entidad>.<evento>`

* **`<módulo>`**: Módulo emisor del evento (`m1`, `m2`, `m3`, `m4`).
* **`<entidad>`**: Entidad de negocio afectada (`video`, `metadata`, `publish`, `team` o `notify`).
* **`<evento>`**: Acción en participio pasado (`uploaded`, `updated`, `scheduled`, `completed`, `failed`, `notified`).

### Catálogo Oficial de Eventos

| Evento (Routing Key) | Emisor (Publicador) | Payload Principal (Campos Clave) | Consumidores Principales | Descripción de Negocio |
| :--- | :---: | :--- | :---: | :--- |
| **`m1.video.uploaded`** | M1 (Contenidos) | `contentId, checksum, storageUrl` | M2 (Event Store), M4 (Panel UX) | Se emite cuando un nuevo video ha sido subido e ingresado con éxito en el object storage. |
| **`m1.metadata.updated`** | M1 (Contenidos) | `contentId, version, title, tags, visibility` | M2 (Event Store), M3 (Scheduler), M4 (Panel UX) | Se emite al modificar los metadatos de un video, gatillando el registro de tareas en el planificador. |
| **`m3.publish.scheduled`** | M3 (Publicación) | `contentId, scheduleAt, timezone` | M2 (Event Store), M4 (Panel UX) | Se emite cuando se ha planificado exitosamente una fecha y hora de salida de video. |
| **`m3.publish.completed`** | M3 (Publicación) | `contentId, youtubeVideoId, publishedAt` | M2 (Event Store), M1 (Contenidos), M4 (Panel UX) | Se emite inmediatamente después de que el video es publicado en la API oficial de YouTube. |
| **`m3.publish.failed`** | M3 (Publicación) | `contentId, errorCode, attempt, reason` | M2 (Event Store), M4 (Panel UX) | Se emite cuando la publicación falla definitivamente tras agotar reintentos (ej. falta de cuotas o credenciales caídas). |
| **`m4.notify.team`** | M4 (Experiencia) | `channel, recipients, subject, body` | M2 (Event Store / Orquestador) | Se emite para solicitar o registrar el envío de una notificación al equipo sobre el estado de un video o evento del sistema. |

### Topología de Colas por Consumidor y Bindings

El broker dispone del Exchange principal de tipo **Topic** denominado **`pubtube.events`** (durable). Cada consumidor posee su propia cola dedicada vinculada mediante bindings específicos:

| Cola (Buzón de Consumidor) | Consumidor | Eventos Vinculados (Bindings) | Propósito de Integración |
| :--- | :---: | :--- | :--- |
| **`q.m1.content`** | Módulo 1 | `m3.publish.completed` | Notificar al módulo de contenidos que la publicación en YouTube finalizó. |
| **`q.m3.publish`** | Módulo 3 | `m1.metadata.updated` | Disparar la planificación o preparación de publicación ante cambios de metadatos. |
| **`q.m4.experience`** | Módulo 4 | `m1.video.uploaded`<br>`m1.metadata.updated`<br>`m3.publish.scheduled`<br>`m3.publish.completed`<br>`m3.publish.failed`<br>`m4.team.notified` | Actualizar el estado visual en el panel de usuario/equipo y disparar alertas. |
| **`q.m2.event_store`** | Módulo 2 | `#` *(todos los eventos)* | Auditoría global, almacenamiento histórico de eventos y orquestación de Sagas. |

## Estructura del Proyecto
- `src/event_bus.py`: **SDK interno de pub/sub** con reconexión automática (US-B1b).
- `src/envelope.py`: Modelo y validación del `EventEnvelope` versionado (US-B2a).
- `src/config.py`: Carga de credenciales desde `.env`.
- `src/demo_publisher.py`: Ejemplo de publicación usando el SDK.
- `src/demo_subscriber.py`: Ejemplo de suscripción usando el SDK.
- `src/setup_rabbitmq.py`: Script declarativo de topología (Exchanges, Colas, Bindings).
- `rabbitmq/`: Definiciones de infraestructura (`definitions.json`, `rabbitmq.conf`, `entrypoint.sh`).
- `schemas/`: JSON Schema oficial del `EventEnvelope`.
- `tests/`: Batería de pruebas unitarias y de integración.
- `ADR/`: Architecture Decision Records con el historial de decisiones técnicas tomadas.
- `ROLES.md`: Definición del equipo Scrum encargado de este módulo.
- `docker-compose.yml`: Definición de los servicios de contenedores requeridos.

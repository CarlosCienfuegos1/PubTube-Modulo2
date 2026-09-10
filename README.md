# PubTube - Módulo 2 (Núcleo Pub/Sub y Orquestación)

Este repositorio contiene la implementación del **Módulo 2** para la plataforma PubTube, cuyo objetivo es actuar como el *backbone* de mensajería y orquestar el flujo de eventos entre todos los módulos del sistema (M1, M3 y M4).

## Responsabilidades del Módulo
- Proveer el broker de mensajería asincrónica garantizando el desacoplamiento.
- Manejar entregas confiables (DLQ, reintentos).
- Orquestar la saga de publicación (programado → publicado → notificado).
- Establecer y hacer cumplir el formato del *Envelope* de evento versionado.
- Mantener el **Event Store**: persistencia de todos los eventos del sistema en PostgreSQL, para auditoría y trazabilidad por `correlationId`.

## Stack Tecnológico
Según el ADR-0001, la arquitectura base utiliza:
- **Python 3** (Librerías principales: `pika`, `pydantic`, `python-dotenv`, `psycopg`)
- **RabbitMQ 4.x** (Broker de mensajería con soporte para AMQP 0-9-1)
- **PostgreSQL 16** (Persistencia del Event Store)
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
Abre el archivo `.env` que se acaba de crear y reemplaza los valores de `RABBITMQ_USER`, `RABBITMQ_PASS`, `POSTGRES_USER`, `POSTGRES_PASSWORD` y `POSTGRES_DB` por las credenciales que desees usar. El archivo `.env` está ignorado por git para evitar filtrar contraseñas.

> **Nota (Windows):** si tienes problemas de conexión a `localhost` (por ejemplo, la conexión a Postgres se queda "colgada"), reemplaza `RABBITMQ_HOST` y `POSTGRES_HOST` por `127.0.0.1` en tu `.env`. Esto evita que la resolución de `localhost` intente primero IPv6 (`::1`), que Docker Desktop no expone.

### 2. Levantar la Infraestructura (RabbitMQ + PostgreSQL)
Inicia los contenedores en segundo plano:
```bash
docker compose up -d
```
Esto levanta dos servicios:
- **RabbitMQ**: interfaz de administración (Management UI) en `http://127.0.0.1:15672`, con el usuario y contraseña definidos en tu `.env`.
- **PostgreSQL**: expuesto en `127.0.0.1:5432`, con la base de datos definida en `POSTGRES_DB`.

Verifica que ambos servicios estén saludables:
```bash
docker compose ps
```
Ambas filas deben mostrar `healthy`, no solo `running`.

### 3. Instalar Dependencias de Python
Se recomienda crear un entorno virtual para no ensuciar el sistema:
```bash
python -m venv venv
# Activar en Windows PowerShell
.\venv\Scripts\Activate.ps1
# Instalar dependencias (incluye pytest y psycopg)
pip install -r requirements.txt
```

### 4. Configurar Exchanges y Colas
Al iniciar RabbitMQ mediante Docker Compose (`docker compose up -d`), el broker **carga automáticamente la infraestructura y topología** desde `rabbitmq/definitions.json`.

Si deseas verificar o declarar los exchanges y colas programáticamente mediante Python, puedes ejecutar:
```bash
python src/setup_rabbitmq.py
```

El esquema de la base de datos del Event Store (tabla `events`) se crea automáticamente la primera vez que corre `src/event_store_consumer.py` (ver paso 6) — no requiere un script de setup aparte.

### 5. Ejecutar Pruebas
Puedes ejecutar la suite completa de pruebas unitarias y de integración (requiere RabbitMQ y PostgreSQL levantados para que corran también las de integración; si no están disponibles, esas se saltan automáticamente):
```bash
python -m pytest tests/ -v
```

### 6. Probar el Flujo Completo (Pub/Sub + Event Store)
Disponemos de scripts para simular el paso de mensajes y su persistencia:

- **Terminal 1** — deja corriendo el Event Store, que escucha TODOS los eventos del sistema (cola `q.m2.event_store`, bindeada a `#`) y los persiste en PostgreSQL:
  ```bash
  python src/event_store_consumer.py
  ```
- **Terminal 2** — levanta el suscriptor de demo del Módulo 4 (cola `q.m4.experience`):
  ```bash
  python src/demo_subscriber.py
  ```
- **Terminal 3** — publica un evento de ejemplo (`m1.video.uploaded`):
  ```bash
  python src/demo_publisher.py
  ```

Deberías ver el evento aparecer tanto en la Terminal 1 (confirmación de persistencia) como en la Terminal 2 (recepción por el consumidor de M4).

Para consultar directamente lo que quedó guardado en el Event Store:
```bash
docker exec -it pubtube-postgres psql -U <tu_usuario_postgres> -d <tu_db>
```
```sql
SELECT type, correlation_id, received_at FROM events ORDER BY received_at;
```

## Arquitectura de Mensajería y Catálogo de Eventos

### Convención de Routing Keys
Todas las routing keys del sistema siguen estrictamente la convención jerárquica:
> `<módulo>.<entidad>.<evento>`

* **`<módulo>`**: Módulo emisor del evento (`m1`, `m2`, `m3`, `m4`).
* **`<entidad>`**: Entidad de negocio afectada (`video`, `metadata`, `publish`, `team`).
* **`<evento>`**: Acción en participio pasado (`uploaded`, `updated`, `scheduled`, `completed`, `failed`, `notified`).


### Catálogo Oficial de Eventos

| Evento (Routing Key) | Emisor (Publicador) | Payload Principal (Campos Clave) | Consumidores Principales | Descripción de Negocio |
| :--- | :---: | :--- | :---: | :--- |
| **`m1.video.uploaded`** | M1 (Contenidos) | `contentId, checksum, storageUrl` | M2 (Event Store), M4 (Panel UX) | Se emite cuando un nuevo video ha sido subido e ingresado con éxito en el object storage. |
| **`m1.metadata.updated`** | M1 (Contenidos) | `contentId, version, title, tags, visibility` | M2 (Event Store), M3 (Scheduler), M4 (Panel UX) | Se emite al modificar los metadatos de un video, gatillando el registro de tareas en el planificador. |
| **`m3.publish.scheduled`** | M3 (Publicación) | `contentId, scheduleAt, timezone` | M2 (Event Store), M4 (Panel UX) | Se emite cuando se ha planificado exitosamente una fecha y hora de salida de video. |
| **`m3.publish.completed`** | M3 (Publicación) | `contentId, youtubeVideoId, publishedAt` | M2 (Event Store), M1 (Contenidos), M4 (Panel UX) | Se emite inmediatamente después de que el video es publicado en la API oficial de YouTube. |
| **`m3.publish.failed`** | M3 (Publicación) | `contentId, errorCode, attempt, reason` | M2 (Event Store), M4 (Panel UX) | Se emite cuando la publicación falla definitivamente tras agotar reintentos (ej. falta de cuotas o credenciales caídas). |
| **`m4.team.notified`** | M4 (Experiencia) | `channel, recipients, subject, body` | M2 (Event Store / Orquestador) | Se emite para solicitar o registrar el envío de una notificación al equipo sobre el estado de un video o evento del sistema. |

### Topología de Colas por Consumidor y Bindings

El broker dispone del Exchange principal de tipo **Topic** denominado **`pubtube.events`** (durable). Cada consumidor posee su propia cola dedicada vinculada mediante bindings específicos:

| Cola (Buzón de Consumidor) | Consumidor | Eventos Vinculados (Bindings) | Propósito de Integración |
| :--- | :---: | :--- | :--- |
| **`q.m1.content`** | Módulo 1 | `m3.publish.completed` | Notificar al módulo de contenidos que la publicación en YouTube finalizó. |
| **`q.m3.publish`** | Módulo 3 | `m1.metadata.updated` | Disparar la planificación o preparación de publicación ante cambios de metadatos. |
| **`q.m4.experience`** | Módulo 4 | `m1.video.uploaded`<br>`m1.metadata.updated`<br>`m3.publish.scheduled`<br>`m3.publish.completed`<br>`m3.publish.failed`<br>`m4.team.notified` | Actualizar el estado visual en el panel de usuario/equipo y disparar alertas. |
| **`q.m2.event_store`** | Módulo 2 | `#` *(todos los eventos)* | Auditoría global, almacenamiento histórico de eventos y orquestación de Sagas. |

## Event Store (Persistencia en PostgreSQL)

El Event Store persiste **todos** los eventos que circulan por `pubtube.events` en la tabla `events`, con el siguiente esquema:

| Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `id` | `UUID` (PK) | Identificador único del evento (del Envelope). Garantiza idempotencia: un evento con el mismo `id` no se duplica. |
| `type` | `TEXT` | Tipo de evento (ej. `m1.video.uploaded`). |
| `version` | `INTEGER` | Versión del esquema del evento. |
| `timestamp` | `TEXT` | Timestamp ISO-8601 de creación del evento (según el Envelope). |
| `correlation_id` | `UUID` | ID de correlación, para trazar un flujo/saga completo. |
| `causation_id` | `UUID` (nullable) | ID del evento que causó la emisión de este. |
| `source` | `TEXT` | Módulo de origen. |
| `routing_key` | `TEXT` | Routing key con la que se publicó el evento. |
| `payload` | `JSONB` | Cuerpo del evento. |
| `received_at` | `TIMESTAMPTZ` | Timestamp real de cuándo el Event Store lo persistió (no confundir con `timestamp`, que es de creación). |

El consumidor (`src/event_store_consumer.py`) usa **ack manual**: si la inserción en PostgreSQL falla, el mensaje se vuelve a encolar (`nack` + `requeue=True`) en vez de perderse.

## Estructura del Proyecto
- `src/`: Código fuente en Python (Envelope, setup, event store, simuladores, configuración).
- `rabbitmq/`: Definiciones de infraestructura (`definitions.json`, `rabbitmq.conf`).
- `tests/`: Batería de pruebas unitarias y de integración.
- `ADR/`: Architecture Decision Records con el historial de decisiones técnicas tomadas.
- `ROLES.md`: Definición del equipo Scrum encargado de este módulo.
- `docker-compose.yml`: Definición de los servicios de contenedores requeridos (RabbitMQ + PostgreSQL).
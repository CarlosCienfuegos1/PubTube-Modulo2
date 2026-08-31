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

### 1. Variables de Entorno
Copia el archivo de ejemplo para crear tus credenciales locales:
```bash
cp .env.example .env
```
*(Edita el archivo `.env` según necesites. El archivo `.env` está ignorado por git por seguridad).*

### 2. Levantar la Infraestructura (RabbitMQ)
Inicia el contenedor de RabbitMQ en segundo plano:
```bash
docker compose up -d
```
Puedes acceder a la interfaz de administración (Management UI) en `http://localhost:15672` con las credenciales que pusiste en el archivo `.env` (por defecto `admin` / `admin123`).

### 3. Instalar Dependencias de Python
Se recomienda crear un entorno virtual para no ensuciar el sistema:
```bash
python -m venv venv
# Activar en Windows
.\venv\Scripts\activate
# Instalar dependencias
pip install -r requirements.txt
```

### 4. Configurar Exchanges y Colas
Para crear la infraestructura interna en RabbitMQ (el exchange `pubtube.events`):
```bash
python src/setup_rabbitmq.py
```

### 5. Probar el Flujo
Disponemos de dos scripts para simular el paso de mensajes:
- En una terminal, levanta el suscriptor: `python src/demo_subscriber.py`
- En otra terminal, levanta el publicador: `python src/demo_publisher.py`

## Estructura del Proyecto
- `src/`: Código fuente en Python (Envelope, setup, simuladores).
- `ADR/`: Architecture Decision Records con el historial de decisiones técnicas tomadas.
- `ROLES.md`: Definición del equipo Scrum encargado de este módulo.
- `docker-compose.yml`: Definición de los servicios de contenedores requeridos.
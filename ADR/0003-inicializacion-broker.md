# ADR 0003: Inicialización Declarativa del Broker de Eventos

**Fecha:** 2026-09-06  
**Estado:** Aprobado

## Contexto

El Módulo 2 requiere que RabbitMQ cuente con la topología completa de mensajería (exchange `pubtube.events`, colas de consumidores y bindings) disponible desde el arranque del entorno. Depender de configuraciones manuales en la consola web o delegar la creación de colas en el código de cada microservicio genera inconsistencias, dependencias circulares y pérdida de eventos por diferencias en el orden de inicio de los contenedores Docker.

## Decisión

Se ha decidido implementar un aprovisionamiento declarativo del broker:

* **Mecanismo:** Archivo estático `definitions.json` con la definición de exchanges, colas y bindings.
* **Configuración:** Carga automática mediante la directiva `load_definitions` dentro de `rabbitmq.conf`.
* **Orquestación:** Montaje de los archivos de configuración y volúmenes persistentes gestionados vía Docker Compose.

## Justificación

* Garantiza que la infraestructura de mensajería exista en el "segundo cero", desacoplando el broker del ciclo de vida y del orden de arranque de los microservicios.
* Elimina scripts imperativos de post-arranque (`rabbitmqadmin` o Bash) y dependencias de red intermedias, manteniendo la infraestructura inmutable y auditable como código dentro del repositorio.
* Asegura reproducibilidad total para que cualquier módulo levante el entorno local verificado con healthcheck mediante un único comando (`docker compose up`).

## Consecuencias

* Los servicios cliente ya no requieren lógica repetitiva para declarar infraestructura en el broker al iniciar.
* Cualquier adición o modificación en routing keys, exchanges o colas exige actualizar manualmente el archivo central `definitions.json` en Git antes de que otros equipos lo consuman.
* Facilita la ejecución inmediata de pruebas de integración y dobles de prueba (stubs) tanto en entornos locales como en pipelines de CI.
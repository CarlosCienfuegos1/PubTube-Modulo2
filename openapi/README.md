# Contratos OpenAPI — PubTube Módulo 2

Este directorio contiene la especificación formal y contratos de interfaz OpenAPI 3.0 para la API HTTP del **Módulo 2 (Núcleo Pub/Sub y Orquestación)** de PubTube.

---

## 📁 Estructura del Directorio

| Archivo | Formato | Descripción |
| :--- | :---: | :--- |
| **`openapi.yaml`** | YAML | Especificación OpenAPI 3.0.3 completa, unificada y autocontenida de todos los endpoints del servicio. |
| **`openapi.json`** | JSON | Especificación idéntica en formato JSON (ideal para importar directamente en Postman, Insomnia o gateways). |
| **`health.yaml`** | YAML | Contrato modular enfocado exclusivamente en los endpoints de verificación de salud (`/health`, `/api/events/health`, `/events/health`). |
| **`traceability.yaml`** | YAML | Contrato modular enfocado en la consulta de trazabilidad histórica por saga (`/events/{correlation_id}`, `/api/events/{correlation_id}`). |

---

## 🌐 Endpoints Documentados

### 1. Verificación de Salud (Health Checks)
Permiten comprobar que el servicio está activo, respondiendo y listo para recibir tráfico:
- `GET /health`: Liveness probe raíz para balanceadores de carga y Kubernetes/Docker health checks.
- `GET /api/events/health`: Endpoint utilizado por el API Gateway (PubTube M4) en los smoke tests de integración CI/CD.
- `GET /events/health`: Endpoint de salud bajo el contexto de recursos de eventos.

**Respuesta exitosa (`200 OK`)**:
```json
{
  "status": "ok"
}
```

---

### 2. Trazabilidad de Sagas por `correlationId`
Permite consultar la secuencia cronológica de eventos persistidos en el Event Store de PostgreSQL asociados a una saga completa:
- `GET /events/{correlation_id}`: Consulta directa al servicio.
- `GET /api/events/{correlation_id}`: Consulta a través del API Gateway conservando el prefijo `/api`.

**Parámetro de ruta**:
- `correlation_id` *(string, requerido)*: Identificador de correlación que agrupa todos los eventos del flujo de publicación.

**Respuestas**:
- **`200 OK`**: Retorna el objeto de trazabilidad con la lista de eventos ordenados cronológicamente por momento de recepción (`received_at`).
  ```json
  {
    "correlationId": "550e8400-e29b-41d4-a716-446655440000",
    "events": [
      {
        "id": "a1a1a1a1-1111-2222-3333-444444444444",
        "type": "m1.video.uploaded",
        "version": 1,
        "timestamp": "2026-10-19T22:00:00Z",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
        "causation_id": null,
        "source": "module-1",
        "routing_key": "m1.video.uploaded",
        "payload": {
          "contentId": "vid-001",
          "storageUrl": "https://storage.pubtube.internal/videos/vid-001.mp4"
        },
        "received_at": "2026-10-19T22:00:01.123456Z"
      }
    ]
  }
  ```
- **`400 Bad Request`**: Si el `correlation_id` está vacío o no es válido:
  ```json
  {
    "detail": "'correlation_id' es obligatorio y no puede estar vacío."
  }
  ```
- **`404 Not Found`**: Si no existen eventos registrados para ese identificador:
  ```json
  {
    "detail": "No se encontraron eventos para correlationId '550e8400-e29b-41d4-a716-446655440000'."
  }
  ```

---

## 🛠️ Cómo Utilizar e Inspeccionar los Contratos

### Visualización Interactiva (Swagger UI / Redoc)
1. **FastAPI Docs (Nativo)**:
   Al ejecutar la aplicación localmente:
   ```bash
   uvicorn src.api:app --reload --port 8000
   ```
   Puedes acceder a la documentación interactiva en:
   - Swagger UI: `http://localhost:8000/docs`
   - Redoc: `http://localhost:8000/redoc`

2. **Swagger Editor Online**:
   Abre [editor.swagger.io](https://editor.swagger.io/) y arrastra o pega el contenido de `openapi/openapi.yaml`.

3. **Importar en Postman / Insomnia**:
   - En Postman: `File -> Import -> Seleccionar openapi/openapi.json`.
   - Se creará automáticamente la colección con todos los endpoints, parámetros y respuestas de ejemplo.

---

## 🧪 Ejemplos de Consulta con `curl`

### Health Check:
```bash
curl -X GET http://localhost:8000/health
```

### Consulta de Trazabilidad:
```bash
curl -X GET http://localhost:8000/events/550e8400-e29b-41d4-a716-446655440000
```

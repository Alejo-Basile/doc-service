# Diseño del documento BSON — colección `documents`

- **Versión del esquema:** 1
- **Responsable:** P2 (doc-service)
- **Depende de:** SPEC §3.6, §4, §5.4; Planificacion S0-P2-05
- **Estado:** Aceptado

---

## 1. Modelo de documento

Colección: **`documents`** (nombre configurable vía `MONGO_DATABASE`, default `documents`).

```json
{
  "_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "status": "PENDING_UPLOAD",
  "object_key": "",
  "txt_ref": "",
  "failure_reason": "",
  "correlation_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "schema_version": 1,
  "expires_at": {
    "$date": "2026-11-30T10:45:03Z"
  },
  "created_at": {
    "$date": "2026-11-30T10:15:03Z"
  },
  "updated_at": {
    "$date": "2026-11-30T10:15:03Z"
  },
  "history": [
    {
      "status": "PENDING_UPLOAD",
      "actor": "doc-service",
      "correlation_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
      "at": { "$date": "2026-11-30T10:15:03Z" }
    }
  ]
}
```

### Ejemplo en estado `COMPLETED`

```json
{
  "_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "status": "COMPLETED",
  "object_key": "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf",
  "txt_ref": "extracted-txt/01J9Z8QK3M7X2V0N4P6R8T1Y0B.txt",
  "failure_reason": "",
  "correlation_id": "corr-abc-123",
  "schema_version": 1,
  "expires_at": { "$date": "2026-11-30T10:45:03Z" },
  "created_at": { "$date": "2026-11-30T10:15:03Z" },
  "updated_at": { "$date": "2026-11-30T10:22:10Z" },
  "history": [
    { "status": "PENDING_UPLOAD", "actor": "doc-service", "correlation_id": "corr-abc-123", "at": { "$date": "2026-11-30T10:15:03Z" } },
    { "status": "UPLOADED", "actor": "minio-webhook", "correlation_id": "corr-abc-123", "at": { "$date": "2026-11-30T10:15:20Z" } },
    { "status": "QUEUED", "actor": "doc-service", "correlation_id": "corr-abc-123", "at": { "$date": "2026-11-30T10:15:21Z" } },
    { "status": "PROCESSING", "actor": "worker", "correlation_id": "corr-abc-123", "at": { "$date": "2026-11-30T10:16:00Z" } },
    { "status": "COMPLETED", "actor": "worker", "correlation_id": "corr-abc-123", "at": { "$date": "2026-11-30T10:22:10Z" } }
  ]
}
```

---

## 2. Justificación de cada campo

| Campo | Tipo BSON | Obligatorio | Justificación |
|---|---|---|---|
| `_id` | string (ULID) | sí | ULID ordenable por tiempo (SPEC §5.4). El driver Mongo lo usa como `_id` nativo; no hay campo separado `id`. |
| `status` | string | sí | Estado del ciclo de vida (SPEC §4). Se consulta para reconciliación, dashboards y debugging. |
| `object_key` | string | no (vacío hasta UPLOADED) | Clave del PDF en MinIO. Derivable de `_id` (`raw-pdfs/<id>.pdf`), se almacena para no recomputar y para poder validar consistencia. |
| `txt_ref` | string | no (vacío hasta COMPLETED) | Referencia al `.txt` extraído en MinIO. Se setea solo en la transición COMPLETED (ADR-0006). |
| `failure_reason` | string | no | Causa del fallo/rechazo. Se setea solo en transiciones a FAILED, REJECTED o EXTRACTION_FAILED (SPEC §4). |
| `correlation_id` | string | sí | ID de trazabilidad extremo a extremo. Propaga entre doc-service, Redis y worker. |
| `schema_version` | int | sí | Versión del contrato del documento. Un worker que no entiende la versión **no procesa** (SPEC §5.4). Default: 1. |
| `expires_at` | Date | sí | TTL para limpieza automática del documento y el objeto raw. Calculado como `created_at + DOC_UPLOAD_GRACE` (SPEC §4). |
| `created_at` | Date | sí | Timestamp de creación (UTC). Inmutable. |
| `updated_at` | Date | sí | Timestamp de última modificación. Se actualiza en cada transición de estado. Usado para el índice de reconciliación. |
| `history` | array de subdocumentos | sí | Historial inmutable de transiciones. Cada entrada: `status`, `actor`, `correlation_id`, `at`, `reason` (opcional). Permite auditoría y debugging sin logs externos. |

### Campos que **deliberadamente no existen**

| Campo ausente | Razón |
|---|---|
| `attempts` | El contador de reintentos es el *delivery count* de Redis `XPENDING`, no un campo del mensaje ni del documento (SPEC §5.4). Almacenarlo obligaría a reescribir el documento en cada intento para mantener un dato que Redis ya tiene. |
| `size_bytes` | No es necesario para la lógica de negocio en esta fase. Si se necesita para métricas, se agrega en una versión futura del esquema. |
| `content_type` | El cliente puede mentir; la validación real es el `%PDF-` post-subida (SPEC §9). No se almacena para evitar confiar en un dato no verificado. |
| `pdf_validated` | Se infiere del `status`: si está en `REJECTED` con `failure_reason: NOT_A_PDF`, el PDF no era válido. Un campo booleano redundante crearía dos fuentes de verdad. |

---

## 3. Índices

| Índice | Campos | Tipo | Justificación |
|---|---|---|---|
| `_id_` | `_id` | único (default de Mongo) | Clave primaria. ULID único por diseño (generado en el doc-service). |
| `status_1` | `status` | simple | El reconciliador consulta por estado (`PENDING_UPLOAD` vencidos, `UPLOADED` sin procesar, `PROCESSING` colgados). Sin este índice, cada consulta haría un collection scan. |
| `updated_at_1` | `updated_at` | simple | Complementa al índice de `status` para ordenar por última actividad en dashboards y para el job de reconciliación que busca documentos viejos. |
| `expires_at_1` | `expires_at` | **TTL** (expireAfterSeconds: 0) | MongoDB elimina automáticamente el documento cuando `expires_at` está en el pasado (SPEC §4). `expireAfterSeconds: 0` significa "expirar en la fecha exacta del campo". |

### Índice TTL: cómo funciona

- Mongo revisa el índice TTL cada 60 segundos.
- Cuando `expires_at < now()`, el documento se marca para eliminación.
- **Esto es solo una red de seguridad**: el reconciliador (Fase 2) es el que decide la expiración lógica (transición a `UPLOAD_EXPIRED`). El TTL es el fallback por si el reconciliador no corre.
- **Restricción:** `expires_at` debe ser un `Date` BSON, no un string. El driver Go lo serializa correctamente con `time.Time`.

### Índices que se crean en la inicialización

```javascript
db.documents.createIndex({ "status": 1 })
db.documents.createIndex({ "updated_at": 1 })
db.documents.createIndex({ "expires_at": 1 }, { expireAfterSeconds: 0 })
```

> El índice `_id_` ya existe por defecto en toda colección de MongoDB.

---

## 4. Transiciones y condicionalidad

Toda escritura de estado usa **transición condicional** (SPEC §4, §11.1):

```javascript
db.documents.updateOne(
  { _id: docId, status: statusActualEsperado },
  { $set: { status: nuevoStatus, updated_at: new Date() }, $push: { history: nuevaEntrada } }
)
```

Si `matchedCount === 0`, la transición **no** se aplicó (otro actor ya la hizo o el estado era incorrecto). El caller decide si reintentar o abortar.

### Matriz de transiciones (resumen)

```
PENDING_UPLOAD    → UPLOADED, UPLOAD_EXPIRED
UPLOADED          → QUEUED
QUEUED            → PROCESSING
PROCESSING        → COMPLETED, REJECTED, RETRYING, EXTRACTION_FAILED
RETRYING          → PROCESSING, EXTRACTION_FAILED
EXTRACTION_FAILED → COMPENSATING
COMPENSATING      → FAILED
COMPLETED, FAILED, REJECTED, UPLOAD_EXPIRED → (terminal)
```

---

## 5. Mapeo Go ↔ BSON

La entidad de dominio `internal/domain/document.go` lleva los tags `bson` directamente:

```go
type Document struct {
    ID            string        `bson:"_id" json:"id"`
    Status        Status        `bson:"status" json:"status"`
    ObjectKey     string        `bson:"object_key,omitempty" json:"object_key,omitempty"`
    TxtRef        string        `bson:"txt_ref,omitempty" json:"txt_ref,omitempty"`
    FailureReason string        `bson:"failure_reason,omitempty" json:"failure_reason,omitempty"`
    CorrelationID string        `bson:"correlation_id" json:"correlation_id"`
    SchemaVersion int           `bson:"schema_version" json:"schema_version"`
    ExpiresAt     time.Time     `bson:"expires_at" json:"expires_at"`
    CreatedAt     time.Time     `bson:"created_at" json:"created_at"`
    UpdatedAt     time.Time     `bson:"updated_at" json:"updated_at"`
    History       []StatusEntry `bson:"history" json:"history"`
}
```

**Decisión:** los tags `bson` viven en la entidad de dominio (no en un modelo separado de persistencia). Razones:
1. La capa `domain` no importa el driver de Mongo; los tags son solo strings en structs, no una dependencia.
2. Un modelo duplicado en la capa de persistencia introduce riesgo de divergencia entre ambos.
3. El SPEC (§3.6) pide "tipado explícito de structs/BSON", no una capa ORM.

---

## 6. Evolución del esquema

| Versión | Cambio | Migración |
|---|---|---|
| 1 | Esquema inicial | N/A |

**Regla:** cualquier cambio que agregue, renombre o elimine un campo requiere:
1. Incrementar `schema_version` en el código.
2. Registrar el cambio en esta tabla.
3. Evaluar si los workers que lean documentos antiguos pueden manejar la versión anterior (SPEC §5.4: si no entienden la versión, no procesan).
4. Si el cambio es retrocompatible (solo agrega campos opcionales), no se requiere migración de datos existentes.

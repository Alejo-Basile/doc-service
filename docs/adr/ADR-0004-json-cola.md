# ADR-0004: JSON en la cola (Redis Streams) en lugar de Protobuf

- **Estado:** Aceptada
- **Fecha:** 2026-10-04
- **Decisor:** P2 (doc-service)
- **Revisada por:** P1 (plataforma), P3 (worker), P4 (QA/integración)
- **Contexto:** SPEC §5.4; Planificacion S0-P2-06, ADR-0001/0002

---

## Contexto

El mensaje de trabajo viaja por Redis Streams desde doc-service hasta
extraction-worker. Necesitamos un formato de serialización para ese mensaje.

Opciones consideradas: JSON (texto), Protobuf (binario), MessagePack (binario),
Avro (binario con schema registry).

El mensaje es **pequeño** (5 campos: `document_id`, `object_key`, `correlation_id`,
`enqueued_at`, `schema_version`) y el throughput esperado no es extremo
(decenas de documentos por minuto, no miles por segundo).

---

## Decisión

Usamos **JSON** como formato del mensaje en Redis Streams.

### Mensaje de trabajo (contrato)

```json
{
  "document_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "object_key": "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf",
  "correlation_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "enqueued_at": "2026-11-30T10:15:03Z",
  "schema_version": 1
}
```

### Mensaje de DLQ (agrega diagnóstico)

```json
{
  "document_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "object_key": "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf",
  "delivery_count": 5,
  "correlation_id": "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
  "failed_at": "2026-11-30T10:17:44Z",
  "error_code": "PDF_CORRUPT",
  "error_message": "Failed to parse xref table at offset 91823",
  "error_kind": "business",
  "schema_version": 1
}
```

### Reglas del contrato

- El binario del PDF **nunca** viaja por la cola (SPEC §5.4). Solo metadatos.
- `object_key` es derivable de `document_id` (`raw-pdfs/<id>.pdf`): la operación
  es idempotente por diseño.
- `document_id` es un ULID (ordenable por tiempo).
- El mensaje **no** lleva `attempts`: el contador de reintentos es el
  *delivery count* de Redis `XPENDING` (SPEC §5.4).
- `schema_version` es obligatorio. Un worker que no entiende la versión
  **no procesa** el mensaje.

---

## Opciones descartadas

### Opción A: Protobuf

- **Por qué se descarta:**
  1. **Complejidad innecesaria:** para un mensaje de 5 campos, la compresión
     binaria de Protobuf no ahorra nada significativo (~200 bytes vs ~150 bytes).
  2. **Acoplamiento de toolchain:** los equipos usan Go (doc-service) y Rust
     (worker). Generar y mantener stubs de Protobuf en ambos lenguajes agrega
     un paso de build y una dependencia de `protoc`/`prost` que no justifica
     el tamaño del mensaje.
  3. **Debugging más difícil:** un mensaje JSON se lee con `jq` en un `redis-cli`.
     Un mensaje Protobuf requiere una herramienta de decodificación.
  4. **Redis Streams no serializa:** Redis almacena strings; el payload se
     serializa en el cliente. Protobuf no ahorra I/O de red, solo bytes en
     el payload — y el payload ya es pequeño.

- **Consecuencia descartada:** mayor superficie de build, peor debugging,
  sin beneficio medible en throughput ni en uso de memoria.

### Opción B: MessagePack

- **Por qué se descarta:** binario y compacto, pero no es legible sin herramienta.
  No ofrece ventaja sobre JSON para mensajes pequeños. La comunidad de Go/Rust
  lo soporta, pero agrega una dependencia sin beneficio claro.

### Opción C: Avro con Schema Registry

- **Por qué se descarta:** pensado para flujos de datos de alto throughput
  con evolución de schema compleja. Para un mensaje de 5 campos con una
  version field, el overhead de operar un Schema Registry no se justifica.

---

## Consecuencias

### Positivas

- **Debugging trivial:** `redis-cli XREAD ... | jq` muestra el contenido del
  mensaje sin herramientas especiales.
- **Sin toolchain extra:** Go usa `encoding/json` (stdlib); Rust usa `serde_json`
  (de facto estándar). Sin `protoc`, sin codegen.
- **Contrato legible:** el SPEC puede mostrar el JSON directamente; los ADRs
  y la documentación son autoexplicativos.
- **Evolución simple:** agregar un campo opcional no rompe consumidores que
  ignoren campos desconocidos (JSON es tolerante a campos extra).

### Negativas / Riesgos

- **~30% más de bytes** que Protobuf para el mismo mensaje. Irrelevante
  para mensajes de ~200 bytes a decenas de ops/min.
- **Sin schema enforcement en runtime:** el validador de `schema_version`
  lo implementa el consumer (SPEC §5.4), no el formato. Mitigación: test de
  contrato en CI que valida el JSON contra el schema documentado.
- **Tipos débiles:** `delivery_count` podría llegar como string si el productor
  tiene un bug. Mitigación: el consumer valida tipos antes de procesar.

---

## Referencias

- SPEC §5.4 (contrato de mensajes, reglas de attempts)
- ADR-0001 (por qué microservicios), ADR-0002 (por qué Strangler Fig)
- Planificacion: S0-P2-06, S2-P3-07 (conteo de intentos y DLQ atómica)

# ADR-0007: El mensaje de la cola usa campos planos, sin wrapper `payload`

**Estado**: Aceptada
**Fecha**: 2026-11-05
**Decisores**: P2 (Backend)
**Relacionada**: SPEC §5.4, ADR-0004, issue #34

## Contexto

`doc-service` publicaba en `stream:pdf-processing` un único campo `payload` con
el JSON del contrato:

```
XADD stream:pdf-processing * payload {"document_id":…,"object_key":…}
```

En cambio, la SPEC §5.4 mostraba campos planos y `extraction-worker` los usa
como representación nativa (`WorkItem::from_stream_fields`): su inspección de
la PEL (`XPENDING`/`XRANGE`) lee `document_id` y `correlation_id` directamente
de los campos del mensaje, y sus tests de integración insertan mensajes planos.
Con el wrapper, esas herramientas ven `null` y el diagnóstico de mensajes
pendientes pierde los identificadores del documento. El parser del worker
acepta ambas formas sólo como compatibilidad transitoria.

## Decisión

El contrato canónico es **un campo plano por atributo**:

```
XADD stream:pdf-processing * \
  document_id <id> \
  object_key raw-pdfs/<id>.pdf \
  correlation_id <id> \
  enqueued_at <RFC3339> \
  schema_version 1
```

- `schema_version` viaja como texto (`"1"`): Redis Streams sólo almacena
  strings; el worker lo convierte a entero al parsear.
- No se agregan campos fuera de estos cinco. `deny_unknown_fields` del worker
  hace que cualquier campo nuevo sea *breaking*; la evolución se hace subiendo
  `schema_version`.
- No se incluye `attempts`: el contador es el *delivery count* de `XPENDING`
  (SPEC §5.4).
- Se mantiene `enqueued_at`, que el worker exige y la SPEC §5.4 omitía.

## Consecuencias

### Positivas
- `XRANGE`, `XPENDING` y el replay de la DLQ muestran `document_id` y
  `correlation_id` sin decodificar un JSON anidado.
- `doc-service` y el worker comparten la misma representación; no hace falta
  cambiar el parser del worker (sólo puede retirar la rama `payload` cuando
  ya no haya mensajes viejos en vuelo).
- El mensaje sigue siendo JSON legible, en línea con ADR-0004.

### Negativas / mitigaciones
- Los mensajes ya publicados con `payload` siguen siendo legibles por el
  worker (compatibilidad transitoria); el cambio no rompe los que estén en la
  PEL al desplegar.
- Un test de contrato en `doc-service` fija los nombres y tipos de los cinco
  campos para detectar regresiones antes de que lleguen al worker.

## Alternativas descartadas

- **Mantener `payload` y cambiar al worker**: obliga a tocar P3 y deja el
  diagnóstico de la PEL sin identificadores.
- **Envelope versionado `{schema_version, payload}`**: agrega un nivel de
  anidamiento sin beneficio; `schema_version` ya es un campo del contrato.

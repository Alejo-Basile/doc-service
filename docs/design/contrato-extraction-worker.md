# Contrato doc-service ↔ `extraction-worker` (cambio de artefacto `.txt` → `.md`)

- **Estado:** propuesto para coordinación con P3 (repositorio `extraction-worker`). El worker
  **no se asume migrado**: hasta que P3 confirme e implemente, el worker puede seguir escribiendo
  `.txt`/`txt_ref` y doc-service lo sirve igual vía read-fallback (ver
  `docs/design/legacy-txt-fallback.md`).
- **Base:** SPEC §11.3 (mensaje de cola), ADR-0006 (COMPLETED del worker), cambiomd.md Fase 4.

## 1. Lo que NO cambia (ya verificado en código)

- **Mensaje de cola (SPEC §11.3):** `document_id`, `object_key` (siempre
  `raw-pdfs/<document_id>.pdf`), `correlation_id`, `enqueued_at`, `schema_version` — sin campo
  `attempts` (el contador vive en el delivery count de `XPENDING`). `deny_unknown_fields` en ambos
  lados. **La forma del mensaje no cambia** con el cambio de artefacto.
- **Webhook de MinIO:** solo acepta bucket `raw-pdfs`; los eventos de `extracted-*` se ignoran.
- **Máquina de estados:** misma transición terminal `PROCESSING → COMPLETED` condicional
  (`filter {_id, status: "PROCESSING"}`) con entrada en `status_history` (ADR-0006).

## 2. Lo que debe cambiar en `extraction-worker` (pendiente, P3)

| Antes (hoy posible) | Después (objetivo) |
|---|---|
| Sube `extracted-txt/<id>.txt` | Sube `extracted-md/<id>.md` |
| `updateOne` con `$set: {status: "COMPLETED", txt_ref: "<key>", ...}` | `updateOne` con `$set: {status: "COMPLETED", "md_ref": "extracted-md/<id>.md", ...}` |
| Verificación previa: `HEAD` del `.txt` | Verificación previa: `HEAD` del `.md` |
| Compensación: borra el `.txt` subido | Compensación: borra el `.md` subido |

- El worker **solo debe escribir `md_ref`**. El campo `txt_ref` queda como solo lectura de
  compatibilidad en doc-service (D5).
- Mientras el worker no migre, doc-service resuelve `md_ref` y, si falta, `txt_ref` con su bucket
  correspondiente: **no hace falta deploy simultáneo**. La migración del worker puede ser
  independiente y gradual.

## 3. Failure-reason `TXT_REF_MISSING` (T4.5)

Se conserva el código histórico `TXT_REF_MISSING` (`FailureReasonTxtMissing`). Es un valor
persistido en Mongo/API y compartido con el worker. Renombrarlo a `MD_REF_MISSING` requiere
coordinación explícita con P3 y compatibilidad para valores legacy ya persistidos; hasta entonces,
cualquier cambio es breaking. **Decisión: mantener el código hasta coordinar el rename.**

## 4. Tareas de coordinación (fuera de este repo)

- **T4.1 (P3):** implementar los cambios de la sección 2 y confirmar que escribe
  `md_ref` = `extracted-md/<id>.md` al cerrar `PROCESSING → COMPLETED`.
- **T4.2 (P1/infra):** crear el bucket `extracted-md` con política equivalente a `extracted-txt`
  (escritura solo worker, lectura doc-service y descarga). **No modificar el repo `infra` desde
  doc-service.**
- **Criterio de cierre:** un documento procesado por el worker migrado termina `COMPLETED` con
  `md_ref` no vacío, `.md` presente en `extracted-md` y descarga OK; un documento legacy con
  `txt_ref` sigue descargable vía fallback.

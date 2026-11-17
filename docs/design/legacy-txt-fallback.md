# Compatibilidad legacy: read-fallback `txt_ref` (D5)

- **Estado:** implementado (cambiomd.md Fase 3)
- **Alcance:** solo lectura. Ninguna escritura nueva usa `txt_ref`.

## 1. Qué es y por qué

El pipeline de extracción cambió su artefacto de `.txt` (`extracted-txt`, campo `txt_ref`)
a Markdown (`.md`, `extracted-md`, campo `md_ref`). En lugar de una migración masiva de los
datos existentes (**decisión D5**), se implementa un **fallback de lectura**: los documentos
históricos siguen siendo descargables y verificables sin tocar sus datos ni sus objetos.

## 2. Semántica

- **Nivel de referencia, no de objeto:** `ExtractedRef()` resuelve `md_ref` primero;
  **solo si `md_ref` está vacío** cae a `txt_ref` (legacy). Si `md_ref` está seteado,
  **nunca** se cae a `txt_ref`, aunque el objeto `.md` no exista — eso es caso de detección
  del reconciliador, no de fallback.
- **Bucket por tipo de referencia:** `md_ref` → `extracted-md`; `txt_ref` → bucket legacy
  (`MINIO_LEGACY_BUCKET_TXT`, default `extracted-txt`).
- **Escrituras:** exclusivamente `md_ref`. El campo `TxtRef` del dominio es de solo lectura
  de compatibilidad.
- **Idempotente:** la resolución es una función pura del documento; repetirla no cambia nada.

## 3. Dónde aplica

| Sitio | Comportamiento |
|---|---|
| `GET /api/v2/documents/{id}/download` | Presigna desde el bucket según la referencia resuelta (md o legacy). Sin ninguna referencia → 409. |
| `GET /api/v2/documents/{id}` | Expone `txt_ref` en la respuesta cuando existe (documentos legacy). |
| Reconciliador (`sweepCompletedMissingMd`) | Verifica existencia del artefacto resuelto en su bucket. |
| `crossCheck` | Igual que arriba para la lista `completed_no_md`. |

## 4. Contrato con `extraction-worker` (repo externo, P3)

- **Hoy** el worker puede seguir escribiendo `txt_ref` (artefacto `.txt`): el fallback lo cubre
  sin cambios. **No asumir que ya genera Markdown.**
- **Cuando migre**, el worker debe escribir `md_ref` = `extracted-md/<id>.md` y subir el `.md`
  a `extracted-md`, cerrando `PROCESSING → COMPLETED` como marca ADR-0006.
- `TXT_REF_MISSING` (`FailureReasonTxtMissing`) se conserva como código compartido con el
  worker. Renombrarlo a `MD_REF_MISSING` requiere coordinación con P3 y compatibilidad para
  valores legacy persistidos.

## 5. Objetos TXT históricos

**Jamás se modifican ni eliminan desde doc-service.** La protección es estructural: el puerto
`ObjectStorage` no expone `DeleteLegacyTXT` ni `ListObjectsLegacyTXT` — no hay código que pueda
borrar o listar el bucket legacy. Su limpieza futura es decisión de infraestructura (lifecycle
del bucket o migración manual) — pendiente de coordinar con P1 (cambiomd.md Fase 4).

## 6. Cómo retirar el fallback (runbook)

Cuando el worker ya genere Markdown y no queden datos legacy:

1. Verificar que `extraction-worker` escribe `md_ref` = `extracted-md/<id>.md` (Fase 4).
2. Migrar (script puntual `txt_ref` → `md_ref` + copia del objeto) **o** dejar expirar los
   documentos legacy por su TTL.
3. Confirmar en Mongo que no existen documentos `COMPLETED` con `txt_ref` y sin `md_ref`.
4. Eliminar en un solo commit:
   - `TxtRef` y `ExtractedRef()` de `internal/domain/document.go` (+ tests);
   - `StatLegacyTXT`/`PresignGetLegacyTXT` de `internal/ports`, `internal/adapters/minio`,
     `internal/resilience/decorators.go` y los mocks de tests;
   - la rama `legacy` de `DownloadDocument` y de las verificaciones del reconciliador;
   - `MINIO_LEGACY_BUCKET_TXT` de config, `.env.example` y `cmd/api/main.go`;
   - los tests marcados como fallback/legacy.
5. Recién entonces considerar la limpieza del bucket `extracted-txt` (lifecycle o manual).

**Rollback de esta fase:** `git revert` del commit de Fase 3 de `cambiomd.md`. No toca datos:
el fallback es solo de lectura.

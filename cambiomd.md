# cambiomd.md — Cambio de salida de extracción: `.txt` → `.md`

> Feature document del cambio. Creado: 2026-11-17.
> Rama: `feat/cambio-md` (stacked sobre `feat/reconciliador-p2`).
> Estrategia de entrega: `feature-branch-chain` por fases (una unidad de commit/PR por fase).
> Forecast de líneas: Fase 1 ≈ 400–600 (rename mecánico + tests); el resto por fase < 200.

## 1. Contexto

El pipeline de extracción cambió: la salida ya **no es texto plano** (`.txt` en el bucket
`extracted-txt`, campo `txt_ref`), sino **Markdown** (`.md`). El flujo sigue siendo:
el cliente sube PDFs (`raw-pdfs/<id>.pdf`), el `extraction-worker` procesa el PDF y
publica el resultado; doc-service valida existencia, purga huérfanos y sirve la descarga.
doc-service debe adaptar sus contratos al artefacto nuevo.

Lo que **no** cambia:

- El lado de entrada: presign POST sobre `raw-pdfs`, validación `%PDF-`, webhook de MinIO.
- El mensaje de cola (SPEC §11.3): `document_id`, `object_key` (`raw-pdfs/<id>.pdf`),
  `correlation_id`, `enqueued_at`, `schema_version`. La forma del mensaje no cambia.
- La máquina de estados (`internal/domain`): mismas transiciones, mismos terminales.
- ADR-0006: el worker sigue escribiendo `PROCESSING → COMPLETED` directo en Mongo.

## 2. Decisiones y supuestos

| # | Decisión | Default elegido | Estado |
|---|----------|-----------------|--------|
| D1 | Bucket de salida | `extracted-txt` → `extracted-md` (default; configurable con `MINIO_BUCKET_MD`) | asumido |
| D2 | Campo Mongo/API | `txt_ref` → `md_ref` (dominio `TxtRef` → `MdRef`) | asumido |
| D3 | Clave de objeto | `<document_id>.txt` → `<document_id>.md` (la escribe el worker; doc-service solo la almacena/lee) | asumido |
| D4 | Mecanismo de entrega | Sigue siendo URL prefirmada (`GET /documents/{id}/download`); cambia el campo `txt_ref`→`md_ref` en las respuestas | asumido |
| D5 | Datos existentes (`txt_ref` en Mongo, objetos `.txt` legacy) | Fase 3: fallback temporal de lectura **o** script de migración | **pendiente de decisión** |
| D6 | Contrato con `extraction-worker` (repo externo) | El worker debe escribir `md_ref` = `extracted-md/<id>.md`; coordinación en Fase 4 | **externo** |

Si un supuesto no coincide con el cambio real (p. ej. infra fija otro nombre de bucket),
corregir esta tabla antes de avanzar a Fase 2.

## 3. Mapa de impacto (estado actual)

| Área | Archivo | Qué cambia |
|------|---------|------------|
| Dominio | `internal/domain/document.go:188` | `TxtRef` (`bson:"txt_ref"`/`json:"txt_ref"`) → `MdRef` (`md_ref`) |
| Puertos | `internal/ports/repository.go` | `ObjectStorage`: `StatTXT`, `DeleteTXT`, `PresignGetTXT`, `ListObjectsTXT` → `*MD` |
| Config | `internal/config/config.go` | `MinIOBucketTXT`, `MINIO_BUCKET_TXT`, default `extracted-txt` → `MinIOBucketMD`, `MINIO_BUCKET_MD`, `extracted-md` |
| MinIO | `internal/adapters/minio/storage.go` | `Config.BucketTXT`, `bucketTXT`, los 4 métodos del puerto |
| Resiliencia | `internal/resilience/decorators.go` | decorador breaker de los 4 métodos |
| Mongo | `internal/adapters/mongo/repository.go:99` | comentario de merge de `extraSet` (`txt_ref` → `md_ref`) |
| API | `internal/api/documents.go:200,239–256` | clave de respuesta `"txt_ref"` → `"md_ref"`; mensajes/logs de `DownloadDocument` |
| Reconciler | `internal/reconciler/reconciler.go` | `purgeOrphanTXT`, `deleteTXTObject`, `validTxtKeys`, cross-check `StatTXT` (≈636–750, 1018–1127) |
| Wiring | `cmd/api/main.go:97` | `BucketTXT: cfg.MinIOBucketTXT` → `BucketMD: cfg.MinIOBucketMD` |
| Env | `.env.example:19` | `MINIO_BUCKET_TXT=extracted-txt` → `MINIO_BUCKET_MD=extracted-md` |
| Tests | `internal/{config,api,reconciler,adapters/mongo,adapters/minio,resilience,webhook,pdfsvc,domain}` | fixtures `extracted-txt/*.txt` → `extracted-md/*.md`, `TxtRef`→`MdRef`, mocks `*TXT`→`*MD` (9 archivos) |
| Docs (Fase 2) | `docs/design/mongo-schema.md`, `docs/adr/ADR-0006-*`, `docs/SPEC.md`, `docs/pasos-iniciales.md` | ejemplos y tablas de contrato |

## 4. Fases

### Fase 1 — Contratos y rename TXT→MD en Go + tests [COMPLETA — commit `35b9fa9`]

Objetivo: que el backend hable de `md` de punta a punta y la suite hermética pase en verde.

- [x] T1.1 `internal/domain/document.go`: `TxtRef` → `MdRef` con `bson:"md_ref,omitempty" json:"md_ref,omitempty"`
- [x] T1.2 `internal/ports/repository.go`: `StatTXT`→`StatMD`, `DeleteTXT`→`DeleteMD`, `PresignGetTXT`→`PresignGetMD`, `ListObjectsTXT`→`ListObjectsMD` (+ comentarios)
- [x] T1.3 `internal/config` + `config_test.go`: `MinIOBucketTXT`→`MinIOBucketMD`, env `MINIO_BUCKET_MD`, default `extracted-md` — TDD: RED observado (`default esperado 'extracted-md', got "extracted-txt"`) → GREEN
- [x] T1.4 `internal/adapters/minio/storage.go` + test: `BucketTXT`→`BucketMD`, `bucketTXT`→`bucketMD`
- [x] T1.5 `internal/resilience/decorators.go` + test: renombre de los 4 métodos decorados
- [x] T1.6 `internal/adapters/mongo/repository.go` + test: comentario/keys `txt_ref`→`md_ref` (los tests de `extraSet` usan la clave literal)
- [x] T1.7 `internal/api/documents.go` + `documents_test.go`: campo de respuesta `md_ref`, mensajes/errores/logs de `DownloadDocument` — TDD: RED observado (`campo "md_ref" no encontrado en la respuesta`) → GREEN
- [x] T1.8 `internal/reconciler/reconciler.go` + test: `purgeOrphanTXT`→`purgeOrphanMD`, `deleteTXTObject`→`deleteMDObject`, `validTxtKeys`→`validMdKeys`, mensajes y acción de dry-run `delete_txt_object`→`delete_md_object`
- [x] T1.9 Mocks restantes: `internal/webhook/handler_test.go` (fixture "wrong bucket" → `extracted-md/doc-1.md`), `internal/pdfsvc/validator_test.go`
- [x] T1.10 `internal/domain/document_bson_test.go`: TDD sobre el tag `md_ref` (RED: `campo BSON "md_ref" no encontrado ... Keys: [_id txt_ref ...]` → GREEN) + fixtures
- [x] T1.11 `.env.example`: `MINIO_BUCKET_MD=extracted-md`
- [x] T1.12 Verificación: `go vet`, `gofmt`, suite hermética — todo verde (10 paquetes ok, 0 FAIL; spot check del padre con la suite completa: exit=0)
- [x] T1.13 Commit de trabajo (tests + código juntos): `35b9fa9` `feat(p2)!: cambiar artefacto extraído txt a md (md_ref, extracted-md)` — 19 archivos, 265+/265−
- [x] T1.14 (delta) Identificadores internos del reconciler: `CompletedNoTxt`→`CompletedNoMd` (+`json:"completed_no_md"`), `TXTBucketObjects`→`MDBucketObjects` (+`json:"md_bucket_objects"`), `sweepCompletedMissingTxt`→`sweepCompletedMissingMd`, nombres de test/fixture (`...SinMd...`, `doc-no-md`, `doc-md-missing`). Incluido en `35b9fa9`.

Reglas de la fase:

- Renombrado mecánico: no cambia la máquina de estados ni los contratos de entrada.
- TDD: los 3 cambios con delta de comportamiento (default de config, campo bson/json,
  clave de respuesta) observan RED antes de implementar. Para renombres puros de
  identificadores no hay RED significativo más allá de la compilación: se actualizan
  tests y código en el mismo commit (regla del repo) y la suite queda verde.
- Superficie de edición acotada a los archivos de este plan. No tocar `docs/` (Fase 2).

### Fase 2 — Documentación del repo

- [ ] T2.1 `docs/design/mongo-schema.md`: ejemplo de documento → `"md_ref": "extracted-md/<id>.md"`
- [ ] T2.2 `docs/adr/ADR-0006-worker-completed-directo.md`: `.txt`→`.md`, `extracted-txt`→`extracted-md`
- [ ] T2.3 `docs/SPEC.md` (copia local): tabla de buckets, §6.5 huérfanos, montaje de buckets. ⚠ La fuente única de verdad es `infra/docs/SPEC.md` (repo `infra`): anotar que el cambio debe propagarse allí; **no** modificar el repo `infra` desde acá
- [ ] T2.4 `docs/pasos-iniciales.md`: tabla de objetos §11 (`extracted-txt`/`<id>.txt` → `extracted-md`/`<id>.md`) y referencias de contrato (S1-P1-04/06, S3-P2-09); el plan histórico no se reescribe entero
- [ ] T2.5 `README.md` / `AGENTS.md`: actualizar menciones a `extracted-txt`/`txt_ref` si las hay
- [ ] T2.6 Verificación de coherencia: sin referencias residuales a `extracted-txt`, `txt_ref` o `StatTXT` fuera de lo histórico

### Fase 3 — Migración y compatibilidad de datos existentes (decisión D5)

- [ ] T3.1 Decidir estrategia: (a) fallback temporal de lectura (`md_ref` y, si falta, `txt_ref`) o (b) script de migración Mongo `txt_ref`→`md_ref` + retiro del bucket viejo
- [ ] T3.2 Implementar la estrategia elegida con tests
- [ ] T3.3 Runbook: objetos `.txt` legacy en `extracted-txt` (se conservan hasta el lifecycle o se migran)

### Fase 4 — Contrato con `extraction-worker` e infra

- [ ] T4.1 Verificar que el worker escribe `md_ref` = `extracted-md/<id>.md` al marcar `PROCESSING→COMPLETED` (ADR-0006)
- [ ] T4.2 Infra (repo `infra`, no desde acá): crear bucket `extracted-md` con política equivalente a `extracted-txt` (escritura solo worker, lectura doc-service y descarga)
- [ ] T4.3 Mensaje de cola: sin cambios de forma; `schema_version` se mantiene (documentar en el PR)
- [ ] T4.4 Webhook MinIO: sigue filtrando solo `raw-pdfs`; sin cambios de código
- [ ] T4.5 Failure-reason `TXT_REF_MISSING` (`FailureReasonTxtMissing` en `internal/domain/document.go`): valor persistido en Mongo/API y compartido con el worker. Decidir con el worker si pasa a `MD_REF_MISSING` (con compat para valores legacy) o se conserva el código histórico. **Quedó fuera de Fase 1 a propósito**.

### Fase 5 — Verificación E2E y cierre

- [ ] T5.1 Suite de integración con servicios (Mongo **replica set**, Redis, MinIO) según `AGENTS.md`
- [ ] T5.2 E2E chain (si se restaura `tests/e2e/chain`): `md_ref` no vacío, `.md` presente en `extracted-md`, descarga OK
- [ ] T5.3 Actualizar `docs/ESTADO.md` (avance, PRs, bloqueos, próxima tarea)

## 5. Entrega

- **Rama**: `feat/cambio-md` sobre `feat/reconciliador-p2`. Motivo: el rename toca
  `purgeOrphanTXT`/`purgeOrphanMD`, código que solo existe en esa rama (commits
  `22ba4d0..a09bb2c`, aún sin mergear a `main`).
- **Cadena de PRs**: primero `feat/reconciliador-p2` → `main` (decisión del usuario);
  luego `feat/cambio-md` → `main` (o PR apilado contra `feat/reconciliador-p2`).
- Un commit lógico por fase. Fase 1 = 1 commit (rename + tests juntos, regla del repo).
- Commits convencionales en español, sin atribución AI. No mergear: reviewer ≠ autor.
- Rollback: `git revert` del commit de la fase; el rename no toca datos.

## 6. Verificación (definición de hecho)

```bash
# Gate de lint (ambos deben pasar):
go vet ./...
test -z "$(gofmt -l .)"

# Suite hermética (idéntica al job `unit` de CI):
PKGS=$(go list ./... | grep -vE 'internal/adapters/(mongo|changestream|relay|redis)$')
go test -race -count=1 $PKGS
```

- La suite de integración (`mongo`, `changestream`, `relay`, `redis`, `minio`, `metrics`)
  requiere servicios levantados; en local sin servicios es esperado que falle/salte.
- Fase 5 agrega la verificación E2E con servicios reales.

## Registro de progreso

- 2026-11-17: plan creado; Fase 1 implementada (delegada a un writer acotado + delta).
  Commit `35b9fa9` (19 archivos). Verificación: `go build` OK, `go vet` OK, `gofmt` OK,
  suite hermética 10/10 paquetes ok (spot check del padre con la suite completa: exit=0).
  Pendiente: Fase 2 (docs), Fase 3 (decisión D5), Fase 4 (worker/infra + T4.5), Fase 5 (E2E).
  Nota: la suite de integración no se corrió en local (requiere Mongo replica set, Redis,
  MinIO); queda para Fase 5 / CI.

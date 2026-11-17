# Feature: Reconciliador P2 — dry-run + huérfanos extracted-txt + estados trabados (SPEC §6.5)

## Objetivo
Completar el job de reconciliación según SPEC §6.5: modo **dry-run** antes de toda acción
destructiva, limpieza de huérfanos también en `extracted-txt`, y **recuperación real**
(reencolar o `FAILED`) de los estados intermedios trabados.

## Problema
(`tareas-revision.md`, Prioridad 2) Hoy el job no tiene dry-run aunque el SPEC exige
"se ejecuta con dry-run antes de destruir nada"; la limpieza de objetos sin registro solo
cubre `raw-pdfs`; y `sweepStuckIntermediate` solo detecta/loguea — no reencola ni marca
`FAILED`, así que un worker muerto deja documentos trabados para siempre.

## Por qué
- SPEC §6.5 es la fuente única de verdad para el job: dry-run primero, huérfanos en
  **ambos** buckets (`raw-pdfs` y `extracted-txt`), y estados intermedios viejos se
  reencolan (si el objeto existe en MinIO) o se marcan `FAILED`.
- Los objetos huérfanos de `extracted-txt` acumulan costo de almacenamiento sin control
  (el SPEC lo explícita como objetivo del sweep).

## Scope
- **T1** — Modo dry-run: config `RECONCILE_DRY_RUN` + override `?dry_run=1` en
  `POST /internal/reconcile`; todas las mutaciones planificadas y logueadas, ninguna
  ejecutada; reporte planificado vs ejecutado.
- **T2** — Huérfanos de `extracted-txt`: segundo pass en `sweepOrphans` con
  `ListObjectsTXT` + `DeleteTXT` (nuevo método de `ports.ObjectStorage`, adaptador MinIO
  y decorador resiliente), mismo umbral de edad que raw, sin tocar txt referenciados
  (`doc.TxtRef`).
- **T3** — Estados intermedios trabados (`PROCESSING`/`RETRYING`/`COMPENSATING` con
  `updated_at` > umbral): `HEAD` del objeto → existe = reencolar (patrón de
  `internal/api/internal.go` retry handler); no existe = completar la vía de fallo de la
  SAGA hasta `FAILED`.

## Diseño T3 — resolución de la contradicción del SPEC (decisión documentada)
SPEC §6.5 manda "los marca `FAILED`" pero el diagrama de SPEC §4 no tiene arista
`PROCESSING→FAILED` (la matriz de `internal/domain` la implementa literal). Resolución:
**cero cambios a la matriz** — se completa la vía de fallo que la SAGA ya define:
- Objeto presente: `PROCESSING→RETRYING` + `Enqueue` (arista §4 "Fallo transitorio");
  `RETRYING` solo `Enqueue` (el `RETRYING→PROCESSING` lo hace el worker —
  `internal/api/internal.go:95-156` es el patrón a espejar).
- Objeto ausente: `→EXTRACTION_FAILED (OBJECT_MISSING) →COMPENSATING →FAILED`.
  Son las mismas aristas que habría recorrido el worker vivo ante un error de negocio
  ("van directo a `EXTRACTION_FAILED`", SPEC §4) y la constante
  `FailureReasonObjectMissing` ya existe en la taxonomía exactamente para esto.
- `COMPENSATING` trabado: `→FAILED (COMPENSATION_FAILED)` (arista y constantes existentes).
- Alternativa descartada: aristas directas `PROCESSING/RETRYING→FAILED`. Es literal a
  §6.5 pero contradice el diagrama §4 de la fuente única de verdad (repo `infra`, que no
  se modifica desde acá) y saltea la limpieza de recursos que `COMPENSATING` representa.

## Fuera de scope
- `PENDING_UPLOAD` vencidos, `sweepStuckUploaded` y `sweepStuckQueued` (ya implementados).
- Cualquier cambio a la matriz de `internal/domain`.
- Umbrales por config (`stuckAfter` sigue hardcodeado 30m en `cmd/api/main.go`) — YAGNI.
- Métricas de acciones planificadas (slog alcanza).
- Prioridades 3/4 de `tareas-revision.md`, E2E y failover de Sentinel (bloqueados por infra).

## Restricciones (del equipo)
- Invariantes preservadas: transiciones condicionales `filter {_id, status}` + entrada en
  `status_history` (fecha, actor, `correlation_id`); `WorkMessage` SIN campo `attempts`
  (el contador vive en el delivery count de `XPENDING`); `deny_unknown_fields`; driver
  `mongo-driver/v2`.
- Mensaje de cola SPEC §11.3: `document_id`, `object_key` (`raw-pdfs/<document_id>.pdf`),
  `correlation_id`, `enqueued_at`, `schema_version` — espejar el armado del retry handler.
- El worker escribe `PROCESSING→COMPLETED` directo en Mongo (ADR-0006): no se toca.
- Código/tests siguen el estilo del archivo que se extienden; commits en español
  (Conventional Commits).

## Tareas
- [x] T1 — Modo dry-run: config + guards de mutación + reporte — SPEC §6.5
- [x] T2 — Huérfanos de `extracted-txt`: pass extra + `DeleteTXT` en puerto/adaptador/decorador — SPEC §6.5
- [x] T3 — Estados trabados: `HEAD` → reencolar o completar vía de fallo hasta `FAILED` — SPEC §6.5

## Criterios de aceptación
1. `RECONCILE_DRY_RUN=true` (o `?dry_run=1`): la corrida detecta igual, loguea el plan y
   no ejecuta **ninguna** mutación (0 updates, 0 enqueues, 0 deletes); el reporte distingue
   planificado vs ejecutado. RED → GREEN observado.
2. `sweepOrphans` elimina objetos de `extracted-txt` sin registro válido (mismo umbral de
   edad que raw) y jamás un txt referenciado por un documento (`TxtRef`) — test con ambos
   buckets.
3. Un `PROCESSING` trabado con objeto presente termina `RETRYING` + mensaje encolado; con
   objeto ausente termina `FAILED` con `failure_reason=OBJECT_MISSING` y las transiciones
   auditadas en `history`; un `COMPENSATING` trabado termina `FAILED` con
   `COMPENSATION_FAILED`. Idempotente: dos corridas seguidas no duplican encolados ni
   producen transiciones inválidas.
4. Suite hermética en verde (`go test -race` sobre el filtro de CI), `gofmt` y `go vet`
   limpios.

## Checks
```bash
# Gate de lint de CI:
go vet ./... && test -z "$(gofmt -l .)"
# Suite hermética (idéntica al job `unit` de CI):
PKGS=$(go list ./... | grep -vE 'internal/adapters/(mongo|changestream|relay|redis)$')
go test -race -count=1 $PKGS
```
- Integración: dejarla al job `integration` de CI (necesita Mongo réplica-set + Redis + MinIO).

## Entrega
- Estrategia: **`single-pr`** — rama `feat/reconciliador-p2` + PR único con work-unit
  commits (T1, T2, T3 + docs), mismo criterio que P1 (PR #79, ~1200 líneas,
  squash-mergeado). El presupuesto de ~400 líneas se excede a propósito: se reporta el
  overage y los commits quedan como slices retro-encadenables si hiciera falta.
- Ruta por tarea: **delegada** (writer trigger: cada tarea toca 2+ archivos no triviales).
- Slice final: `a238083` (docs P1), `22ba4d0` (T1, 466 l), `4da618b` (T2, 282 l),
  `afd5c19` (T3, 638 l) + commit de docs. Total ~1386 líneas de autor — overage ~3.5x
  del presupuesto, reportado; los commits son slices retro-encadenables si hiciera falta.

## Progreso
| Tarea | Estado | Commit | Assessment RDD |
|---|---|---|---|
| T1 | hecho | `22ba4d0` | medium / slice_budget_reached → declined (omitido a decisión del usuario, 2026-11-17) |
| T2 | hecho | `4da618b` | high / high_risk → consent no recolectable (tool `question` ausente); omitido por criterio de usuario (2026-11-17). Verificador independiente: clean + spot check del padre |
| T3 | hecho | `afd5c19` | medium / slice_budget_reached → consent no recolectable (tool `question` ausente); omitido por criterio de usuario (2026-11-17). Writer self-verification + spot check (tier medium) |

## Próximo paso
Feature cerrada. Pendiente: push + PR a `main` (decisión del usuario) y las
prioridades 3/4 de `tareas-revision.md`.

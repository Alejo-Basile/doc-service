# Feature: Resiliencia P1 — jitter en backoff + circuit breaker (SPEC §7)

## Objetivo
Completar los patrones de resiliencia del SPEC §7 en doc-service: backoff exponencial
**con jitter** en el relay (§7.1), **circuit breaker** por dependencia MinIO/Mongo/Redis
(§7.2) y la **composición correcta** retry→breaker (§7.3), con test que demuestra que un
breaker abierto rechaza sin tocar la red.

## Problema
(`tareas-revision.md`, Prioridad 1) El backoff del relay es duplicado determinista sin
jitter (`internal/adapters/relay/relay.go`), no existe circuit breaker en el repo, y sin
composición explícita cada reintento vuelve a golpear una dependencia caída — retry storm
justo cuando menos presión hace falta (SPEC §7.3).

## Por qué
- SPEC §7.1–§7.3 es la fuente única de verdad; el plan referencia la composición por
  dependencia en `S5-P3-01` ("cada dependencia tiene su propia instancia de breaker").
- Con MinIO/Mongo/Redis caídos, hoy la presión se multiplica sobre el servicio herido.

## Scope
- **T1** — jitter en `enqueueWithRetry` del relay (equal jitter + hook inyectable).
- **T2** — paquete `internal/resilience`: breaker `sony/gobreaker/v2` por dependencia
  (mongo/minio/redis), decoradores de `ports.ObjectStorage`, `ports.DocumentRepository` y
  `ports.WorkQueue`, wiring en `cmd/api/main.go`.
- **T3** — composición: el relay aborta reintentos ante el rechazo rápido del breaker
  (`ErrOpenState`/`ErrTooManyRequests`) + test de composición sin-red.

## Fuera de scope
- Jitter en el watcher del change stream (`changestream/watcher.go`) — también es
  determinista hoy; queda para tarea aparte.
- Breaker sobre `DistributedLease`, `ResumeTokenStore`, watcher y metrics collector
  (son background o bypassean los puertos).
- Health probes (`/readyz`, `/healthz`) siguen tocando la red real a propósito:
  diagnóstico honesto, nunca detrás de un breaker.
- Env vars para umbrales del breaker: constantes con los valores del SPEC §7.2 (YAGNI).
- Prioridad 2 (reconciliador) y 3/4 de `tareas-revision.md`.

## Restricciones (del equipo)
- Artefactos técnicos en inglés (código, tests, mensajes); commits en español.
- Invariantes preservadas: transiciones condicionales `filter {_id, status}` +
  `status_history`; `WorkMessage` SIN campo `attempts`; driver `mongo-driver/v2`.
- Errores de negocio (`ErrNotFound`, `ErrDuplicate`, `ErrInvalidCursor`) y
  `context.Canceled` NO cuentan como fallo del breaker.
- El worker escribe `PROCESSING → COMPLETED` directo (ADR-0006): no se toca.

## Tareas
- [ ] T1 — Jitter en el backoff del relay (`internal/adapters/relay/relay.go` + tests) — SPEC §7.1
- [ ] T2 — `internal/resilience`: breaker por dependencia + decoradores de puertos + wiring — SPEC §7.2
- [ ] T3 — Composición retry→breaker: fail-fast del relay + test de composición — SPEC §7.3

## Criterios de aceptación
1. Backoff del relay = exponencial + equal jitter (`d/2 + rand[0, d/2]`), hook inyectable;
   tests de bounds y de variación (RED → GREEN).
2. Un breaker por dependencia (`mongo`, `minio`, `redis`): `ReadyToTrip` por
   `ConsecutiveFailures > 5`, `Timeout` 30s → HalfOpen; transiciones logueadas (slog).
3. Errores de negocio y `context.Canceled` no cuentan como fallo del breaker (test).
4. Con el breaker abierto las llamadas se rechazan SIN tocar el adaptador (contador en el
   fake) y el relay aborta sin retry storm (test de composición).
5. `gofmt`/`go vet` limpios, suite hermética en verde, tests de `relay` + `resilience` en verde.

## Ruta y entrega
- Ruta por tarea: **delegated direct** (writer por tarea, secuencial — un solo writer
  activo; el parent hace los work-unit commits). Trigger: implementación multi-archivo no
  trivial (relay.go + relay_test.go, paquete nuevo, wiring en main.go, go.mod).
- Forecast: ~800–950 líneas autografiadas. **Estrategia: `single-pr`** — la política del
  repo (AGENTS.md) es una PR por tarea con plantilla; las unidades de review son los
  work-unit commits (el candidato de review nativa es el commit, no la rama). El exceso
  sobre ~400 es boilerplate mecánico de decoradores de puertos, no complejidad de diseño.
  Slices: PR única con 3 commits (T1 | T2 | T3); los commits ya están partibles si hiciera
  falta encadenar.

## Verificación (por tarea)
- `test -z "$(gofmt -l .)"` · `go vet ./...` · suite hermética (filtro AGENTS.md) ·
  `go test -race -count=1 ./internal/adapters/relay/... ./internal/resilience/...`

## Progreso
| Tarea | Estado | Commit | Review assess |
|-------|--------|--------|---------------|
| T1 | hecho | `12ecbc0` | medium / under_budget (slice abierto) |
| T2 | hecho | `0839c4f` | medium / slice_budget_reached → **declined** (omitido a decisión del usuario, 2026-11-16) |
| T3 | hecho | `b614ef7` | medium / slice_budget_reached → **declined** (omitido a decisión del usuario, 2026-11-16) |

Suite hermética en verde tras los 3 commits (`go test -race` sobre el filtro de CI). PR #79 abierto a `main`: https://github.com/Alejo-Basile/doc-service/pull/79 (4 commits: `12ecbc0`, `0839c4f`, `b614ef7`, `da16d89`).

## Próximo paso
T1 — jitter en el backoff del relay (RED → GREEN).

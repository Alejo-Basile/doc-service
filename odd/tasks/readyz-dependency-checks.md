# Feature: /readyz con verificación real de dependencias

## Objetivo
Que `GET /readyz` compruebe conectividad real a MongoDB, MinIO y Redis y responda 503 si alguna dependencia crítica falla, cumpliendo SPEC §11.7 y el DoD global §3.4.5 del plan (`docs/pasos-iniciales.md`).

## Problema
Hoy `/readyz` es un stub de liveness (`internal/transport/http/health.go:27-34`, TODO en código): reporta OK aunque Mongo/MinIO/Redis estén caídos. Un pod no apto para servir tráfico queda "ready".

## Por qué
- SPEC §11.7: health checks que verifiquen conectividad real, no solo que el proceso esté vivo.
- DoD global §3.4.5 del plan; hito S2: "/readyz que sí comprueba Mongo y MinIO".
- Sin esto, Traefik/K8s/HEALTHCHECK no pueden sacar de rotación una réplica degradada.

## Scope
- `GET /readyz`: checks reales (Ping de Mongo, Ping de Redis, verificación liviana de MinIO) en paralelo, con timeout corto, cuerpo JSON con estado por dependencia, 200 si todas OK, 503 si alguna falla.
- `GET /healthz` queda EXACTAMENTE como está (liveness; el HEALTHCHECK del Dockerfile depende de su semántica).
- Wiring aditivo en `cmd/api/main.go` y `internal/transport/http/server.go`. Sin refactor de lo que funciona.

## Fuera de scope
- Circuit breaker, jitter, reconciliador, DLQ (otras tareas).
- Cambiar rutas públicas/internas ni el middleware de auth.
- Métricas nuevas de salud.

## Restricciones (del equipo)
- No refactorizar lo que ya funciona; cambios aditivos.
- Toda transición/invariante existente se preserva; tests herméticos deben seguir en verde.
- Artefactos técnicos en inglés (código, tests, mensajes); commits en español.

## Tareas
- [x] T1 — Puerto `HealthProbe` + implementaciones por adaptador (mongo/redis/minio)
- [x] T2 — Handler `/readyz`: checks en paralelo, timeout, 200/503 + cuerpo JSON por dependencia
- [x] T3 — Wiring aditivo en `cmd/api/main.go` y `internal/transport/http/server.go`
- [x] T4 — Tests herméticos (fakes): RED → GREEN; suite hermética + `gofmt`/`go vet` en verde

## Criterios de aceptación
1. `/healthz` sin cambios de comportamiento.
2. `/readyz` con todas las dependencias OK → 200 y body con las tres en `ok`.
3. Una dependencia con error → 503, body nombrando cuál falla (sin secretos ni detalles internos crudos).
4. Checks en paralelo con deadline corto (~2 s); una dependencia lenta no cuelga el endpoint.
5. Tests herméticos cubren los tres casos (all-ok, una caída, timeout) sin Docker/red.
6. `go build ./...`, `go vet ./...`, `gofmt -l .` limpio y suite hermética en verde.

## Verificación
```bash
PKGS=$(go list ./... | grep -vE 'internal/adapters/(mongo|changestream|relay|redis)$')
go test -race -count=1 $PKGS
go vet ./... && test -z "$(gofmt -l .)"
```
Suite de integración: dejada al job de CI (requiere Mongo replica set/Redis/MinIO).

## Ruta y estrategia de entrega
- Ruta: **delegada** (writer trigger: 2+ archivos no triviales — handler, puerto, adaptadores, wiring, tests).
- Entrega: `ask-on-risk`; forecast ≈ 150-250 líneas (< 400) → un solo PR/rama `feat/readyz-dependency-checks`, un work-unit commit (código + tests juntos).

## Progreso
- 2026-11-16: feature creada; exploración previa completada (3 explore subagents).

## Verificación registrada
- 2026-11-16 (T4): tests herméticos `/readyz` en `internal/transport/http/health_test.go`
  con fakes: all-ok → 200, probe fallido → 503 nombrando la dependencia (sin DSN/credenciales),
  probe lento (ignora el contexto) → 503 a los ~2 s. RED observado con el handler stub
  (200 en vez de 503 y `checks` vacío); GREEN tras implementar el handler.
- `go vet ./... && test -z "$(gofmt -l .)"`: OK (sin salida).
- `go build ./...`: OK (sin salida).
- `PKGS=$(go list ./... | grep -vE 'internal/adapters/(mongo|changestream|relay|redis)$') && go test -race -count=1 $PKGS`: OK.
  Integración de los adaptadores mongo/redis y MinIO real: delegada al job de CI.

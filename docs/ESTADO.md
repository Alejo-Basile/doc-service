# Contexto del proyecto: doc-service (migración de pdf-extractext a microservicios)

> Actualizado: 2026-11-17. Pegalo entero al inicio de una conversación con otra IA, o guardalo
> como `docs/ESTADO.md` en el repo. Si algo de acá contradice el código, manda el código.

## 1. Quién soy y qué hago
Soy **P2 (Backend Go)** en un equipo de 4 personas. Soy dueño del `doc-service`.
Repo: https://github.com/Alejo-Basile/doc-service (público, Go + Gin).
Quiero que me ayudes a continuar el trabajo sin salirte del plan: **
no refactorices lo que ya funciona, usa el orquestador y deriva tareas a subagentes, y cuidá los tokens.**

## 2. El proyecto
Se migra el monolito **pdf-extractext** (Python, síncrono: subís un PDF y recibís el `.txt`) a
microservicios con el patrón **Strangler Fig + SAGA**. Traefik reparte tráfico entre monolito y servicios
nuevos (canary), y al final se apaga el monolito. Los clientes viejos usan una fachada síncrona.

Documentos de referencia:
- Plan de ejecución: `pasos-iniciales.md` (v1.1, 11 semanas, tareas `S<semana>-P<persona>-<n>`).
- Spec global (fuente de verdad): repo `infra`, `docs/SPEC.md` v2.1 y `docs/adr/` (ADRs globales).
  El doc-service tiene copias de sus ADRs locales en `docs/adr/`.

### Equipo y stack
| Persona | Rol | Dueño de |
|---|---|---|
| P1 | Platform/DevOps | Traefik v3, MinIO, Redis (AOF, Sentinel), MongoDB Replica Set, Prometheus/Grafana/Loki, CI/CD |
| **P2 (yo)** | Backend Go | `doc-service` |
| P3 | Systems Rust | `extraction-worker` (Tokio, Redis Streams, Retry + Circuit Breaker, DLQ) |
| P4 | Integración/Calidad | `rate-limiter` (Go, ForwardAuth, GCRA), ruteo Strangler, k6, OpenAPI, canary/cutover |

Estilo: trunk-based, arquitectura orientada a eventos, SAGA orquestada dentro del doc-service.

## 3. Cómo funciona el flujo
1. `POST /api/v2/documents` (idempotente por `Idempotency-Key`) → doc-service inserta el documento en
   Mongo (`PENDING_UPLOAD`) y devuelve una **URL prefirmada POST con política** de MinIO
   (`content-length-range` = `MAX_PDF_BYTES`, `Content-Type`, key `raw-pdfs/<document_id>.pdf`).
2. El cliente sube el PDF **directo a MinIO** (host público S3). El binario nunca pasa por la API ni la cola.
3. MinIO dispara el webhook `POST /internal/storage/events` (solo red de datos, token en tiempo constante,
   solo bucket `raw-pdfs`). El handler verifica `%PDF-` con un GET por rango. Si no es PDF → `REJECTED`
   (terminal, sin reintentos). Si lo es → `UPLOADED`.
4. El **relay** escucha el Change Stream de Mongo (`UPLOADED`), deduplica con `SET NX EX` en Redis,
   transiciona a `QUEUED` y hace `XADD` a `stream:pdf-processing` + `WAIT 1`. Guarda el *resume token* en Mongo.
5. El **worker (Rust)** consume del consumer group `extraction-workers`, extrae, sube el `.txt` a
   `extracted-txt` y escribe `PROCESSING → COMPLETED` directo en Mongo (rol propio, solo `update`).
   Si falla: `RETRYING`, y tras `WORKER_MAX_ATTEMPTS` → DLQ → `COMPENSATING → FAILED`.
6. `GET /api/v2/documents/{id}` consulta estado. `GET .../download` da URL prefirmada de lectura solo si
   está `COMPLETED` (409 si no).
7. El **reconciliador** (cron cada 10 min, lease en Redis para varias réplicas) es la red de seguridad.

### Estados
`PENDING_UPLOAD → UPLOADED → QUEUED → PROCESSING → COMPLETED`; desvíos `RETRYING`,
`EXTRACTION_FAILED → COMPENSATING → FAILED`. Terminales: `COMPLETED`, `FAILED`, `REJECTED`, `UPLOAD_EXPIRED`.

### Invariantes que no se negocian
- Toda transición es `updateOne` con filtro `{_id, status: esperado}`. Nunca `upsert`, nunca `$inc` derivado de un mensaje.
  Hay dos escritores (doc-service y worker), así que ninguna escritura puede ser ciega.
- Toda transición deja entrada en `status_history` (estado, fecha, actor, `correlation_id`).
- Una transición no declarada en la tabla del dominio se rechaza.
- `UPLOAD_EXPIRED` lo decide solo el reconciliador tras un `HEAD` del objeto; si el objeto existe se promueve a
  `UPLOADED`. El reconciliador **nunca borra registros** (lo hace el TTL de Mongo sobre estados ya terminales).
- Mensaje de cola: sin `attempts` (lo da el delivery count de Redis), `object_key = raw-pdfs/<document_id>.pdf`,
  `schema_version` obligatorio, `deny_unknown_fields` en ambos lados.
- Sistema *at-least-once*: se asume que los mensajes se repiten (idempotencia en API, webhook y relay).

## 4. Estructura del servicio
`cmd/api/main.go` · `internal/domain` (entidades, transiciones) · `internal/ports` · `internal/adapters`
(mongo, minio, redis, changestream, relay) · `internal/api` · `internal/webhook` · `internal/reconciler` ·
`internal/config` · `internal/transport/http` · `docs/design/mongo-schema.md` · `docs/adr/`.

Variables de config propias: `MONGO_URI`, `MONGO_MAX_POOL`, `MONGO_TIMEOUT_MS`, `MINIO_ENDPOINT`,
`MINIO_PUBLIC_ENDPOINT`, `MINIO_TIMEOUT_MS`, `REDIS_URL`, `REDIS_TIMEOUT_MS`, `RECONCILE_INTERVAL_MIN`,
`DOC_UPLOAD_GRACE_MIN`, `STALE_DOCUMENT_MIN`, `MAX_PDF_BYTES`. Sin secretos reales en el repo (solo `.env.example`).

## 5. Reglas del repo
- `main` protegido: PR obligatorio, CI verde, 1 aprobación del code owner (`@ivann-02`). Nunca push a `main`.
- Yo soy el autor, así que mis PRs los tiene que aprobar otro reviewer.
- Plantilla de PR: qué cambia · por qué · cómo se prueba · cómo se revierte.
- Commits chicos (conventional commits). gitleaks en pre-commit y CI. Una tarea por vez con su test y su DoD.
- Tareas P0 de consistencia/idempotencia/seguridad requieren revisión de dos personas.
- **Otro actor también empuja a `main`**: antes de empezar, `git fetch` y rebasar/actualizar.

## 6. Avance (auditado el 2026-11-17)
**Backlog de P2 completo**, de S0-P2-01 a S6-P2-02, con estas excepciones:
- **S1-P2-01 — HECHA hoy (PR #74):** config explícita del pool en `internal/adapters/mongo/client.go`; la prueba
  de carga faltante quedó en `internal/adapters/mongo/pool_load_test.go`
  (`TestClient_PoolChico_Carga20Concurrente_SinTimeouts`: 20 goroutines × 5 ops × 3 operaciones con
  `MaxPoolSize=2`/`MinPoolSize=2`, cero timeouts — 525 ms en local). Se levantó un Replica Set de un nodo con
  Docker (`mongo:7 --replSet rs0` + `rs.initiate`, igual que el job `integration` de CI): NO hizo falta esperar a P1.
- **S0-P2-06 — no verificable desde este repo:** los ADRs globales viven en `infra`.
- **S5-P2-01 — HECHA:** mergeada en `main` como `2a4a278` (PR #73).

### Cambios de esta sesión (2026-11-17, tarde) — cambio txt→md + review pausada
- **Review nativa del candidato `feat/reconciliador-p2`+`feat/cambio-md` PAUSADA a decisión del
  usuario**: 3 de 4 lenses capturados y admitidos (risk, readability, reliability); el lens
  `review-resilience` devolvió `opencode_task_output_empty` de forma consistente (5 relanzamientos).
  **No se reintentará**; el review queda incompleto — NO aprobado, NO acknowledged. Linaje:
  `review-4b987245972d827a`. El fallo es del transporte/host Task del runtime (el agente
  `review-resilience` está configurado igual que sus pares); no es defecto de Gentle AI.
- **Fase 1 de `cambiomd.md`** (commit `35b9fa9`, ya en la rama): rename txt→md de punta a punta en Go
  (`md_ref`/`extracted-md`, `StatMD`/`DeleteMD`/`PresignGetMD`/`ListObjectsMD`, `MinIOBucketMD`).
- **Fase 2 de `cambiomd.md` (docs) COMPLETA** — commit `c268e2d`: `docs/design/mongo-schema.md`,
  `docs/adr/ADR-0006-worker-completed-directo.md`, `docs/SPEC.md` (copia local, con nota de
  propagación pendiente a `infra/docs/SPEC.md` — NO modificar el repo `infra` desde acá),
  `docs/pasos-iniciales.md` (solo referencias de contrato: S1-P1-04/06, S3-P2-09, tablas §11 y
  cheatsheet §18). T2.5 sin cambios (README/AGENTS sin menciones). Coherencia T2.6 OK: solo quedan
  referencias en filas históricas del plan y en este log. Verificación: `gofmt` OK, `go vet` OK,
  suite hermética 12/12 ok.
- **D5 decidida por el usuario (2026-11-17)**: compatibilidad de lectura con **read-fallback**
  (`md_ref` primero; si falta, `txt_ref` legacy con su bucket correspondiente). Escrituras nuevas
  solo `md_ref`; objetos TXT históricos intactos. Ver `docs/design/legacy-txt-fallback.md`.
- **Fase 3 de `cambiomd.md` COMPLETA** (commit `d04a8bb`): read-fallback implementado —
  `ExtractedRef()` (md_ref primero, txt_ref legacy si falta), `StatLegacyTXT`/`PresignGetLegacyTXT`
  solo lectura (el puerto no expone delete/list del bucket legacy: los objetos TXT históricos
  jamás se tocan), fallback en `DownloadDocument` y verificaciones COMPLETED del reconciliador,
  `MINIO_LEGACY_BUCKET_TXT` (default `extracted-txt`). TDD RED→GREEN observado. Runbook de retiro:
  `docs/design/legacy-txt-fallback.md`.
- **Fase 4** (commit `58c15fc`): contrato propuesto en `docs/design/contrato-extraction-worker.md`.
  T4.3/T4.4 verificados en código (sin cambios). T4.5: se conserva `TXT_REF_MISSING` (compartido
  con el worker; renombrar es breaking). T4.1 pendiente P3 (worker) y T4.2 pendiente P1 (bucket
  `extracted-md` en infra) — sin bloqueo para doc-service. **No se asume que el worker ya genera
  Markdown.**
- **Fase 5**: integración 6/6 ok en local (Docker: Mongo réplica-set + Redis + MinIO); hermética
  12/12 ok. T5.2 (E2E) pendiente: `tests/e2e/` no existe.
  **Próxima tarea**: coordinar T4.1/T4.2 con P3/P1, o prioridades 3/4 de `tareas-revision.md`.

### Cambios de esta sesión (2026-11-17) — Reconciliador P2 (SPEC §6.5)
- **Prioridad 2 de `tareas-revision.md` completa** en la rama `feat/reconciliador-p2` (base `26199f8` = `main`
  tras el PR #79). 3 work-unit commits + docs:
  - `22ba4d0` **modo dry-run** del reconciliador: `RECONCILE_DRY_RUN` + override `?dry_run=1` en
    `POST /internal/reconcile`; toda mutación pasa por wrappers con reporte planificado vs ejecutado.
  - `4da618b` **huérfanos de `extracted-txt`**: segundo pass en `sweepOrphans` + `DeleteTXT`
    (puerto/adaptador MinIO/decorador resiliente); jamás borra un txt referenciado por `TxtRef`.
  - `afd5c19` **recuperación de estados trabados**: reencolado espejando el retry handler
    (`PROCESSING→RETRYING` + XADD) y vía de fallo de la SAGA hasta `FAILED`
    (`OBJECT_MISSING`/`COMPENSATION_FAILED`), sin tocar la matriz de transiciones.
- **Reviews nativas**: consentimiento no recolectable en este runtime (falta el tool `question`,
  3ª ocurrencia — memoria `config/rdd-consent-question-ui`); candidatos registrados como omitidos
  por decisión del usuario. Verificación por tiers cumplida: writer self-verification + spot checks
  + verificador independiente (T2, tier high → verdict clean).
- **PR**: [#80](https://github.com/Alejo-Basile/doc-service/pull/80) abierto a `main`, CI en verde
  (lint, gitleaks, unit, integration, image-scan). Sin bloqueos; espera aprobación de reviewer
  (el autor no puede auto-aprobar).
  **Próxima tarea**: prioridades 3/4 de `tareas-revision.md` (findings advisory `/readyz`, seguridad/higiene).

### Cambios de esta sesión (2026-11-16, tarde) — Resiliencia P1 (SPEC §7)
- **Prioridad 1 de `tareas-revision.md` completa** en la rama `feat/resiliencia-p1-jitter-breaker`
  (base `0673410` = `main` tras el PR #78). 3 work-unit commits:
  - `12ecbc0` **equal jitter** en el backoff del relay (`internal/adapters/relay/relay.go`): helper puro
    `jittered` (duerme `d/2 + rand[0, d/2]`, bounds `[d/2, d]`) + hook de random inyectable para tests
    deterministas. El backoff nominal sigue duplicándose y el resto del loop no cambia (SPEC §7.1).
  - `0839c4f` **circuit breaker por dependencia** (`internal/resilience/`, `sony/gobreaker/v2`): un breaker
    por dependencia (`mongo`/`minio`/`redis`, `ConsecutiveFailures > 5`, `Timeout` 30s → HalfOpen, cambios de
    estado logueados con `slog`) + decoradores de `ports.DocumentRepository`/`ObjectStorage`/`WorkQueue`
    cableados en `cmd/api/main.go`. Errores de negocio (`ErrNotFound`/`ErrDuplicate`/`ErrInvalidCursor`) y
    `context.Canceled` excluidos del conteo vía `IsExcluded` de gobreaker v2.4.0 (no resetea
    `ConsecutiveFailures`); `context.DeadlineExceeded` cuenta como fallo. Los health probes de `/readyz`
    usan los adaptadores crudos: diagnóstico honesto, nunca detrás de un breaker (SPEC §7.2).
  - `b614ef7` **composición retry→breaker** (SPEC §7.3): `enqueueWithRetry` aborta sin más intentos ni
    sleeps ante un rechazo del breaker (`resilience.IsRejected`). Tests de composición con las 3 piezas
    reales (fake inner → decorador → relay) que prueban el orden: el breaker envuelve la llamada
    individual (el inner solo recibe los intentos hasta abrir) y el retry no arma retry storm (aborta
    en el primer rechazo). RED observado: 11 intentos vs 3.
- **Suite hermética en verde** tras los 3 commits (`go test -race` sobre el filtro de CI, 10 paquetes `ok`),
  `gofmt`/`go vet`/`go build` limpios. Tests E2E de relay corrieron contra Mongo/Redis locales.
- **Review nativa:** el candidato (slice T1+T2+T3, riesgo `medium`, `slice_budget_reached` a las ~400
  líneas de presupuesto) fue **omitido a decisión explícita del usuario** (`declined_this_candidate`,
  sin reporte de defecto). Sin registro de revisión; la entrega sigue bajo política ordinaria (PR + CI +
  aprobación de reviewer ≠ autor). Mismo patrón de bloqueo de siempre: la UI `question` que exige
  `consent/v3` no existe en este runtime de OpenCode (ver memoria de sesiones previas).
- **PR abierto** a `main` con los 3 commits + docs de feature. `tareas-revision.md` quedó con la
  Prioridad 1 tildada; siguen pendientes las Prioridades 2-4 y los ítems bloqueados por infraestructura.

### Cambios de esta sesión (2026-11-16)
- **Repo sincronizado** con `origin/main` (fast-forward a `722bae2`). El PR #75 ya está mergeado como
  `944346f` (diff vacío contra el commit local `b476e5d`): la rama `fix/relay-transicion-object-key` sobra.
- **`/readyz` con verificación real de dependencias** (SPEC §11.7, DoD §3.4.5): probes paralelos de
  Mongo/Redis/MinIO con deadline de 2 s, 200/503 con estado por dependencia y errores sanitizados.
  Rama `feat/readyz-dependency-checks`, commits `3b2f5b4` (código+tests) y `c8b8169` (docs de feature).
  Verificación independiente 7/7 PASS. Review nativa (1 lens `review-reliability`) **APROBADA**
  (autoridad quemada); 4 findings advisory no bloqueantes (R3-01..R3-04) en `odd/tasks/readyz-dependency-checks.md`.
- **PR #78 abierto** (https://github.com/Alejo-Basile/doc-service/pull/78): push + PR autorizados por el
  usuario. Requiere reviewer ≠ `ivann-02` (autor) y CI verde para mergear. Checklist de pendientes en `tareas-revision.md`.

### Cambios de esta sesión (2026-11-13, tarde) — handoff del owner
- **`doc-service-owner-changes.md` (handoff del owner):** el cambio pedido ya estaba implementado y mergeado
  en `main` vía **PR #72** (`4efa66a`, transición UPLOADED→QUEUED antes del XADD). El checklist de review del
  handoff pasa íntegro contra el código actual.
- **Delta literal cerrado + ajuste pedido:** la transición ahora persiste el object key en Mongo y, por pedido
  del usuario, la clave se deriva **una sola vez** (`raw-pdfs/<document_id>.pdf`, contrato §11.3) antes de
  persistir — documento y mensaje a la cola siempre coinciden (antes un evento sin `object_key` persistía `""`
  y encolaba la derivada). Tests: `TestRelay_ObjectKeyDerivadoPersisteYCoincideConMensaje` (RED→GREEN observado)
  y `TestRelay_NoEncolaSiDocumentoNoPublicable` (PROCESSING/COMPLETED no encolan).
- **PR #75 abierto** (https://github.com/Alejo-Basile/doc-service/pull/75): commit `b476e5d` en
  `fix/relay-transicion-object-key` (+94/−14, `relay.go` + `relay_test.go`). `go build`/`go vet`/tests/gofmt OK.
  E2E (`tests/e2e/chain/run.sh --runs 10`) NO corrió por falta de infraestructura — queda pendiente (aclarado en el PR).
- **RDD assess del commit:** riesgo `medium` (executable_change), `review_due: false` (`under_budget`) — el slice
  queda pendiente hasta el presupuesto de entrega (~400 líneas). La revisión nativa del candidato anterior quedó
  sin efecto al congelarse el trabajo en commit.

### Cambios de la última sesión
- **PR #74 abierto** (https://github.com/Alejo-Basile/doc-service/pull/74): agrega
  `TestClient_PoolChico_Carga20Concurrente_SinTimeouts` en `internal/adapters/mongo/pool_load_test.go`
  (+126 líneas, solo test). Cierra el DoD de `S1-P2-01`. Verificado local contra `mongo:7` réplica-set `rs0`
  con `-race`: PASS en 0.64s, `gofmt`/`go vet` limpios.
- **PR #73 mergeado** (`2a4a278`): `TestDocumentRepository_WorkerCompleted_Condicional` cierra `S5-P2-01`.
- **Cuenta verificada:** commits y PRs salen de `ivann-02` (git author `ivann-02 <45361311@alumnos.frsr.utn.edu.ar>`,
  GitHub lo atribuye a `@ivann-02`; `gh` con cuenta activa `ivann-02` y protocolo https). OJO: hay otra cuenta
  logueada (`ivanmalaniuk`, inactiva) — no hacer `gh auth switch`. Un PR de `@ivann-02` necesita reviewer ≠ autor.
- Otro actor empujó 5 commits a `main` durante la sesión: Trivy HIGH, presign con Region, `expires_at`,
  tests que se saltean sin infra. `main` quedó lineal y sano.

### Desviaciones menores conocidas (decididas, no tocar)
- El presign POST usa el cliente interno de MinIO y la URL se reescribe a `MINIO_PUBLIC_ENDPOINT`
  (hay test, #49). La firma de una política POST no depende del host, así que es válido.
- El webhook pide `Range: bytes=0-5` en vez de `bytes=0-4` del contrato; `isPDF` valida exactamente
  los primeros 5 bytes. Sin impacto funcional.

### Deuda / pendientes menores
- `TestRepository_UpdateStatus_TransicionInvalida` es un no-op sin aserciones (sin tocar).
- Los tests de integración fallan en local sin Redis/Mongo (ambiental, no regresión).
- No verificado: si existe `openapi/documents.yaml` generado desde el código (figura en el calendario de la semana 4).

## 7. Punto actual y próximos pasos
0. **Review nativa PAUSADA (decisión del usuario)**: linaje `review-4b987245972d827a`, 3/4 lenses
   capturados; `review-resilience` falló de forma consistente con `opencode_task_output_empty`.
   No reintentar. NO tratar como aprobado/acknowledged. Continuar implementación sin que el
   problema de transporte del reviewer la bloquee.
1. **PR de Resiliencia P1** (`feat/resiliencia-p1-jitter-breaker`): esperar CI verde y aprobación de
   un reviewer ≠ `ivann-02`, y mergear. Cierra la Prioridad 1 de `tareas-revision.md` (SPEC §7.1-§7.3).
   Review nativa omitida por decisión del usuario (no bloquea la entrega).
2. **PR #78** (https://github.com/Alejo-Basile/doc-service/pull/78): mergear cuando haya CI verde y
   aprobación. Cierra el gap `/readyz` (SPEC §11.7).
3. **Siguiente tarea natural: Prioridad 2 de `tareas-revision.md`** — reconciliador (SPEC §6.5): modo
   dry-run, huérfanos de `extracted-txt`, estados intermedios trabados. Ver `tareas-revision.md`.
4. **PRs #74 y #75 ya mergeados** (`81eaea2`, `944346f`). Borrar la rama local `fix/relay-transicion-object-key`.
   **E2E pendiente** (`tests/e2e/chain/run.sh --runs 10`) cuando haya infraestructura (Mongo réplica-set + Redis + MinIO + worker).
5. **Backlog de P2 completo** al mergear el PR #74 (solo `S0-P2-06` es no verificable desde este repo).
   Si P1/P3/P4 piden algo del doc-service para las fases 4-5 (canary, cutover, reconciliador en producción),
   tratarlo como tarea nueva con su criterio de aceptación.
6. Si una tarea depende de infraestructura de P1 que no está, marcarla BLOQUEADA y pasar a la siguiente.
   No inventar secretos ni tocar el repo `infra`. OJO: para tests de integración ya no hace falta esperar a P1:
   un nodo local con Docker (`mongo:7 --replSet rs0` + `rs.initiate`) alcanza, como hace CI.

## 8. Cómo quiero que trabajes
- Verificá antes de continuar (estado de git, `go build ./... && go vet ./... && go test ./...`) y reportá solo lo que falle.
- Leé solo lo necesario del repo. Del plan, solo las secciones de P2 (5.3, 6.4, 7.4, 8.4), 11 (contratos) y 17 "Go (P2)".
- Al terminar, resumí en 4 puntos: qué verificaste, qué problemas encontraste, qué implementaste, cuál es la próxima tarea.

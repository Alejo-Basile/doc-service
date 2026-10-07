# doc-service

Document Management Service — servicio en **Go (Gin)** del proyecto de migración a microservicios.

> Especificación arquitectónica (fuente única de verdad): [`infra/docs/SPEC.md`](https://github.com/Alejo-Basile/infra/blob/main/docs/SPEC.md) v2.1

## Qué hace este repo

- CRUD de metadatos de documentos y ciclo de vida completo (máquina de estados, SPEC §4).
- Emisión de **URLs prefirmadas `POST` con política** de MinIO (límite de tamaño en el servidor de objetos, SPEC §5.1).
- Endpoint interno del **webhook de MinIO** `ObjectCreated` (con validación `%PDF-` por rango, SPEC §5.2, §9).
- **Relay de MongoDB Change Streams → Redis Streams** (SPEC §5.3) con `WAIT 1` tras el `XADD`.
- **Orquestador de la SAGA** y job de reconciliación (`HEAD` de objetos, reencolado, limpieza de huérfanos, SPEC §6).

## Qué NO hace este repo

- **No extrae PDFs**: eso es `extraction-worker/` (Rust). Cero llamadas síncronas hacia el worker (SPEC §1.3).
- **No hace ruteo ni TLS**: eso es Traefik (configuración en `infra/`).
- **No hace rate limiting**: eso es el servicio `rate-limiter` en `platform-services/` (ForwardAuth, SPEC §8).
- **No contiene secretos reales**: solo `.env.example` con valores ficticios (repo público).

## Stack y estructura

- Go (versión fijada en `go.mod`), framework Gin, Mongo Go Driver, cliente Redis Sentinel-aware (SPEC §3.5).
- `docs/adr/`: ADRs **locales** del servicio. Las ADRs **globales** viven en `infra/docs/adr/`.

## Escaneo de secretos (gitleaks, S0-P1-04)

- **Local (primera capa):** instalar el hook de pre-commit **una vez** en este clone
  (gitleaks no tiene comando `install`; el hook es un `.git/hooks/pre-commit` que
  ejecuta `gitleaks git --staged`):

  ```bash
  printf '#!/usr/bin/env bash\nexec gitleaks git --staged --verbose\n' \
    > .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit
  ```
- **CI (segunda capa):** el job `gitleaks` (`.github/workflows/gitleaks.yml`)
  escanea el diff de cada PR y cada push a `main`.

## Gobernanza

- Rama `main` protegida por ruleset: push directo y force push denegados, PR obligatorio con CI en verde y al menos 1 aprobación del code owner (`@ivann-02`).
- Plantilla de PR obligatoria: *qué cambia · por qué · cómo se prueba · cómo se revierte*.

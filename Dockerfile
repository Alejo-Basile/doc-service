# syntax=docker/dockerfile:1
# doc-service (Go 1.24) — imagen de produccion multi-stage, no-root.
# S1 / CI-C: la API v2 escucha en :8080 dentro de la red `infra_internal`;
# los endpoints /internal/* NO se publican al host (solo Traefik enruta /api/v2).

# --- Build -------------------------------------------------------------------
FROM golang:1.24-alpine AS build
# No descargar toolchains remotas: usar la del builder (>= toolchain de go.mod).
ENV GOTOOLCHAIN=local
WORKDIR /src
# Cache de dependencias por capas.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w" \
      -o /out/api ./cmd/api

# --- Runtime -----------------------------------------------------------------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
 && adduser -D -u 10001 appuser
COPY --from=build /out/api /usr/local/bin/api
USER 10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=6 --start-period=20s \
  CMD wget -qO- http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/api"]

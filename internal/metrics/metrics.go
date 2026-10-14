// Package metrics centraliza las métricas Prometheus del doc-service
// (SPEC §10: observabilidad). No se exponen labels con document_id
// (cardinalidad y trazabilidad: los IDs viajan en los logs JSON, no en métricas).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// DocumentsCreated totaliza las altas de documento vía POST /api/v2/documents
	// cuando la creación es efectiva (no idempotente ni fallida).
	DocumentsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "docservice",
		Subsystem: "api",
		Name:      "documents_created_total",
		Help:      "Total de documentos creados vía la API v2 del doc-service.",
	})

	// WebhookEvents totaliza los eventos ObjectCreated de MinIO que el
	// doc-service acepta y procesa (transición PENDING_UPLOAD -> UPLOADED).
	WebhookEvents = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "docservice",
		Subsystem: "webhook",
		Name:      "events_total",
		Help:      "Total de eventos de webhook de MinIO procesados.",
	})
)

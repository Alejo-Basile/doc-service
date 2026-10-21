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

	// RelayEnqueueErrors totaliza los fallos de encolado del relay tras agotar
	// los reintentos (S4-P2-05). Un valor creciente señala documentos UPLOADED
	// que quedaron sin encolar y que el reconciliador deberá recuperar: es la
	// señal para alertar "UPLOADED sin encolar".
	RelayEnqueueErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "docservice",
		Subsystem: "relay",
		Name:      "enqueue_errors_total",
		Help:      "Total de eventos UPLOADED que el relay no pudo encolar tras agotar reintentos.",
	})

	// RelayEnqueued totaliza los eventos encolados con éxito por el relay.
	RelayEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "docservice",
		Subsystem: "relay",
		Name:      "enqueued_total",
		Help:      "Total de eventos UPLOADED encolados exitosamente por el relay.",
	})
)

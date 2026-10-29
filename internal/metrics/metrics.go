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

	// Nota: las tres métricas de negocio se declaran SIN Subsystem a propósito.
	// El nombre expuesto debe ser exactamente docservice_documents_*, que es el
	// que consultan dashboard-v1-s4.json y alert.rules.yml (plan-S4-P1.md §4.2).
	// Un Subsystem extra las publicaría como docservice_business_* y los
	// paneles/alertas quedarían en NoData.

	// DocumentsByStatus expone el conteo de documentos por estado.
	// Label: status (ej: PENDING_UPLOAD, UPLOADED, QUEUED, PROCESSING,
	// RETRYING, EXTRACTION_FAILED, COMPENSATING, COMPLETED, FAILED, REJECTED, UPLOAD_EXPIRED).
	// Tipo: Gauge (no Counter) porque el valor sube y baja con transiciones.
	DocumentsByStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "docservice",
		Name:      "documents_by_status",
		Help:      "Conteo actual de documentos por estado (incluye PENDING_UPLOAD).",
	}, []string{"status"})

	// DocumentsNonTerminal expone el total de documentos en estados intermedios
	// operativos (excluye PENDING_UPLOAD porque su duración depende de la
	// ventana de subida del cliente y se gobierna con expires_at/reconciliador).
	// Estados incluidos: UPLOADED, QUEUED, PROCESSING, RETRYING, EXTRACTION_FAILED, COMPENSATING.
	// Tipo: Gauge.
	DocumentsNonTerminal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "docservice",
		Name:      "documents_non_terminal",
		Help:      "Total de documentos en estados intermedios operativos (excluye PENDING_UPLOAD).",
	})

	// DocumentsOldestIntermediateAgeSeconds expone la antigüedad en segundos
	// del documento más viejo por cada estado intermedio.
	// Label: status (solo estados intermedios operativos).
	// Tipo: Gauge. Valor 0 si no hay documentos en ese estado.
	// Se usa para alertar documentos atascados (> 900s = 15 min).
	DocumentsOldestIntermediateAgeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "docservice",
		Name:      "documents_oldest_intermediate_age_seconds",
		Help:      "Antigüedad en segundos del documento más viejo por estado intermedio operativo (excluye PENDING_UPLOAD).",
	}, []string{"status"})
)

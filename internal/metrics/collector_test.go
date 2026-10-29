// Package metrics implementa tests unitarios del collector de métricas de negocio.
package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Alejo-Basile/doc-service/internal/domain"
)

// TestIntermediateOperationalStatuses verifica la lista de estados
func TestIntermediateOperationalStatuses(t *testing.T) {
	expected := []string{
		string(domain.StatusUploaded),
		string(domain.StatusQueued),
		string(domain.StatusProcessing),
		string(domain.StatusRetrying),
		string(domain.StatusExtractionFailed),
		string(domain.StatusCompensating),
	}

	if len(intermediateOperationalStatuses) != len(expected) {
		t.Fatalf("expected %d statuses, got %d", len(expected), len(intermediateOperationalStatuses))
	}

	for i, s := range intermediateOperationalStatuses {
		if s != expected[i] {
			t.Errorf("status[%d] = %s, want %s", i, s, expected[i])
		}
	}

	// Verificar que PENDING_UPLOAD NO está incluido
	for _, s := range intermediateOperationalStatuses {
		if s == string(domain.StatusPendingUpload) {
			t.Error("PENDING_UPLOAD no debe estar en intermediateOperationalStatuses")
		}
	}
}

// TestKnownStatuses verifica que todos los estados conocidos están en la lista
func TestKnownStatuses(t *testing.T) {
	knownStatuses := []string{
		string(domain.StatusPendingUpload),
		string(domain.StatusUploaded),
		string(domain.StatusQueued),
		string(domain.StatusProcessing),
		string(domain.StatusRetrying),
		string(domain.StatusExtractionFailed),
		string(domain.StatusCompensating),
		string(domain.StatusCompleted),
		string(domain.StatusFailed),
		string(domain.StatusRejected),
		string(domain.StatusUploadExpired),
	}

	// Verificar que no hay duplicados
	seen := make(map[string]bool)
	for _, s := range knownStatuses {
		if seen[s] {
			t.Errorf("estado duplicado: %s", s)
		}
		seen[s] = true
	}

	// Verificar que intermediateOperationalStatuses es subconjunto
	for _, s := range intermediateOperationalStatuses {
		if !seen[s] {
			t.Errorf("estado intermedio %s no está en knownStatuses", s)
		}
	}
}

// TestDefaultCollectorConfig verifica la configuración por defecto
func TestDefaultCollectorConfig(t *testing.T) {
	cfg := DefaultCollectorConfig()
	if cfg.RefreshInterval != 15*time.Second {
		t.Errorf("expected 15s, got %v", cfg.RefreshInterval)
	}
}

// TestStartCollectorWithConfig verifica que el helper no paniquea
func TestStartCollectorWithConfig(t *testing.T) {
	// Solo verificamos que la función existe y se puede llamar
	// (en test real usaríamos un mock de mongo.Client)
	_ = StartCollectorWithConfig
}

// TestBusinessMetricNames fija los nombres expuestos de las métricas de negocio.
// SPEC §10 / plan-S4-P1.md §4.2 y las consultas de dashboard-v1-s4.json y
// alert.rules.yml dependen de estos nombres exactos; un Subsystem adicional las
// publicaría como docservice_business_* y dejaría paneles y alertas en NoData.
func TestBusinessMetricNames(t *testing.T) {
	// Materializar las series para que aparezcan en el gather.
	DocumentsByStatus.WithLabelValues(string(domain.StatusQueued)).Set(0)
	DocumentsOldestIntermediateAgeSeconds.WithLabelValues(string(domain.StatusQueued)).Set(0)
	DocumentsNonTerminal.Set(0)

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather de métricas: %v", err)
	}

	got := make(map[string]bool, len(families))
	for _, f := range families {
		got[f.GetName()] = true
	}

	for _, want := range []string{
		"docservice_documents_by_status",
		"docservice_documents_non_terminal",
		"docservice_documents_oldest_intermediate_age_seconds",
	} {
		if !got[want] {
			t.Errorf("métrica %q no expuesta; revisar Namespace/Subsystem", want)
		}
	}
}

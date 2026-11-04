// Package metrics implementa tests unitarios del collector de métricas de negocio.
package metrics

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
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

// TestKnownStatuses verifica la lista canónica del dominio: sin duplicados,
// todos válidos y con los estados intermedios operativos como subconjunto.
func TestKnownStatuses(t *testing.T) {
	all := domain.AllStatuses()
	if len(all) != 11 {
		t.Fatalf("expected 11 estados, got %d", len(all))
	}

	seen := make(map[string]bool, len(all))
	for _, s := range all {
		if !s.IsValid() {
			t.Errorf("estado %q de AllStatuses no pasa IsValid", s)
		}
		if seen[string(s)] {
			t.Errorf("estado duplicado en AllStatuses: %s", s)
		}
		seen[string(s)] = true
	}

	for _, s := range intermediateOperationalStatuses {
		if !seen[s] {
			t.Errorf("estado intermedio %s no está en AllStatuses", s)
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

// TestCollectOnce_ActualizaGauges es un test de integración: necesita MongoDB.
// En el job hermético (sin MONGO_TEST_URI) se salta para no acoplar la suite
// unitaria a infraestructura. El job de integración define MONGO_TEST_URI.
func TestCollectOnce_ActualizaGauges(t *testing.T) {
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Skip("MONGO_TEST_URI no definido: test de integración")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dbName := fmt.Sprintf("docservice_metrics_%d", time.Now().UnixNano())
	client, err := mongo.NewClient(ctx, uri, dbName, mongo.DefaultPoolConfig())
	if err != nil {
		t.Fatalf("MongoDB no disponible: %v", err)
	}
	t.Cleanup(func() {
		_ = client.DB().Drop(context.Background())
		_ = client.Close(context.Background())
	})

	now := time.Now().UTC()
	docs := []any{
		bson.D{{Key: "_id", Value: "q1"}, {Key: "status", Value: string(domain.StatusQueued)}, {Key: "created_at", Value: now.Add(-2 * time.Minute)}, {Key: "updated_at", Value: now.Add(-2 * time.Minute)}},
		bson.D{{Key: "_id", Value: "q2"}, {Key: "status", Value: string(domain.StatusQueued)}, {Key: "created_at", Value: now.Add(-time.Minute)}, {Key: "updated_at", Value: now.Add(-time.Minute)}},
		bson.D{{Key: "_id", Value: "p1"}, {Key: "status", Value: string(domain.StatusProcessing)}, {Key: "created_at", Value: now.Add(-30 * time.Second)}, {Key: "updated_at", Value: now.Add(-30 * time.Second)}},
		bson.D{{Key: "_id", Value: "c1"}, {Key: "status", Value: string(domain.StatusCompleted)}, {Key: "created_at", Value: now.Add(-5 * time.Minute)}, {Key: "updated_at", Value: now.Add(-5 * time.Minute)}},
		bson.D{{Key: "_id", Value: "u1"}, {Key: "status", Value: string(domain.StatusPendingUpload)}, {Key: "created_at", Value: now}, {Key: "updated_at", Value: now}},
	}
	if _, err := client.Collection("documents").InsertMany(ctx, docs); err != nil {
		t.Fatalf("insertar documentos: %v", err)
	}

	collectOnce(ctx, client)

	cases := []struct {
		name string
		got  float64
		want float64
	}{
		{"by_status QUEUED", testutil.ToFloat64(DocumentsByStatus.WithLabelValues(string(domain.StatusQueued))), 2},
		{"by_status PROCESSING", testutil.ToFloat64(DocumentsByStatus.WithLabelValues(string(domain.StatusProcessing))), 1},
		{"by_status COMPLETED", testutil.ToFloat64(DocumentsByStatus.WithLabelValues(string(domain.StatusCompleted))), 1},
		{"by_status PENDING_UPLOAD", testutil.ToFloat64(DocumentsByStatus.WithLabelValues(string(domain.StatusPendingUpload))), 1},
		{"by_status FAILED reseteado a 0", testutil.ToFloat64(DocumentsByStatus.WithLabelValues(string(domain.StatusFailed))), 0},
		{"non_terminal excluye PENDING_UPLOAD y COMPLETED", testutil.ToFloat64(DocumentsNonTerminal), 3},
	}
	for _, c := range cases {
		if math.Abs(c.got-c.want) > 0.001 {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	if got := testutil.ToFloat64(DocumentsOldestIntermediateAgeSeconds.WithLabelValues(string(domain.StatusQueued))); math.Abs(got-120) > 5 {
		t.Errorf("age QUEUED = %v, want ~120", got)
	}
	if got := testutil.ToFloat64(DocumentsOldestIntermediateAgeSeconds.WithLabelValues(string(domain.StatusProcessing))); math.Abs(got-30) > 5 {
		t.Errorf("age PROCESSING = %v, want ~30", got)
	}
	if got := testutil.ToFloat64(DocumentsOldestIntermediateAgeSeconds.WithLabelValues(string(domain.StatusFailed))); got != 0 {
		t.Errorf("age FAILED = %v, want 0 (no es estado intermedio)", got)
	}
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

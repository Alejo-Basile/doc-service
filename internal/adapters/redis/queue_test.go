package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/redis/go-redis/v9"
)

func testRedisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return addr
}

func testRedisPassword(t *testing.T) string {
	t.Helper()
	return os.Getenv("REDIS_TEST_PASSWORD")
}

func setupTestQueue(t *testing.T) (*WorkQueue, context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := DefaultWorkQueueConfig()
	cfg.StreamKey = "test:stream:" + suffix
	cfg.WaitCount = 1
	cfg.WaitTimeout = 200 * time.Millisecond
	cfg.DedupTTL = 1 * time.Minute

	q := NewWorkQueue(testRedisAddr(t), testRedisPassword(t), cfg)

	// Verificar conectividad.
	if err := q.Ping(ctx); err != nil {
		cancel()
		t.Fatalf("no se pudo conectar a Redis: %v", err)
	}

	cleanup := func() {
		// Limpiar claves de dedup y stream de este test.
		client := q.Client()
		keys, _ := client.Keys(ctx, "dedup:doc-*").Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Del(ctx, cfg.StreamKey)
		_ = q.Close()
		cancel()
	}

	return q, ctx, cleanup
}

func TestWorkQueue_Enqueue(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	docID := fmt.Sprintf("doc-enq-%d", time.Now().UnixNano())
	msg := ports.WorkMessage{
		DocumentID:    docID,
		ObjectKey:     "raw-pdfs/" + docID + ".pdf",
		CorrelationID: "corr-enq",
		EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
		SchemaVersion: 1,
	}

	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue falló: %v", err)
	}

	n, err := q.XLen(ctx)
	if err != nil {
		t.Fatalf("XLen falló: %v", err)
	}
	if n != 1 {
		t.Errorf("XLen esperado 1, got %d", n)
	}
}

func TestWorkQueue_Deduplicacion(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	msg := ports.WorkMessage{
		DocumentID:    "doc-dup",
		ObjectKey:     "raw-pdfs/doc-dup.pdf",
		CorrelationID: "corr-dup",
		EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
		SchemaVersion: 1,
	}

	// Primer enqueue: OK.
	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("primer Enqueue falló: %v", err)
	}

	// Segundo enqueue con el mismo document_id: deduplicado (no error, pero no agrega).
	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("segundo Enqueue no debía fallar: %v", err)
	}

	// Tercer enqueue: también deduplicado.
	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("tercer Enqueue no debía fallar: %v", err)
	}

	// El stream solo debe tener 1 mensaje.
	n, err := q.XLen(ctx)
	if err != nil {
		t.Fatalf("XLen falló: %v", err)
	}
	if n != 1 {
		t.Errorf("XLen esperado 1 (deduplicado), got %d", n)
	}
}

func TestWorkQueue_DocumentIDsDistintos(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	suffix := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		docID := fmt.Sprintf("doc-multi-%d-%d", suffix, i)
		msg := ports.WorkMessage{
			DocumentID:    docID,
			ObjectKey:     "raw-pdfs/" + docID + ".pdf",
			CorrelationID: fmt.Sprintf("corr-multi-%d", i),
			EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
			SchemaVersion: 1,
		}
		if err := q.Enqueue(ctx, msg); err != nil {
			t.Fatalf("Enqueue %d falló: %v", i, err)
		}
	}

	n, err := q.XLen(ctx)
	if err != nil {
		t.Fatalf("XLen falló: %v", err)
	}
	if n != 3 {
		t.Errorf("XLen esperado 3, got %d", n)
	}
}

func TestWorkQueue_PayloadDelContrato(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	msg := ports.WorkMessage{
		DocumentID:    "doc-payload",
		ObjectKey:     "raw-pdfs/doc-payload.pdf",
		CorrelationID: "corr-payload",
		EnqueuedAt:    "2026-10-04T10:00:00Z",
		SchemaVersion: 1,
	}

	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue falló: %v", err)
	}

	msgs, err := q.XRange(ctx, "-", "+", 10)
	if err != nil {
		t.Fatalf("XRange falló: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("XRange longitud esperada 1, got %d", len(msgs))
	}

	// Verificar que el payload tiene los campos del contrato.
	payload, ok := msgs[0].Values["payload"].(string)
	if !ok {
		t.Fatalf("payload no es string: %v", msgs[0].Values["payload"])
	}

	// No debe tener campo attempts.
	if contains(payload, "attempts") {
		t.Error("el payload no debe contener campo 'attempts'")
	}

	// Debe tener los campos del contrato.
	for _, campo := range []string{"document_id", "object_key", "correlation_id", "enqueued_at", "schema_version"} {
		if !contains(payload, campo) {
			t.Errorf("payload falta campo %q: %s", campo, payload)
		}
	}
}

func TestWorkQueue_Ping(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	if err := q.Ping(ctx); err != nil {
		t.Errorf("Ping falló: %v", err)
	}
}

func TestWorkQueue_WaitNoBloqueaEnStandalone(t *testing.T) {
	// En Redis standalone (sin réplicas), WAIT 1 puede retornar 0 réplicas
	// pero NO debe fallar el Enqueue. El XADD ya se hizo.
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	msg := ports.WorkMessage{
		DocumentID:    "doc-wait",
		ObjectKey:     "raw-pdfs/doc-wait.pdf",
		CorrelationID: "corr-wait",
		EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
		SchemaVersion: 1,
	}

	// No debía fallar aunque WAIT no encuentre réplicas.
	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue con WAIT falló: %v", err)
	}

	n, err := q.XLen(ctx)
	if err != nil {
		t.Fatalf("XLen falló: %v", err)
	}
	if n != 1 {
		t.Errorf("XLen esperado 1, got %d", n)
	}
}

// contains verifica si un string contiene un substring.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestWorkQueue_IntegracionConRedisReal es un test de humo que verifica
// que el cliente Redis funciona contra la instancia real.
func TestWorkQueue_IntegracionConRedisReal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr(t), Password: testRedisPassword(t)})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis no disponible: %v", err)
	}

	// XADD básico.
	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "test:smoke",
		Values: map[string]interface{}{"hello": "world"},
	}).Result()
	if err != nil {
		t.Fatalf("XADD falló: %v", err)
	}
	if id == "" {
		t.Error("XADD retornó ID vacío")
	}

	// Limpiar.
	client.Del(ctx, "test:smoke")
}

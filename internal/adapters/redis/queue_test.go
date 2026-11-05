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

// TestWorkQueue_FalloXADDLiberaDedup verifica que si XADD falla (p. ej. por un
// tipo de clave incorrecto), la marca de deduplicación se libera para que el
// replay del watcher pueda reencolar el documento cuando Redis vuelva (#55).
func TestWorkQueue_FalloXADDLiberaDedup(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	docID := fmt.Sprintf("doc-xadd-fail-%d", time.Now().UnixNano())
	msg := ports.WorkMessage{
		DocumentID:    docID,
		ObjectKey:     "raw-pdfs/" + docID + ".pdf",
		CorrelationID: "corr-xadd-fail",
		EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
		SchemaVersion: 1,
	}

	// Forzar que XADD falle: la clave del stream existe con otro tipo (string).
	streamKey := q.StreamKey()
	if err := q.Client().Set(ctx, streamKey, "no-es-un-stream", 0).Err(); err != nil {
		t.Fatalf("Set streamKey: %v", err)
	}

	if err := q.Enqueue(ctx, msg); err == nil {
		t.Fatal("Enqueue debía fallar con WRONGTYPE en XADD")
	}

	dedupKey := "dedup:" + docID
	exists, err := q.Client().Exists(ctx, dedupKey).Result()
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists != 0 {
		t.Errorf("la marca %s no se liberó tras el fallo de XADD", dedupKey)
	}

	// Al corregir la clave, el replay puede reencolar exactamente una vez.
	if err := q.Client().Del(ctx, streamKey).Err(); err != nil {
		t.Fatalf("Del streamKey: %v", err)
	}
	if err := q.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue tras liberar dedup falló: %v", err)
	}
	n, err := q.XLen(ctx)
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if n != 1 {
		t.Errorf("XLen = %d, want 1", n)
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

func TestWorkQueue_ContratoCamposPlanos(t *testing.T) {
	q, ctx, cleanup := setupTestQueue(t)
	defer cleanup()

	msg := ports.WorkMessage{
		DocumentID:    "doc-contrato",
		ObjectKey:     "raw-pdfs/doc-contrato.pdf",
		CorrelationID: "corr-contrato",
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

	values := msgs[0].Values
	if _, wrapped := values["payload"]; wrapped {
		t.Fatalf("el mensaje no debe envolver el contrato en `payload`: %v", values)
	}
	if len(values) != 5 {
		t.Errorf("campos del mensaje = %d, want 5: %v", len(values), values)
	}

	// Mismos nombres y tipos que `WorkItem::from_stream_fields` del worker:
	// cinco campos planos, con schema_version como texto (Redis no tiene enteros).
	esperado := map[string]string{
		"document_id":    msg.DocumentID,
		"object_key":     msg.ObjectKey,
		"correlation_id": msg.CorrelationID,
		"enqueued_at":    msg.EnqueuedAt,
		"schema_version": "1",
	}
	for campo, valor := range esperado {
		got, ok := values[campo].(string)
		if !ok {
			t.Errorf("campo %q no es string: %v", campo, values[campo])
			continue
		}
		if got != valor {
			t.Errorf("campo %q = %q, want %q", campo, got, valor)
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

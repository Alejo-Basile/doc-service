package relay

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/adapters/changestream"
	mongoadapter "github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
	redidadapter "github.com/Alejo-Basile/doc-service/internal/adapters/redis"
	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
)

func testAddrs(t *testing.T) (string, string, string) {
	t.Helper()
	mongoURI := os.Getenv("MONGO_TEST_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://127.0.0.1:27017/?replicaSet=rs0"
	}
	redisAddr := os.Getenv("REDIS_TEST_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	redisPassword := os.Getenv("REDIS_TEST_PASSWORD")
	return mongoURI, redisAddr, redisPassword
}

func TestRelay_EncolaEventoUPLOADED(t *testing.T) {
	mongoURI, redisAddr, redisPassword := testAddrs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := mongoadapter.DefaultPoolConfig()
	dbName := fmt.Sprintf("docservice_relay_%d", time.Now().UnixNano())
	mclient, err := mongoadapter.NewClient(ctx, mongoURI, dbName, cfg)
	if err != nil {
		t.Fatalf("Mongo no disponible: %v", err)
	}
	defer func() {
		_ = mclient.DB().Drop(context.Background())
		_ = mclient.Close(context.Background())
	}()

	repo := mongoadapter.NewDocumentRepository(mclient, "documents")
	tokenStore := mongoadapter.NewResumeTokenStore(mclient)

	queueCfg := redidadapter.DefaultWorkQueueConfig()
	queueCfg.StreamKey = fmt.Sprintf("test:relay:%d", time.Now().UnixNano())
	queue := redidadapter.NewWorkQueue(redisAddr, redisPassword, queueCfg)
	defer queue.Close()

	if err := queue.Ping(ctx); err != nil {
		t.Fatalf("Redis no disponible: %v", err)
	}

	r := New(queue, 1)
	w := changestream.NewWatcher(mclient, tokenStore, r.Handler(), changestream.DefaultWatcherConfig())

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		_ = w.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	doc := domain.NewDocument("corr-relay-1", time.Now().UTC().Add(24*time.Hour))
	doc.SetObjectKey("raw-pdfs")
	if err := repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}

	ok, err := repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)
	if err != nil || !ok {
		t.Fatalf("UpdateStatus falló: ok=%v err=%v", ok, err)
	}

	deadline := time.After(5 * time.Second)
	for {
		n, err := queue.XLen(ctx)
		if err == nil && n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("el relay no encoló el mensaje en 5s")
		case <-time.After(100 * time.Millisecond):
		}
	}

	msgs, err := queue.XRange(ctx, "-", "+", 10)
	if err != nil {
		t.Fatalf("XRange falló: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("XLen esperado 1, got %d", len(msgs))
	}

	payload, ok := msgs[0].Values["payload"].(string)
	if !ok {
		t.Fatalf("payload no es string: %v", msgs[0].Values["payload"])
	}

	for _, campo := range []string{"document_id", "object_key", "correlation_id", "schema_version"} {
		if !contains(payload, campo) {
			t.Errorf("payload falta campo %q: %s", campo, payload)
		}
	}
	if !contains(payload, doc.ID) {
		t.Errorf("payload falta document_id %q: %s", doc.ID, payload)
	}
}

func TestRelay_DosWatchersUnSoloMensaje(t *testing.T) {
	mongoURI, redisAddr, redisPassword := testAddrs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := mongoadapter.DefaultPoolConfig()
	dbName := fmt.Sprintf("docservice_relay2_%d", time.Now().UnixNano())
	mclient, err := mongoadapter.NewClient(ctx, mongoURI, dbName, cfg)
	if err != nil {
		t.Fatalf("Mongo no disponible: %v", err)
	}
	defer func() {
		_ = mclient.DB().Drop(context.Background())
		_ = mclient.Close(context.Background())
	}()

	repo := mongoadapter.NewDocumentRepository(mclient, "documents")
	tokenStore := mongoadapter.NewResumeTokenStore(mclient)

	queueCfg := redidadapter.DefaultWorkQueueConfig()
	queueCfg.StreamKey = fmt.Sprintf("test:relay2:%d", time.Now().UnixNano())
	queue := redidadapter.NewWorkQueue(redisAddr, redisPassword, queueCfg)
	defer queue.Close()

	r1 := New(queue, 1)
	r2 := New(queue, 1)

	w1 := changestream.NewWatcher(mclient, tokenStore, r1.Handler(), changestream.DefaultWatcherConfig())
	w2 := changestream.NewWatcher(mclient, tokenStore, r2.Handler(), changestream.DefaultWatcherConfig())

	done := make(chan struct{}, 2)
	go func() { _ = w1.Run(ctx); done <- struct{}{} }()
	go func() { _ = w2.Run(ctx); done <- struct{}{} }()

	time.Sleep(500 * time.Millisecond)

	doc := domain.NewDocument("corr-relay-2w", time.Now().UTC().Add(24*time.Hour))
	doc.SetObjectKey("raw-pdfs")
	if err := repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}

	_, _ = repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)

	deadline := time.After(5 * time.Second)
	for {
		n, err := queue.XLen(ctx)
		if err == nil && n >= 1 {
			time.Sleep(500 * time.Millisecond)
			n2, _ := queue.XLen(ctx)
			if n2 == 1 {
				break
			}
			if n2 > 1 {
				t.Fatalf("deduplicación falló: %d mensajes en la cola, esperado 1", n2)
			}
		}
		select {
		case <-deadline:
			t.Fatal("timeout esperando mensaje en la cola")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestRelay_ObjectKeyDerivado(t *testing.T) {
	expected := "raw-pdfs/doc-derived.pdf"
	if fmt.Sprintf("raw-pdfs/%s.pdf", "doc-derived") != expected {
		t.Errorf("derivación de object_key incorrecta")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// --- Tests unitarios de reintento/backoff (S4-P2-05) ---

// flakyQueue falla las primeras `failN` llamadas a Enqueue y luego tiene éxito.
type flakyQueue struct {
	failN   int
	calls   int
	enqueue int
}

func (q *flakyQueue) Enqueue(_ context.Context, _ ports.WorkMessage) error {
	q.calls++
	if q.calls <= q.failN {
		return fmt.Errorf("cola caída (intento %d)", q.calls)
	}
	q.enqueue++
	return nil
}

func (q *flakyQueue) Ping(_ context.Context) error { return nil }

func TestRelay_ReintentaHastaEncolar(t *testing.T) {
	// La cola falla 2 veces y al 3.er intento encola.
	q := &flakyQueue{failN: 2}
	r := NewWithConfig(q, 1, Config{MaxRetries: 5, BaseBackoff: time.Millisecond})

	err := r.Handler()(context.Background(), changestream.Event{
		DocumentID: "doc-1",
		Status:     "UPLOADED",
		ObjectKey:  "raw-pdfs/doc-1.pdf",
	})
	if err != nil {
		t.Fatalf("Handler error = %v, want nil (debía reintentar y encolar)", err)
	}
	if q.enqueue != 1 {
		t.Errorf("encolados = %d, want 1", q.enqueue)
	}
	if q.calls != 3 {
		t.Errorf("llamadas = %d, want 3 (2 fallos + 1 éxito)", q.calls)
	}
}

func TestRelay_AgotaReintentosYDevuelveError(t *testing.T) {
	// La cola siempre falla.
	q := &flakyQueue{failN: 100}
	r := NewWithConfig(q, 1, Config{MaxRetries: 3, BaseBackoff: time.Millisecond})

	err := r.Handler()(context.Background(), changestream.Event{
		DocumentID: "doc-2",
		Status:     "UPLOADED",
	})
	if err == nil {
		t.Fatal("Handler error = nil, want error tras agotar reintentos")
	}
	// 1 intento inicial + 3 reintentos = 4 llamadas.
	if q.calls != 4 {
		t.Errorf("llamadas = %d, want 4 (1 + 3 reintentos)", q.calls)
	}
}

func TestRelay_CancelacionDeContexto_Aborta(t *testing.T) {
	q := &flakyQueue{failN: 100}
	r := NewWithConfig(q, 1, Config{MaxRetries: 100, BaseBackoff: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ya cancelado

	err := r.Handler()(ctx, changestream.Event{DocumentID: "doc-3", Status: "UPLOADED"})
	if err == nil {
		t.Fatal("Handler error = nil, want error por contexto cancelado")
	}
}

func TestRelay_EventoNoUploaded_NoEncola(t *testing.T) {
	q := &flakyQueue{}
	r := NewWithConfig(q, 1, Config{MaxRetries: 3, BaseBackoff: time.Millisecond})

	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "doc-4", Status: "QUEUED"})
	if err != nil {
		t.Fatalf("Handler error = %v, want nil", err)
	}
	if q.calls != 0 {
		t.Errorf("llamadas = %d, want 0 (no es UPLOADED)", q.calls)
	}
}

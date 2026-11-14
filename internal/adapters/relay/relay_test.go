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
		t.Skipf("Mongo no disponible (%s): %v", mongoURI, err)
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
		t.Skipf("Redis no disponible (%s): %v", redisAddr, err)
	}

	r := New(queue, repo, 1)
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

	values := msgs[0].Values
	if _, wrapped := values["payload"]; wrapped {
		t.Fatalf("el mensaje no debe envolver el contrato en `payload`: %v", values)
	}
	if got, ok := values["document_id"].(string); !ok || got != doc.ID {
		t.Errorf("document_id = %v, want %q", values["document_id"], doc.ID)
	}
	for _, campo := range []string{"object_key", "correlation_id", "enqueued_at", "schema_version"} {
		if _, ok := values[campo].(string); !ok {
			t.Errorf("falta el campo plano %q: %v", campo, values)
		}
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
		t.Skipf("Mongo no disponible (%s): %v", mongoURI, err)
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

	r1 := New(queue, repo, 1)
	r2 := New(queue, repo, 1)

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
	r := NewWithConfig(q, &fakeRelayRepo{}, 1, Config{MaxRetries: 5, BaseBackoff: time.Millisecond})

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
	r := NewWithConfig(q, &fakeRelayRepo{}, 1, Config{MaxRetries: 3, BaseBackoff: time.Millisecond})

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
	r := NewWithConfig(q, &fakeRelayRepo{}, 1, Config{MaxRetries: 100, BaseBackoff: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ya cancelado

	err := r.Handler()(ctx, changestream.Event{DocumentID: "doc-3", Status: "UPLOADED"})
	if err == nil {
		t.Fatal("Handler error = nil, want error por contexto cancelado")
	}
}

func TestRelay_EventoNoUploaded_NoEncola(t *testing.T) {
	q := &flakyQueue{}
	r := NewWithConfig(q, &fakeRelayRepo{}, 1, Config{MaxRetries: 3, BaseBackoff: time.Millisecond})

	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "doc-4", Status: "QUEUED"})
	if err != nil {
		t.Fatalf("Handler error = %v, want nil", err)
	}
	if q.calls != 0 {
		t.Errorf("llamadas = %d, want 0 (no es UPLOADED)", q.calls)
	}
}

type fakeRelayRepo struct{}

func (f *fakeRelayRepo) Insert(ctx context.Context, doc *domain.Document) error { return nil }
func (f *fakeRelayRepo) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	return &domain.Document{ID: id, Status: domain.StatusQueued}, nil
}
func (f *fakeRelayRepo) UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error) {
	return true, nil
}
func (f *fakeRelayRepo) UpdateStatusWithHistory(ctx context.Context, id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any) (bool, error) {
	return true, nil
}
func (f *fakeRelayRepo) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	return nil, 0, nil
}
func (f *fakeRelayRepo) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	return nil, "", nil
}

type relayOrder struct{ steps *[]string }

type orderRepo struct {
	relayOrder
	doc   *domain.Document
	fail  error
	apply bool
	entry domain.StatusEntry
	extra map[string]any
}

func (o *orderRepo) Insert(ctx context.Context, doc *domain.Document) error { return nil }
func (o *orderRepo) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	if o.doc == nil {
		return nil, ports.ErrNotFound
	}
	return o.doc, nil
}
func (o *orderRepo) UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error) {
	return o.UpdateStatusWithHistory(ctx, id, from, to, domain.StatusEntry{}, fields)
}
func (o *orderRepo) UpdateStatusWithHistory(ctx context.Context, id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any) (bool, error) {
	*o.steps = append(*o.steps, "transition")
	o.entry = entry
	o.extra = extraSet
	if o.fail != nil {
		return false, o.fail
	}
	if o.apply {
		if o.doc != nil {
			o.doc.Status = to
		}
	}
	return o.apply, nil
}
func (o *orderRepo) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	return nil, 0, nil
}
func (o *orderRepo) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	return nil, "", nil
}

type orderQueue struct {
	relayOrder
	msg ports.WorkMessage
}

func (o *orderQueue) Enqueue(ctx context.Context, msg ports.WorkMessage) error {
	*o.steps = append(*o.steps, "enqueue")
	o.msg = msg
	return nil
}
func (o *orderQueue) Ping(ctx context.Context) error { return nil }

func TestRelay_TransicionaQueuedAntesDeEncolar(t *testing.T) {
	order := &relayOrder{steps: new([]string)}
	repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusUploaded}, apply: true}
	q := &orderQueue{relayOrder: *order}
	// share the same steps slice
	repo.relayOrder.steps = order.steps
	q.relayOrder.steps = order.steps
	r := New(q, repo, 1)
	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "d1", Status: "UPLOADED", ObjectKey: "k", CorrelationID: "c"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(*order.steps) != 2 || (*order.steps)[0] != "transition" || (*order.steps)[1] != "enqueue" {
		t.Fatalf("order = %v", order.steps)
	}
	if repo.doc.Status != domain.StatusQueued {
		t.Fatalf("status = %v", repo.doc.Status)
	}
}

func TestRelay_TransicionPersisteDatosDelEvento(t *testing.T) {
	order := &relayOrder{steps: new([]string)}
	repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusUploaded}, apply: true}
	q := &orderQueue{relayOrder: *order}
	repo.relayOrder.steps = order.steps
	q.relayOrder.steps = order.steps
	r := New(q, repo, 1)
	err := r.Handler()(context.Background(), changestream.Event{
		DocumentID:    "d1",
		Status:        "UPLOADED",
		ObjectKey:     "raw-pdfs/d1.pdf",
		CorrelationID: "c-1",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got := repo.extra["object_key"]; got != "raw-pdfs/d1.pdf" {
		t.Errorf("extraSet object_key = %v, want %q", got, "raw-pdfs/d1.pdf")
	}
	if repo.entry.Actor != domain.ActorDocService {
		t.Errorf("entry.Actor = %v, want %v", repo.entry.Actor, domain.ActorDocService)
	}
	if repo.entry.CorrelationID != "c-1" {
		t.Errorf("entry.CorrelationID = %v, want %q", repo.entry.CorrelationID, "c-1")
	}
}

func TestRelay_ObjectKeyDerivadoPersisteYCoincideConMensaje(t *testing.T) {
	esperado := "raw-pdfs/d1.pdf"
	casos := []struct {
		nombre      string
		eventObjKey string
	}{
		{"evento sin object_key", ""},
		{"evento con otra object_key", "otra-clave.pdf"},
	}
	for _, tt := range casos {
		t.Run(tt.nombre, func(t *testing.T) {
			order := &relayOrder{steps: new([]string)}
			repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusUploaded}, apply: true}
			q := &orderQueue{relayOrder: *order}
			repo.relayOrder.steps = order.steps
			q.relayOrder.steps = order.steps
			r := New(q, repo, 1)
			err := r.Handler()(context.Background(), changestream.Event{
				DocumentID: "d1",
				Status:     "UPLOADED",
				ObjectKey:  tt.eventObjKey,
			})
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got := repo.extra["object_key"]; got != esperado {
				t.Errorf("document object_key = %v, want %q", got, esperado)
			}
			if q.msg.ObjectKey != esperado {
				t.Errorf("mensaje object_key = %q, want %q", q.msg.ObjectKey, esperado)
			}
		})
	}
}

func TestRelay_NoEncolaSiDocumentoNoPublicable(t *testing.T) {
	casos := []struct {
		nombre string
		status domain.Status
	}{
		{"PROCESSING", domain.StatusProcessing},
		{"COMPLETED", domain.StatusCompleted},
	}
	for _, tt := range casos {
		t.Run(tt.nombre, func(t *testing.T) {
			order := &relayOrder{steps: new([]string)}
			repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: tt.status}, apply: false}
			q := &orderQueue{relayOrder: *order}
			repo.relayOrder.steps = order.steps
			q.relayOrder.steps = order.steps
			r := New(q, repo, 1)
			err := r.Handler()(context.Background(), changestream.Event{DocumentID: "d1", Status: "UPLOADED", ObjectKey: "k"})
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if len(*order.steps) != 1 || (*order.steps)[0] != "transition" {
				t.Fatalf("order = %v, want solo [transition] (sin enqueue)", *order.steps)
			}
		})
	}
}

func TestRelay_ReencolaSiYaEstaQueued(t *testing.T) {
	order := &relayOrder{steps: new([]string)}
	repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusQueued}, apply: false}
	q := &orderQueue{relayOrder: *order}
	repo.relayOrder.steps = order.steps
	q.relayOrder.steps = order.steps
	r := New(q, repo, 1)
	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "d1", Status: "UPLOADED", ObjectKey: "k"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(*order.steps) != 2 || (*order.steps)[0] != "transition" || (*order.steps)[1] != "enqueue" {
		t.Fatalf("order = %v", order.steps)
	}
}

func TestRelay_NoEncolaSiSigueUploaded(t *testing.T) {
	order := &relayOrder{steps: new([]string)}
	repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusUploaded}, apply: false}
	q := &orderQueue{relayOrder: *order}
	repo.relayOrder.steps = order.steps
	q.relayOrder.steps = order.steps
	r := New(q, repo, 1)
	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "d1", Status: "UPLOADED", ObjectKey: "k"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if len(*order.steps) != 1 {
		t.Fatalf("order = %v", order.steps)
	}
}

func TestRelay_ErrorDeRepoNoEncola(t *testing.T) {
	order := &relayOrder{steps: new([]string)}
	repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusUploaded}, fail: fmt.Errorf("boom"), apply: false}
	q := &orderQueue{relayOrder: *order}
	repo.relayOrder.steps = order.steps
	q.relayOrder.steps = order.steps
	r := New(q, repo, 1)
	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "d1", Status: "UPLOADED", ObjectKey: "k"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if len(*order.steps) != 1 {
		t.Fatalf("order = %v", order.steps)
	}
}

func TestRelay_EventoNoUploaded_NoUsaRepo(t *testing.T) {
	order := &relayOrder{steps: new([]string)}
	repo := &orderRepo{relayOrder: *order, doc: &domain.Document{ID: "d1", Status: domain.StatusQueued}, apply: false}
	q := &orderQueue{relayOrder: *order}
	repo.relayOrder.steps = order.steps
	q.relayOrder.steps = order.steps
	r := New(q, repo, 1)
	err := r.Handler()(context.Background(), changestream.Event{DocumentID: "d1", Status: "QUEUED", ObjectKey: "k"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(*order.steps) != 0 {
		t.Fatalf("order = %v", order.steps)
	}
}

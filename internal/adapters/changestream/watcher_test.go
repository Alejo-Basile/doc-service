package changestream

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
	"github.com/Alejo-Basile/doc-service/internal/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func testMongoURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		uri = "mongodb://127.0.0.1:27017/?replicaSet=rs0"
	}
	return uri
}

type testHarness struct {
	client     *mongo.Client
	repo       *mongo.DocumentRepository
	tokenStore *mongo.ResumeTokenStore
	dbName     string
}

func setupHarness(t *testing.T) (*testHarness, context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	cfg := mongo.DefaultPoolConfig()
	dbName := fmt.Sprintf("docservice_cs_%d", time.Now().UnixNano())

	client, err := mongo.NewClient(ctx, testMongoURI(t), dbName, cfg)
	if err != nil {
		cancel()
		t.Fatalf("MongoDB no disponible: %v", err)
	}

	repo := mongo.NewDocumentRepository(client, "documents")
	tokenStore := mongo.NewResumeTokenStore(client)

	cleanup := func() {
		_ = client.DB().Drop(context.Background())
		_ = client.Close(context.Background())
		cancel()
	}

	return &testHarness{client, repo, tokenStore, dbName}, ctx, cleanup
}

func TestWatcher_DetectaEventoUPLOADED(t *testing.T) {
	h, ctx, cleanup := setupHarness(t)
	defer cleanup()

	var mu sync.Mutex
	var received []Event

	handler := func(ctx context.Context, event Event) error {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}

	w := NewWatcher(h.client, h.tokenStore, handler, DefaultWatcherConfig())

	// Correr el watcher en background.
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		_ = w.Run(ctx)
	}()

	// Dar tiempo a que el watcher inicie.
	time.Sleep(500 * time.Millisecond)

	// Insertar un documento y transicionarlo a UPLOADED.
	doc := domain.NewDocument("corr-cs-1", time.Now().UTC().Add(24*time.Hour))
	doc.SetObjectKey("raw-pdfs")
	if err := h.repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}

	ok, err := h.repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)
	if err != nil || !ok {
		t.Fatalf("UpdateStatus falló: ok=%v err=%v", ok, err)
	}

	// Esperar a que el watcher reciba el evento.
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()

		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("el watcher no recibió el evento UPLOADED en 3s")
		case <-time.After(100 * time.Millisecond):
		}
	}

	mu.Lock()
	event := received[0]
	mu.Unlock()

	if event.DocumentID != doc.ID {
		t.Errorf("DocumentID esperado %q, got %q", doc.ID, event.DocumentID)
	}
	if event.Status != "UPLOADED" {
		t.Errorf("Status esperado 'UPLOADED', got %q", event.Status)
	}
	if event.ObjectKey != doc.ObjectKey {
		t.Errorf("ObjectKey esperado %q, got %q", doc.ObjectKey, event.ObjectKey)
	}

	// Cancelar el watcher.
	cancelFn := context.CancelFunc(nil)
	if c, ok := ctx.(interface{ Cancel() }); ok {
		_ = c
	}
	// El ctx del harness ya tiene cancel en cleanup; pero necesitamos
	// cancelar ahora. Usamos un ctx derived.
	_ = cancelFn

	// Esperar a que el watcher termine.
	select {
	case <-watcherDone:
	case <-time.After(2 * time.Second):
		// El watcher puede tardar en cancelarse; no es fatal para el test.
	}
}

func TestWatcher_NoDetectaOtrosEstados(t *testing.T) {
	h, ctx, cleanup := setupHarness(t)
	defer cleanup()

	var mu sync.Mutex
	var received []Event

	handler := func(ctx context.Context, event Event) error {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}

	w := NewWatcher(h.client, h.tokenStore, handler, DefaultWatcherConfig())

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		_ = w.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	// Insertar un documento que NO pasa a UPLOADED.
	doc := domain.NewDocument("corr-cs-2", time.Now().UTC().Add(24*time.Hour))
	if err := h.repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}

	// Transicionar a QUEUED (no es UPLOADED).
	// Primero a UPLOADED, luego a QUEUED — pero el pipeline filtra status=UPLOADED.
	// Hacer solo Insert (PENDING_UPLOAD) no debería generar evento.
	time.Sleep(1 * time.Second)

	mu.Lock()
	n := len(received)
	mu.Unlock()

	if n != 0 {
		t.Errorf("el watcher no debía recibir eventos para estados no-UPLOADED, got %d", n)
	}
}

func TestWatcher_PersisteResumeToken(t *testing.T) {
	h, ctx, cleanup := setupHarness(t)
	defer cleanup()

	var mu sync.Mutex
	var received []Event

	handler := func(ctx context.Context, event Event) error {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}

	w := NewWatcher(h.client, h.tokenStore, handler, DefaultWatcherConfig())

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		_ = w.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	// Insertar y transicionar.
	doc := domain.NewDocument("corr-cs-token", time.Now().UTC().Add(24*time.Hour))
	doc.SetObjectKey("raw-pdfs")
	if err := h.repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}

	_, _ = h.repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)

	// Esperar evento.
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no se recibió evento en 3s")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Verificar que el resume token fue persistido.
	// Dar un margen para que el watcher lo guarde.
	time.Sleep(500 * time.Millisecond)

	token, err := h.tokenStore.Get(ctx)
	if err != nil {
		t.Fatalf("tokenStore.Get falló: %v", err)
	}
	if token == "" {
		t.Error("el resume token debía estar persistido después del evento")
	}
}

func TestWatcher_SimulacionDosWatchers(t *testing.T) {
	// Simular dos watchers activos: ambos reciben el mismo evento,
	// pero la deduplicación en el WorkQueue garantiza 1 solo XADD.
	h, ctx, cleanup := setupHarness(t)
	defer cleanup()

	var mu sync.Mutex
	var received1, received2 []Event

	handler1 := func(ctx context.Context, event Event) error {
		mu.Lock()
		received1 = append(received1, event)
		mu.Unlock()
		return nil
	}
	handler2 := func(ctx context.Context, event Event) error {
		mu.Lock()
		received2 = append(received2, event)
		mu.Unlock()
		return nil
	}

	w1 := NewWatcher(h.client, h.tokenStore, handler1, DefaultWatcherConfig())
	w2 := NewWatcher(h.client, h.tokenStore, handler2, DefaultWatcherConfig())

	done := make(chan struct{}, 2)
	go func() { _ = w1.Run(ctx); done <- struct{}{} }()
	go func() { _ = w2.Run(ctx); done <- struct{}{} }()

	time.Sleep(500 * time.Millisecond)

	// Insertar y transicionar.
	doc := domain.NewDocument("corr-cs-2w", time.Now().UTC().Add(24*time.Hour))
	doc.SetObjectKey("raw-pdfs")
	if err := h.repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}

	_, _ = h.repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)

	// Esperar a que ambos watchers reciban.
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		n1, n2 := len(received1), len(received2)
		mu.Unlock()
		if n1 >= 1 && n2 >= 1 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("watchers no recibieron evento: w1=%d w2=%d", len(received1), len(received2))
		case <-time.After(100 * time.Millisecond):
		}
	}

	mu.Lock()
	e1, e2 := received1[0], received2[0]
	mu.Unlock()

	// Ambos watchers recibieron el MISMO evento.
	if e1.DocumentID != e2.DocumentID {
		t.Errorf("ambos watchers debían recibir el mismo document_id: %q vs %q", e1.DocumentID, e2.DocumentID)
	}
}

// TestWatcher_ParseEvent verifica el parseo de eventos BSON.
func TestWatcher_ParseEvent(t *testing.T) {
	w := &Watcher{}

	raw := bson.M{
		"operationType": "update",
		"fullDocument": bson.M{
			"_id":            "doc-123",
			"status":         "UPLOADED",
			"object_key":     "raw-pdfs/doc-123.pdf",
			"correlation_id": "corr-123",
			"schema_version": int32(1),
		},
	}

	event, err := w.parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent falló: %v", err)
	}

	if event.DocumentID != "doc-123" {
		t.Errorf("DocumentID esperado 'doc-123', got %q", event.DocumentID)
	}
	if event.Status != "UPLOADED" {
		t.Errorf("Status esperado 'UPLOADED', got %q", event.Status)
	}
	if event.ObjectKey != "raw-pdfs/doc-123.pdf" {
		t.Errorf("ObjectKey esperado 'raw-pdfs/doc-123.pdf', got %q", event.ObjectKey)
	}
	if event.CorrelationID != "corr-123" {
		t.Errorf("CorrelationID esperado 'corr-123', got %q", event.CorrelationID)
	}
	if event.SchemaVersion != 1 {
		t.Errorf("SchemaVersion esperado 1, got %d", event.SchemaVersion)
	}
}

// TestIsResumeTokenInvalid verifica el matcher que detecta el error fatal de
// Change Stream ante un resume token invalidado por el servidor (rotación del
// oplog). Cubre los dos wrappers observados en runtime: la apertura del stream
// ("watch:") y el getMore ("stream error:").
func TestIsResumeTokenInvalid(t *testing.T) {
	tt := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "watch wrapper",
			err:  fmt.Errorf("watch: (ChangeStreamFatalError) Executor error during getMore :: caused by :: cannot resume stream; the resume token was not found"),
			want: true,
		},
		{
			name: "stream error wrapper",
			err:  fmt.Errorf("stream error: (ChangeStreamFatalError) Executor error during getMore :: caused by :: cannot resume stream; the resume token was not found"),
			want: true,
		},
		{
			name: "error no relacionado",
			err:  fmt.Errorf("stream error: connection refused"),
			want: false,
		},
		{
			name: "token corrupto en disco (Location40647)",
			err:  fmt.Errorf(`watch: (Location40647) Bad resume token: error deserialization feature is not enabled`),
			want: true,
		},
		{
			name: "token malformado (FailedToParse)",
			err:  fmt.Errorf(`watch: (FailedToParse) resume token string was not a valid hex string`),
			want: true,
		},
		{
			name: "keystring corrupto (Location50811)",
			err:  fmt.Errorf(`watch: (Location50811) KeyString format error: Unknown type: 0`),
			want: true,
		},
		{
			name: "error relacionado pero sin substring clave",
			err:  fmt.Errorf("watch: (NetworkTimeout) timed out"),
			want: false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if got := isResumeTokenInvalid(tc.err); got != tc.want {
				t.Errorf("isResumeTokenInvalid(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestWatcher_RecuperaTrasTokenInvalido verifica la auto-recuperación
// (S1-P2-07/08): cuando Mongo rechaza reanudar desde el resume token
// persistido ("cannot resume stream; the resume token was not found"), el
// watcher debe descartar el token, reiniciar el stream fresco y seguir
// procesando eventos sin intervención manual.
//
// Para reproducir el fallo de forma determinista se genera un resume token
// REAL (watcher procesa un evento y lo persiste), se corrompe su clusterTime
// llevándolo a opTime 0,0 (anterior al oplog) y se vuelve a sembrar: Mongo
// rechaza la reanudación tal como ocurre tras una rotación del oplog.
func TestWatcher_RecuperaTrasTokenInvalido(t *testing.T) {
	h, ctx, cleanup := setupHarness(t)
	defer cleanup()

	// Fase A: watcher real procesa un evento y persiste un token válido.
	{
		var mu sync.Mutex
		var received []Event
		handler := func(ctx context.Context, event Event) error {
			mu.Lock()
			received = append(received, event)
			mu.Unlock()
			return nil
		}
		phaseA, cancelA := context.WithCancel(ctx)
		w := NewWatcher(h.client, h.tokenStore, handler, DefaultWatcherConfig())
		done := make(chan struct{})
		go func() { defer close(done); _ = w.Run(phaseA) }()

		time.Sleep(500 * time.Millisecond)
		doc := domain.NewDocument("corr-cs-heal-a", time.Now().UTC().Add(24*time.Hour))
		doc.SetObjectKey("raw-pdfs")
		if err := h.repo.Insert(ctx, doc); err != nil {
			t.Fatalf("Insert falló: %v", err)
		}
		ok, err := h.repo.UpdateStatus(ctx, doc.ID,
			domain.StatusPendingUpload, domain.StatusUploaded,
			map[string]any{"object_key": doc.ObjectKey},
		)
		if err != nil || !ok {
			t.Fatalf("UpdateStatus falló: ok=%v err=%v", ok, err)
		}

		deadline := time.After(3 * time.Second)
		for {
			mu.Lock()
			n := len(received)
			mu.Unlock()
			if n >= 1 {
				break
			}
			select {
			case <-deadline:
				t.Fatal("no se recibió evento en fase A")
			case <-time.After(100 * time.Millisecond):
			}
		}
		// Dejar que el watcher persista el token antes de cerrar la fase.
		time.Sleep(500 * time.Millisecond)
		cancelA()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}

	// Fase B: corromper el clusterTime del token persistido (opTime 0,0).
	token, err := h.tokenStore.Get(ctx)
	if err != nil {
		t.Fatalf("tokenStore.Get: %v", err)
	}
	if token == "" {
		t.Fatal("el watcher debió persistir un resume token en fase A")
	}

	var raw bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(token), true, &raw); err != nil {
		t.Fatalf("deserializar token real: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("deserializar raw token: %v", err)
	}
	dataHex, ok := doc["_data"].(string)
	if !ok || len(dataHex) < 16 {
		t.Fatalf("token sin campo _data hex válido: %v", doc)
	}
	staleHex := strings.Repeat("0", 16) + dataHex[16:]
	staleDoc, err := bson.MarshalExtJSON(
		bson.D{{Key: "_data", Value: staleHex}}, true, false,
	)
	if err != nil {
		t.Fatalf("serializar token stale: %v", err)
	}
	if err := h.tokenStore.Save(ctx, string(staleDoc)); err != nil {
		t.Fatalf("sembrar token stale: %v", err)
	}

	// Fase C: nuevo watcher debe auto-recuperarse.
	var mu sync.Mutex
	var received []Event
	handler := func(ctx context.Context, event Event) error {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}
	w := NewWatcher(h.client, h.tokenStore, handler, DefaultWatcherConfig())

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		_ = w.Run(ctx)
	}()

	// 1) El watcher debe darse cuenta solo: eliminar el token inválido.
	deadline := time.After(8 * time.Second)
	for {
		cur, err := h.tokenStore.Get(ctx)
		if err != nil {
			t.Fatalf("tokenStore.Get: %v", err)
		}
		if cur == "" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("el watcher no descartó el resume token inválido en 8s")
		case <-time.After(200 * time.Millisecond):
		}
	}

	// 2) El stream reiniciado fresco debe seguir procesando eventos.
	time.Sleep(500 * time.Millisecond)

	docB := domain.NewDocument("corr-cs-heal-b", time.Now().UTC().Add(24*time.Hour))
	docB.SetObjectKey("raw-pdfs")
	if err := h.repo.Insert(ctx, docB); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}
	ok, err = h.repo.UpdateStatus(ctx, docB.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": docB.ObjectKey},
	)
	if err != nil || !ok {
		t.Fatalf("UpdateStatus falló: ok=%v err=%v", ok, err)
	}

	deadline = time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("el watcher no procesó eventos tras la auto-recuperación")
		case <-time.After(100 * time.Millisecond):
		}
	}

	mu.Lock()
	got := received[0]
	mu.Unlock()
	if got.DocumentID != docB.ID || got.Status != "UPLOADED" {
		t.Errorf("evento recibido inesperado: %+v", got)
	}

	// 3) A partir de acá el resume token se persiste de nuevo (reanudación sana).
	time.Sleep(500 * time.Millisecond)
	token, err = h.tokenStore.Get(ctx)
	if err != nil {
		t.Fatalf("tokenStore.Get tras heal: %v", err)
	}
	if token == "" {
		t.Error("esperaba un resume token persistido tras el heal")
	}
}

// TestWatcher_ClienteSinTimeout_SuperaVentanaDeOperacion verifica que el
// watcher construido con un cliente SIN Timeout de cliente (Timeout=0,
// igual que cmd/api/main.go) mantenga el stream vivo y entregue eventos
// que ocurren después de la ventana típica de timeout de operación.
//
// Contexto: el driver v2 (csot) aplica ClientOptions.Timeout como deadline
// de toda operación, incluidos los getMore del change stream. Con el
// Timeout de 30s de DefaultPoolConfig el stream moría exactamente a los
// 30s, sin resume token persistido (nunca llegaba un evento), y entraba en
// un ciclo reinicio→muerte que perdía eventos del relay del SAGA
// (verificado en smoke test end-to-end). Este test fija la construcción
// correcta: cliente propio con Timeout=0.
func TestWatcher_ClienteSinTimeout_SuperaVentanaDeOperacion(t *testing.T) {
	h, ctx, cleanup := setupHarness(t)
	defer cleanup()

	// Cliente dedicado al watcher con Timeout=0 (patrón de main.go).
	pool := mongo.DefaultPoolConfig()
	pool.Timeout = 0
	watcherClient, err := mongo.NewClient(ctx, testMongoURI(t), h.dbName, pool)
	if err != nil {
		t.Fatalf("cliente del watcher: %v", err)
	}
	defer func() { _ = watcherClient.Close(context.Background()) }()

	tokenStore := mongo.NewResumeTokenStore(watcherClient)

	var mu sync.Mutex
	var received []Event
	handler := func(ctx context.Context, event Event) error {
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		return nil
	}

	w := NewWatcher(watcherClient, tokenStore, handler, DefaultWatcherConfig())
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		_ = w.Run(ctx)
	}()

	// Ventana deliberadamente mayor que un timeout de operación corto:
	// si el cliente llevara Timeout activo, el stream habría muerto y
	// reiniciado sin resume token, perdiendo el evento de abajo.
	time.Sleep(6 * time.Second)

	doc := domain.NewDocument("corr-cs-notimeout", time.Now().UTC().Add(24*time.Hour))
	doc.SetObjectKey("raw-pdfs")
	if err := h.repo.Insert(ctx, doc); err != nil {
		t.Fatalf("Insert falló: %v", err)
	}
	ok, err := h.repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)
	if err != nil || !ok {
		t.Fatalf("UpdateStatus falló: ok=%v err=%v", ok, err)
	}

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("el watcher no entregó el evento UPLOADED: stream murió por Timeout de cliente reintroducido")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

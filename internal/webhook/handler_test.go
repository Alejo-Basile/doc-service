package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// mockRepo implementa ports.DocumentRepository para tests.
type mockRepo struct {
	docs         map[string]*domain.Document
	updateCalls  []updateCall
	historyCalls []historyCall
}

type updateCall struct {
	id   string
	from domain.Status
	to   domain.Status
}

type historyCall struct {
	id    string
	to    domain.Status
	entry domain.StatusEntry
}

func newMockRepo() *mockRepo {
	return &mockRepo{docs: make(map[string]*domain.Document)}
}

func (m *mockRepo) Insert(ctx context.Context, doc *domain.Document) error {
	m.docs[doc.ID] = doc
	return nil
}

func (m *mockRepo) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	doc, ok := m.docs[id]
	if !ok {
		return nil, ports.ErrNotFound
	}
	return doc, nil
}

func (m *mockRepo) UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error) {
	m.updateCalls = append(m.updateCalls, updateCall{id: id, from: from, to: to})
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	return true, nil
}

func (m *mockRepo) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	var result []*domain.Document
	for _, doc := range m.docs {
		if filter.Status == "" || doc.Status == filter.Status {
			result = append(result, doc)
		}
	}
	return result, int64(len(result)), nil
}

func (m *mockRepo) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	var result []*domain.Document
	for _, doc := range m.docs {
		if filter.Status == "" || doc.Status == filter.Status {
			result = append(result, doc)
		}
	}
	return result, "", nil
}

func (m *mockRepo) UpdateStatusWithHistory(ctx context.Context, id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any) (bool, error) {
	m.updateCalls = append(m.updateCalls, updateCall{id: id, from: from, to: to})
	m.historyCalls = append(m.historyCalls, historyCall{id: id, to: to, entry: entry})
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	doc.History = append(doc.History, entry)
	return true, nil
}

// mockStorage implementa ports.ObjectStorage para tests.
type mockStorage struct {
	pdfHeader []byte
	statErr   error
	rangeErr  error // simula GetRange fallando (p.ej. objeto ya borrado)
}

func (m *mockStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	return &ports.PresignPostResult{}, nil
}

func (m *mockStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	if m.statErr != nil {
		return ports.ObjectInfo{}, m.statErr
	}
	return ports.ObjectInfo{Key: objectKey}, nil
}

func (m *mockStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	if m.rangeErr != nil {
		return nil, m.rangeErr
	}
	if m.pdfHeader != nil {
		return m.pdfHeader, nil
	}
	return []byte("%PDF-"), nil
}

func (m *mockStorage) Delete(ctx context.Context, objectKey string) error { return nil }

func (m *mockStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) PresignGetTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) ListObjects(ctx context.Context) ([]ports.ObjectInfo, error) {
	return nil, nil
}

func newTestEngine(h *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	h.RegisterRoutes(engine)
	return engine
}

func postEvent(engine *gin.Engine, token string, body any) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/internal/storage/events", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Minio-Webhook-Token", token)
	}
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

func TestHandleEvent_ValidToken_TransitionsPendingToUploaded(t *testing.T) {
	repo := newMockRepo()
	doc := &domain.Document{
		ID:     "doc-1",
		Status: domain.StatusPendingUpload,
	}
	repo.docs["doc-1"] = doc

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:CompleteMultipartUpload",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	if len(repo.updateCalls) != 1 {
		t.Fatalf("updateCalls = %d, want 1", len(repo.updateCalls))
	}
	if repo.updateCalls[0].to != domain.StatusUploaded {
		t.Errorf("transition to = %s, want UPLOADED", repo.updateCalls[0].to)
	}
}

// TestHandleEvent_RegistraHistorial verifica que la transición del webhook
// escribe una entrada de historial con actor minio-webhook (S4-P2-04).
func TestHandleEvent_RegistraHistorial(t *testing.T) {
	repo := newMockRepo()
	doc := &domain.Document{ID: "doc-hist", Status: domain.StatusPendingUpload}
	repo.docs["doc-hist"] = doc

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-hist.pdf"},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	if len(repo.historyCalls) != 1 {
		t.Fatalf("historyCalls = %d, want 1", len(repo.historyCalls))
	}
	entry := repo.historyCalls[0].entry
	if entry.Actor != domain.ActorMinIOWebhook {
		t.Errorf("actor = %s, want minio-webhook", entry.Actor)
	}
	if entry.Status != domain.StatusUploaded {
		t.Errorf("status en historial = %s, want UPLOADED", entry.Status)
	}
	if len(doc.History) != 1 || doc.History[0].Actor != domain.ActorMinIOWebhook {
		t.Errorf("historial del documento = %+v, want 1 entrada minio-webhook", doc.History)
	}
}

// TestHandleEvent_NotPDF_RejectedRegistraHistorial verifica que el rechazo
// por no-PDF también deja entrada de historial con la razón (S4-P2-04, S2-P2-02).
func TestHandleEvent_NotPDF_RejectedRegistraHistorial(t *testing.T) {
	repo := newMockRepo()
	doc := &domain.Document{ID: "doc-bad", Status: domain.StatusPendingUpload}
	repo.docs["doc-bad"] = doc

	storage := &mockStorage{pdfHeader: []byte("no soy pdf")}
	h := NewHandler(repo, storage, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-bad.pdf"},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	if len(repo.historyCalls) != 1 {
		t.Fatalf("historyCalls = %d, want 1", len(repo.historyCalls))
	}
	entry := repo.historyCalls[0].entry
	if entry.Actor != domain.ActorMinIOWebhook {
		t.Errorf("actor = %s, want minio-webhook", entry.Actor)
	}
	if entry.Status != domain.StatusRejected {
		t.Errorf("status en historial = %s, want REJECTED", entry.Status)
	}
	if entry.Reason != domain.FailureReasonNotAPDF {
		t.Errorf("reason en historial = %s, want NOT_A_PDF", entry.Reason)
	}
}

func TestHandleEvent_InvalidToken_Unauthorized(t *testing.T) {
	repo := newMockRepo()
	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "wrong-token", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
		},
	})

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestHandleEvent_WrongBucket_Ignored(t *testing.T) {
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "other-bucket"},
			"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ignored" {
		t.Errorf("status = %v, want ignored", resp["status"])
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (bucket wrong)", len(repo.updateCalls))
	}
}

func TestHandleEvent_WrongPrefix_Ignored(t *testing.T) {
	repo := newMockRepo()
	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "extracted-txt/doc-1.txt"},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ignored" {
		t.Errorf("status = %v, want ignored", resp["status"])
	}
}

func TestHandleEvent_DocumentNotPending_NoTransition(t *testing.T) {
	repo := newMockRepo()
	// Documento ya en estado QUEUED (no PENDING_UPLOAD).
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusQueued}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "no_transition" {
		t.Errorf("status = %v, want no_transition", resp["status"])
	}
}

func TestHandleEvent_NotPDF_Rejected(t *testing.T) {
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	storage := &mockStorage{pdfHeader: []byte("HELLO")}
	h := NewHandler(repo, storage, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "rejected" {
		t.Errorf("status = %v, want rejected", resp["status"])
	}

	// Regresión (E2E-01): debe haber UNA sola transición, directo de
	// PENDING_UPLOAD a REJECTED. Si hubiera pasado por UPLOADED, el Change Stream
	// habría emitido un UPLOADED transitorio y el relay habría encolado el objeto.
	if len(repo.updateCalls) != 1 {
		t.Fatalf("updateCalls = %d, want 1 (sin pasar por UPLOADED); calls=%+v",
			len(repo.updateCalls), repo.updateCalls)
	}
	if repo.updateCalls[0].from != domain.StatusPendingUpload ||
		repo.updateCalls[0].to != domain.StatusRejected {
		t.Errorf("transición = %s→%s, want PENDING_UPLOAD→REJECTED",
			repo.updateCalls[0].from, repo.updateCalls[0].to)
	}
}

// TestHandleEvent_ObjectAlreadyDeleted_NoPromociona verifica S3-P2-08: si el
// objeto ya no existe cuando llega el evento, el handler no debe promocionar el
// documento a UPLOADED (dejaría un documento fantasma). Devuelve 5xx para que
// MinIO reintente y, persistente el fallo, el reconciliador lo lleve a estado
// terminal (UPLOAD_EXPIRED) sin que quede colgado en UPLOADED.
func TestHandleEvent_ObjectAlreadyDeleted_NoPromociona(t *testing.T) {
	repo := newMockRepo()
	doc := &domain.Document{ID: "doc-gone", Status: domain.StatusPendingUpload}
	repo.docs["doc-gone"] = doc

	storage := &mockStorage{rangeErr: errObjectMissing}
	h := NewHandler(repo, storage, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-gone.pdf"},
		},
	})

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 (objeto ilegible)", w.Code)
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (no debe transicionar)", len(repo.updateCalls))
	}
	if doc.Status != domain.StatusPendingUpload {
		t.Errorf("estado = %s, want PENDING_UPLOAD (el reconciliador lo expira)", doc.Status)
	}
}

// errObjectMissing simula el error del SDK al leer un objeto inexistente.
var errObjectMissing = &objectMissingError{}

type objectMissingError struct{}

func (*objectMissingError) Error() string { return "The specified key does not exist" }

// --- Ventana anti-replay (S3-P2-06) ---

// eventTimePayload arma un evento ObjectCreated con Records[].eventTime.
func eventTimePayload(eventTime string) map[string]any {
	return map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"Records": []map[string]any{
			{
				"eventName": "s3:ObjectCreated:Put",
				"eventTime": eventTime,
				"s3": map[string]any{
					"bucket": map[string]any{"name": "raw-pdfs"},
					"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
				},
			},
		},
	}
}

func TestHandleEvent_EventoReciente_Aceptado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/",
		WithClock(func() time.Time { return now }),
		WithReplayWindow(10*time.Minute),
	)
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", eventTimePayload(now.Add(-1*time.Minute).Format(time.RFC3339Nano)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (evento dentro de la ventana); body=%s", w.Code, w.Body.String())
	}
}

func TestHandleEvent_EventoViejo_Rechazado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/",
		WithClock(func() time.Time { return now }),
		WithReplayWindow(10*time.Minute),
	)
	engine := newTestEngine(h)

	// Evento de hace 1h: por fuera de la ventana (posible replay).
	w := postEvent(engine, "secret123", eventTimePayload(now.Add(-time.Hour).Format(time.RFC3339Nano)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (evento viejo); body=%s", w.Code, w.Body.String())
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (no debe transicionar)", len(repo.updateCalls))
	}
}

func TestHandleEvent_EventoFuturo_Rechazado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/",
		WithClock(func() time.Time { return now }),
		WithReplayWindow(10*time.Minute),
	)
	engine := newTestEngine(h)

	// Evento 1h en el futuro: fuera de la ventana (reloj adelantado o forjado).
	w := postEvent(engine, "secret123", eventTimePayload(now.Add(time.Hour).Format(time.RFC3339Nano)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (evento futuro); body=%s", w.Code, w.Body.String())
	}
}

func TestHandleEvent_SinEventTime_Aceptado(t *testing.T) {
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	// Payload sin eventTime (emisor no estándar): se acepta para no romper.
	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"s3": map[string]any{
			"bucket": map[string]any{"name": "raw-pdfs"},
			"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (sin eventTime se acepta); body=%s", w.Code, w.Body.String())
	}
}

func TestWithinReplayWindow(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	h := NewHandler(newMockRepo(), &mockStorage{}, "s", "raw-pdfs", "raw-pdfs/",
		WithClock(func() time.Time { return now }),
		WithReplayWindow(5*time.Minute),
	)

	cases := []struct {
		name      string
		eventTime string
		want      bool
	}{
		{"vacío se acepta", "", true},
		{"no parseable se acepta", "no-es-fecha", true},
		{"justo en ventana", now.Add(-5 * time.Minute).Format(time.RFC3339Nano), true},
		{"justo fuera", now.Add(-5*time.Minute - time.Second).Format(time.RFC3339Nano), false},
		{"futuro dentro de skew", now.Add(2 * time.Minute).Format(time.RFC3339Nano), true},
		{"futuro excesivo", now.Add(6 * time.Minute).Format(time.RFC3339Nano), false},
	}
	for _, tc := range cases {
		if got := h.withinReplayWindow(tc.eventTime); got != tc.want {
			t.Errorf("%s: withinReplayWindow(%q) = %v, want %v", tc.name, tc.eventTime, got, tc.want)
		}
	}
}

func TestExtractDocumentID(t *testing.T) {
	h := &Handler{keyPrefix: "raw-pdfs/"}

	tests := []struct {
		key  string
		want string
	}{
		{"raw-pdfs/doc-1.pdf", "doc-1"},
		{"raw-pdfs/abc-123.pdf", "abc-123"},
		{"raw-pdfs/nested/doc-1.pdf", "doc-1"},
	}

	for _, tt := range tests {
		got := h.extractDocumentID(tt.key)
		if got != tt.want {
			t.Errorf("extractDocumentID(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestIsPDF(t *testing.T) {
	tests := []struct {
		data []byte
		want bool
	}{
		{[]byte("%PDF-1.7"), true},
		{[]byte("%PDF-"), true},
		{[]byte("HELLO"), false},
		{[]byte("%PD"), false}, // muy corto
		{[]byte{}, false},
	}

	for _, tt := range tests {
		if got := isPDF(tt.data); got != tt.want {
			t.Errorf("isPDF(%q) = %v, want %v", tt.data, got, tt.want)
		}
	}
}

func TestVerifyToken_ConstantTime(t *testing.T) {
	h := &Handler{webhookSecret: "secret123"}

	if !h.verifyToken("secret123") {
		t.Error("verifyToken(correct) = false, want true")
	}
	if h.verifyToken("wrong") {
		t.Error("verifyToken(wrong) = true, want false")
	}
	if h.verifyToken("") {
		t.Error("verifyToken(empty) = true, want false")
	}

	// Sin secreto configurado: siempre rechazar.
	empty := &Handler{webhookSecret: ""}
	if empty.verifyToken("anything") {
		t.Error("verifyToken without secret = true, want false")
	}
}

func TestHandleEvent_RealMinIOPayload_RecordsFormat_Transitions(t *testing.T) {
	repo := newMockRepo()
	repo.docs["doc-1"] = &domain.Document{ID: "doc-1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	// Payload real de MinIO: s3 anidado dentro de Records, sin s3 en la raiz.
	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"Records": []map[string]any{
			{
				"eventName": "s3:ObjectCreated:Put",
				"s3": map[string]any{
					"bucket": map[string]any{"name": "raw-pdfs"},
					"object": map[string]any{"key": "raw-pdfs/doc-1.pdf"},
				},
			},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if len(repo.updateCalls) != 1 {
		t.Fatalf("updateCalls = %d, want 1", len(repo.updateCalls))
	}
	if repo.updateCalls[0].to != domain.StatusUploaded {
		t.Errorf("transition to = %s, want UPLOADED", repo.updateCalls[0].to)
	}
}

func TestHandleEvent_RecordsPayloadURLEncodedKey_Unescapes(t *testing.T) {
	repo := newMockRepo()
	repo.docs["doc 1"] = &domain.Document{ID: "doc 1", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	w := postEvent(engine, "secret123", map[string]any{
		"EventName": "s3:ObjectCreated:Put",
		"Records": []map[string]any{
			{
				"s3": map[string]any{
					"bucket": map[string]any{"name": "raw-pdfs"},
					"object": map[string]any{"key": "raw-pdfs/doc+1.pdf"},
				},
			},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	// QueryUnescape convierte '+' a espacio en application/x-www-form-urlencoded.
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "ok" && resp["status"] != "no_transition" {
		t.Errorf("status = %v, want ok/no_transition", resp["status"])
	}
}

func TestHandleEvent_RecordsPayload_EventNameOnlyInRecord_Transitions(t *testing.T) {
	repo := newMockRepo()
	repo.docs["doc-2"] = &domain.Document{ID: "doc-2", Status: domain.StatusPendingUpload}

	h := NewHandler(repo, &mockStorage{}, "secret123", "raw-pdfs", "raw-pdfs/")
	engine := newTestEngine(h)

	// Sin EventName en la raiz: solo en el record (formato AWS estandar).
	w := postEvent(engine, "secret123", map[string]any{
		"Records": []map[string]any{
			{
				"eventName": "s3:ObjectCreated:Put",
				"s3": map[string]any{
					"bucket": map[string]any{"name": "raw-pdfs"},
					"object": map[string]any{"key": "raw-pdfs/doc-2.pdf"},
				},
			},
		},
	})

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	if len(repo.updateCalls) != 1 {
		t.Fatalf("updateCalls = %d, want 1", len(repo.updateCalls))
	}
}

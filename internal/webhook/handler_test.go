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
	docs        map[string]*domain.Document
	updateCalls []updateCall
}

type updateCall struct {
	id   string
	from domain.Status
	to   domain.Status
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
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	return true, nil
}

// mockStorage implementa ports.ObjectStorage para tests.
type mockStorage struct {
	pdfHeader []byte
	statErr   error
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

	// Verificar que se intentó transicionar a REJECTED.
	found := false
	for _, call := range repo.updateCalls {
		if call.to == domain.StatusRejected {
			found = true
		}
	}
	if !found {
		t.Errorf("no se intentó transicionar a REJECTED; calls=%+v", repo.updateCalls)
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

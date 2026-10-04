package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/pdfsvc"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// --- mocks ---

type mockRepo struct {
	docs map[string]*domain.Document
}

func newMockRepo() *mockRepo {
	return &mockRepo{docs: make(map[string]*domain.Document)}
}

func (m *mockRepo) Insert(ctx context.Context, doc *domain.Document) error {
	if _, exists := m.docs[doc.ID]; exists {
		return fmt.Errorf("duplicate key: documento %s ya existe", doc.ID)
	}
	m.docs[doc.ID] = doc
	return nil
}

func (m *mockRepo) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	doc, ok := m.docs[id]
	if !ok {
		return nil, fmt.Errorf("documento %s: %w", id, ports.ErrNotFound)
	}
	return doc, nil
}

func (m *mockRepo) UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error) {
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	return true, nil
}

func (m *mockRepo) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	var docs []*domain.Document
	for _, d := range m.docs {
		docs = append(docs, d)
	}
	return docs, int64(len(docs)), nil
}

// mockStorage implementa ports.ObjectStorage.
type mockStorage struct{}

func (m *mockStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	return &ports.PresignPostResult{
		UploadURL: "https://s3.example.com/upload/" + objectKey,
		Fields: map[string]string{
			"key":    objectKey,
			"bucket": opts.Bucket,
		},
		ExpiresIn: opts.Expiration,
	}, nil
}

func (m *mockStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	return ports.ObjectInfo{}, nil
}

func (m *mockStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	return []byte("%PDF-1.7"), nil
}

func (m *mockStorage) Delete(ctx context.Context, objectKey string) error { return nil }

func (m *mockStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

// --- helpers ---

func setupTestServer(t *testing.T) (*gin.Engine, *mockRepo) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	repo := newMockRepo()
	storage := &mockStorage{}
	validator := pdfsvc.NewValidator(storage)

	handler := NewDocumentHandler(repo, storage, validator, 25*1024*1024, 30*time.Minute)

	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Set("correlation_id", "test-corr-123")
		c.Next()
	})
	handler.RegisterRoutes(engine)

	return engine, repo
}

// --- tests ---

func TestCreateDocument_Nuevo(t *testing.T) {
	engine, _ := setupTestServer(t)

	body := `{"size_bytes": 1024}`
	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status esperado 201, got %d (body: %s)", w.Code, w.Body.String())
	}

	var resp CreateDocumentResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}

	if resp.DocumentID == "" {
		t.Error("DocumentID no puede estar vacío")
	}
	if resp.Status != "PENDING_UPLOAD" {
		t.Errorf("Status esperado PENDING_UPLOAD, got %s", resp.Status)
	}
	if resp.Method != "POST" {
		t.Errorf("Method esperado POST, got %s", resp.Method)
	}
	if resp.UploadURL == "" {
		t.Error("UploadURL no puede estar vacía")
	}
	if resp.ExpiresIn <= 0 {
		t.Errorf("ExpiresIn debe ser positivo, got %d", resp.ExpiresIn)
	}
}

func TestCreateDocument_SizeExcedeMaximo(t *testing.T) {
	engine, _ := setupTestServer(t)

	// 30 MB > 25 MB máximo.
	body := `{"size_bytes": 31457280}`
	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status esperado 400, got %d", w.Code)
	}

	var problem map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &problem); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if problem["title"] != "Error de validación" {
		t.Errorf("title esperado 'Error de validación', got %v", problem["title"])
	}
}

func TestCreateDocument_Idempotente(t *testing.T) {
	engine, repo := setupTestServer(t)

	// Primer request con idempotency key.
	body := `{"idempotency_key": "my-idem-key-1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("primer request: status esperado 201, got %d", w.Code)
	}

	var resp1 CreateDocumentResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp1); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if resp1.DocumentID != "my-idem-key-1" {
		t.Errorf("DocumentID esperado 'my-idem-key-1', got %s", resp1.DocumentID)
	}

	// Segundo request con la misma clave: debe devolver 200 con el mismo documento.
	req2 := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	engine.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("segundo request: status esperado 200, got %d", w2.Code)
	}

	var resp2 CreateDocumentResponse
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if resp2.DocumentID != resp1.DocumentID {
		t.Errorf("DocumentID debe ser idéntico: %s vs %s", resp1.DocumentID, resp2.DocumentID)
	}

	// Solo debe haber 1 documento en el repo.
	if len(repo.docs) != 1 {
		t.Errorf("repo debía tener 1 documento, got %d", len(repo.docs))
	}
}

func TestCreateDocument_BodyInvalido(t *testing.T) {
	engine, _ := setupTestServer(t)

	body := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status esperado 400, got %d", w.Code)
	}
}

func TestGetDocument_Existe(t *testing.T) {
	engine, repo := setupTestServer(t)

	// Crear un documento directamente en el repo.
	doc := domain.NewDocument("corr-get-1", time.Now().UTC().Add(30*time.Minute))
	doc.SetObjectKey("raw-pdfs")
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID, nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}

	if resp["document_id"] != doc.ID {
		t.Errorf("document_id esperado %q, got %v", doc.ID, resp["document_id"])
	}
	if resp["status"] != "PENDING_UPLOAD" {
		t.Errorf("status esperado PENDING_UPLOAD, got %v", resp["status"])
	}
	if resp["correlation_id"] != "corr-get-1" {
		t.Errorf("correlation_id esperado 'corr-get-1', got %v", resp["correlation_id"])
	}
}

func TestGetDocument_NoExiste(t *testing.T) {
	engine, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/no-existe", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status esperado 404, got %d", w.Code)
	}

	var problem map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &problem); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if problem["title"] != "Recurso no encontrado" {
		t.Errorf("title esperado 'Recurso no encontrado', got %v", problem["title"])
	}
}

func TestGetDocument_ConCampos(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc := domain.NewDocument("corr-campos", time.Now().UTC().Add(30*time.Minute))
	doc.SetObjectKey("raw-pdfs")
	doc.TxtRef = "extracted-txt/test.txt"
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID, nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}

	// Campos que deben estar presentes.
	campos := []string{"document_id", "status", "object_key", "txt_ref", "correlation_id", "schema_version", "failure_reason", "created_at", "updated_at", "expires_at"}
	for _, c := range campos {
		if _, ok := resp[c]; !ok {
			t.Errorf("campo %q no encontrado en la respuesta", c)
		}
	}
}

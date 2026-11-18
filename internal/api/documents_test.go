package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/config"
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
		return fmt.Errorf("documento %s: %w", doc.ID, ports.ErrDuplicate)
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
	for k, v := range fields {
		switch k {
		case "expires_at":
			if t, ok := v.(time.Time); ok {
				doc.ExpiresAt = t
			}
		}
	}
	doc.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (m *mockRepo) UpdateStatusWithHistory(ctx context.Context, id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any) (bool, error) {
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	entry.At = time.Now().UTC()
	doc.History = append(doc.History, entry)
	if entry.Reason != "" && (to == domain.StatusFailed || to == domain.StatusRejected || to == domain.StatusExtractionFailed || to == domain.StatusUploadExpired) {
		doc.FailureReason = entry.Reason
	}
	for k, v := range extraSet {
		if k == "failure_reason" {
			doc.FailureReason = fmt.Sprintf("%v", v)
		}
	}
	return true, nil
}

func (m *mockRepo) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	var docs []*domain.Document
	for _, d := range m.docs {
		docs = append(docs, d)
	}
	return docs, int64(len(docs)), nil
}

func (m *mockRepo) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	// Ordenar por created_at descendente (suficiente para tests).
	var docs []*domain.Document
	for _, d := range m.docs {
		if filter.Status != "" && d.Status != filter.Status {
			continue
		}
		docs = append(docs, d)
	}
	// Orden simple por created_at desc.
	for i := 0; i < len(docs); i++ {
		for j := i + 1; j < len(docs); j++ {
			if docs[j].CreatedAt.After(docs[i].CreatedAt) {
				docs[i], docs[j] = docs[j], docs[i]
			}
		}
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if int64(len(docs)) > limit {
		docs = docs[:limit]
		return docs, "mock-cursor", nil
	}
	return docs, "", nil
}

// mockStorage implementa ports.ObjectStorage.
type mockStorage struct {
	lastPostOpts        ports.PresignPostOptions
	lastGetExpiry       time.Duration
	lastLegacyGetExpiry time.Duration
}

func (m *mockStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	m.lastPostOpts = opts
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

func (m *mockStorage) StatMD(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	return ports.ObjectInfo{}, nil
}

func (m *mockStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	return []byte("%PDF-1.7"), nil
}

func (m *mockStorage) Delete(ctx context.Context, objectKey string) error { return nil }

func (m *mockStorage) DeleteMD(ctx context.Context, objectKey string) error { return nil }

func (m *mockStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) PresignGetMD(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	m.lastGetExpiry = expiry
	return "https://s3.example.com/extracted-md/" + objectKey, nil
}

func (m *mockStorage) StatLegacyTXT(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	return ports.ObjectInfo{}, nil
}

func (m *mockStorage) PresignGetLegacyTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	m.lastLegacyGetExpiry = expiry
	return "https://s3.example.com/extracted-txt/" + objectKey, nil
}

func (m *mockStorage) ListObjects(ctx context.Context) ([]ports.ObjectInfo, error) {
	return nil, nil
}

func (m *mockStorage) ListObjectsMD(ctx context.Context) ([]ports.ObjectInfo, error) {
	return nil, nil
}

// --- helpers ---

type fakeLiveConfig struct{ ptr atomic.Pointer[config.Config] }

func (f *fakeLiveConfig) CurrentConfig() *config.Config { return f.ptr.Load() }
func (f *fakeLiveConfig) set(cfg *config.Config)        { f.ptr.Store(cfg) }

func setupTestServerWithConfig(t *testing.T) (*gin.Engine, *mockRepo, *mockStorage, *fakeLiveConfig) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	repo := newMockRepo()
	storage := &mockStorage{}
	validator := pdfsvc.NewValidator(storage)

	live := &fakeLiveConfig{}
	live.set(&config.Config{MaxPDFBytes: 25 * 1024 * 1024, PresignUploadTTL: 15 * time.Minute, PresignDownloadTTL: 5 * time.Minute})
	handler := NewDocumentHandler(repo, storage, validator, live, 30*time.Minute, "raw-pdfs")

	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Set("correlation_id", "test-corr-123")
		c.Next()
	})
	handler.RegisterRoutes(engine)
	return engine, repo, storage, live
}

func setupTestServer(t *testing.T) (*gin.Engine, *mockRepo) {
	t.Helper()
	engine, repo, _, _ := setupTestServerWithConfig(t)
	return engine, repo
}

// --- tests ---

func TestCreateDocument_Nuevo(t *testing.T) {
	engine, repo := setupTestServer(t)

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

	// S5-P2-04: la creación abre la timeline con actor "client" tanto con
	// idempotency key como sin ella (antes variaba a "doc-service").
	doc, err := repo.GetByID(context.Background(), resp.DocumentID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if len(doc.History) != 1 {
		t.Fatalf("History = %d entradas, want 1", len(doc.History))
	}
	if doc.History[0].Actor != domain.ActorClient {
		t.Errorf("History[0].Actor = %s, want client", doc.History[0].Actor)
	}
}

func TestCreateDocument_ExpiresAtVentanaMasGracia(t *testing.T) {
	engine, repo := setupTestServer(t)
	before := time.Now().UTC()
	request := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	after := time.Now().UTC()

	if response.Code != http.StatusCreated {
		t.Fatalf("status esperado 201, got %d (body: %s)", response.Code, response.Body.String())
	}
	var resp CreateDocumentResponse
	if err := json.Unmarshal(response.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	doc, err := repo.GetByID(context.Background(), resp.DocumentID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	grace := 30 * time.Minute
	if doc.ExpiresAt.Before(before.Add(15*time.Minute+grace-time.Second)) || doc.ExpiresAt.After(after.Add(15*time.Minute+grace+time.Second)) {
		t.Errorf("ExpiresAt = %s, want between request bounds plus upload window and grace", doc.ExpiresAt)
	}
	if resp.ExpiresIn != int((15 * time.Minute).Seconds()) {
		t.Errorf("ExpiresIn = %d, want %d", resp.ExpiresIn, int((15 * time.Minute).Seconds()))
	}
}

func TestCreateDocument_IdempotenteExtiendeExpiresAt(t *testing.T) {
	engine, repo := setupTestServer(t)
	body := `{"idempotency_key":"extend-expiry"}`
	post := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		engine.ServeHTTP(response, request)
		return response
	}
	first := post()
	if first.Code != http.StatusCreated {
		t.Fatalf("primer request: status esperado 201, got %d", first.Code)
	}
	doc := repo.docs["extend-expiry"]
	doc.ExpiresAt = time.Now().UTC().Add(time.Minute)

	before := time.Now().UTC()
	second := post()
	after := time.Now().UTC()
	if second.Code != http.StatusOK {
		t.Fatalf("segundo request: status esperado 200, got %d (body: %s)", second.Code, second.Body.String())
	}
	expectedLower := before.Add(15*time.Minute + 30*time.Minute - time.Second)
	expectedUpper := after.Add(15*time.Minute + 30*time.Minute + time.Second)
	if doc.ExpiresAt.Before(expectedLower) || doc.ExpiresAt.After(expectedUpper) {
		t.Errorf("stored ExpiresAt = %s, want approximately now + upload window + grace", doc.ExpiresAt)
	}
	var resp CreateDocumentResponse
	if err := json.Unmarshal(second.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if resp.ExpiresAt != doc.ExpiresAt.Format(time.RFC3339) {
		t.Errorf("response expires_at = %s, stored value = %s", resp.ExpiresAt, doc.ExpiresAt.Format(time.RFC3339))
	}
}

func TestCreateDocument_IdempotenteNoExtiendeTerminal(t *testing.T) {
	engine, repo := setupTestServer(t)
	body := `{"idempotency_key":"terminal-expiry"}`
	request := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	first := httptest.NewRecorder()
	engine.ServeHTTP(first, request)
	if first.Code != http.StatusCreated {
		t.Fatalf("primer request: status esperado 201, got %d", first.Code)
	}
	doc := repo.docs["terminal-expiry"]
	doc.Status = domain.StatusCompleted
	fixedExpiry := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	doc.ExpiresAt = fixedExpiry

	request = httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	second := httptest.NewRecorder()
	engine.ServeHTTP(second, request)
	if second.Code != http.StatusOK {
		t.Fatalf("segundo request: status esperado 200, got %d", second.Code)
	}
	if !doc.ExpiresAt.Equal(fixedExpiry) {
		t.Errorf("terminal ExpiresAt changed to %s, want %s", doc.ExpiresAt, fixedExpiry)
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
	doc.MdRef = "extracted-md/test.md"
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
	campos := []string{"document_id", "status", "object_key", "md_ref", "correlation_id", "schema_version", "failure_reason", "created_at", "updated_at", "expires_at"}
	for _, c := range campos {
		if _, ok := resp[c]; !ok {
			t.Errorf("campo %q no encontrado en la respuesta", c)
		}
	}
}

// --- DownloadDocument (S3-P2-09) ---

func TestDownloadDocument_Completed_OK(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc := domain.NewDocument("corr-dl-1", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusCompleted
	doc.MdRef = "extracted-md/doc-dl-1.md"
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID+"/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d; body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if resp["download_url"] == "" {
		t.Error("download_url vacío")
	}
	if resp["method"] != "GET" {
		t.Errorf("method esperado GET, got %v", resp["method"])
	}
}

func TestDownloadDocument_NoCompleted_Conflict(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc := domain.NewDocument("corr-dl-2", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusUploaded // no COMPLETED
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID+"/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status esperado 409, got %d", w.Code)
	}
}

func TestDownloadDocument_NoExiste_NotFound(t *testing.T) {
	engine, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/no-existe/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status esperado 404, got %d", w.Code)
	}
}

// --- ListDocuments (S3-P2-10) ---

func TestListDocuments_SinCursor(t *testing.T) {
	engine, repo := setupTestServer(t)

	for i := 0; i < 3; i++ {
		doc := domain.NewDocument(fmt.Sprintf("corr-list-%d", i), time.Now().UTC().Add(30*time.Minute))
		doc.CreatedAt = time.Now().UTC().Add(time.Duration(i) * time.Minute)
		_ = repo.Insert(context.Background(), doc)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d", w.Code)
	}

	var resp ListDocumentsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if len(resp.Documents) != 3 {
		t.Errorf("documents = %d, want 3", len(resp.Documents))
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false")
	}
}

func TestListDocuments_ConStatusFilter(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc1 := domain.NewDocument("corr-s1", time.Now().UTC().Add(30*time.Minute))
	doc1.Status = domain.StatusCompleted
	_ = repo.Insert(context.Background(), doc1)

	doc2 := domain.NewDocument("corr-s2", time.Now().UTC().Add(30*time.Minute))
	doc2.Status = domain.StatusPendingUpload
	_ = repo.Insert(context.Background(), doc2)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents?status=COMPLETED", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d", w.Code)
	}

	var resp ListDocumentsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if len(resp.Documents) != 1 {
		t.Errorf("documents = %d, want 1 (solo COMPLETED)", len(resp.Documents))
	}
}

func TestListDocuments_StatusInvalido_BadRequest(t *testing.T) {
	engine, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents?status=INVALIDO", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status esperado 400, got %d", w.Code)
	}
}

// --- Fallback legacy txt_ref (D5, cambiomd Fase 3) ---

func TestDownloadDocument_FallbackTxtRef(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc := domain.NewDocument("corr-fb-1", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusCompleted
	doc.TxtRef = "extracted-txt/doc-fb-1.txt" // solo legacy: sin md_ref
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID+"/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d; body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	url, _ := resp["download_url"].(string)
	if !strings.Contains(url, "/extracted-txt/") {
		t.Errorf("download_url = %q, want bucket legacy /extracted-txt/", url)
	}
}

func TestDownloadDocument_PrefiereMdRef(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc := domain.NewDocument("corr-fb-2", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusCompleted
	doc.MdRef = "extracted-md/doc-fb-2.md"
	doc.TxtRef = "extracted-txt/doc-fb-2.txt" // ambos: md_ref tiene prioridad
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID+"/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d; body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	url, _ := resp["download_url"].(string)
	if !strings.Contains(url, "/extracted-md/") {
		t.Errorf("download_url = %q, want bucket /extracted-md/ (md_ref tiene prioridad)", url)
	}
	if strings.Contains(url, "/extracted-txt/") {
		t.Errorf("download_url = %q, no debe caer al bucket legacy con md_ref presente", url)
	}
}

func TestDownloadDocument_SinReferencias_Conflict(t *testing.T) {
	engine, repo := setupTestServer(t)

	doc := domain.NewDocument("corr-fb-3", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusCompleted
	// ni md_ref ni txt_ref
	_ = repo.Insert(context.Background(), doc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+doc.ID+"/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status esperado 409, got %d; body=%s", w.Code, w.Body.String())
	}
}

// --- C15: configuración viva desde el snapshot de Consul KV ---

func TestCreateDocument_UsaTTLDeSubidaVigente(t *testing.T) {
	engine, _, storage, live := setupTestServerWithConfig(t)
	live.set(&config.Config{
		MaxPDFBytes:        25 * 1024 * 1024,
		PresignUploadTTL:   120 * time.Second,
		PresignDownloadTTL: 90 * time.Second,
	})

	before := time.Now().UTC()
	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{"size_bytes": 1024}`))
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
	if resp.ExpiresIn != 120 {
		t.Errorf("expires_in = %d, want 120", resp.ExpiresIn)
	}
	if storage.lastPostOpts.Expiration != 2*time.Minute {
		t.Errorf("PresignPost.Expiration = %s, want 2m", storage.lastPostOpts.Expiration)
	}

	got, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("expires_at inválido: %v", err)
	}
	want := before.Add(120 * time.Second).Add(30 * time.Minute)
	if delta := got.Sub(want); delta < -5*time.Second || delta > 5*time.Second {
		t.Errorf("expires_at = %s, want ~%s (delta %s)", got, want, delta)
	}
}

func TestCreateDocument_RechazaSobreMaxVigente(t *testing.T) {
	engine, _, _, live := setupTestServerWithConfig(t)
	live.set(&config.Config{
		MaxPDFBytes:        1048576,
		PresignUploadTTL:   15 * time.Minute,
		PresignDownloadTTL: 5 * time.Minute,
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{"size_bytes": 1048577}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status esperado 400, got %d (body: %s)", w.Code, w.Body.String())
	}

	var problem map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &problem); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	want := "size_bytes excede el máximo permitido: 1048576 bytes"
	if problem["detail"] != want {
		t.Errorf("detail = %v, want %q", problem["detail"], want)
	}
}

func TestDownloadDocument_UsaTTLDescargaVigente(t *testing.T) {
	engine, repo, storage, live := setupTestServerWithConfig(t)
	live.set(&config.Config{
		MaxPDFBytes:        25 * 1024 * 1024,
		PresignUploadTTL:   15 * time.Minute,
		PresignDownloadTTL: 90 * time.Second,
	})

	mdDoc := domain.NewDocument("corr-c15-md", time.Now().UTC().Add(30*time.Minute))
	mdDoc.Status = domain.StatusCompleted
	mdDoc.MdRef = "extracted-md/c15-md.md"
	_ = repo.Insert(context.Background(), mdDoc)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+mdDoc.ID+"/download", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d (body: %s)", w.Code, w.Body.String())
	}
	if storage.lastGetExpiry != 90*time.Second {
		t.Errorf("PresignGetMD expiry = %s, want 90s", storage.lastGetExpiry)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if resp["expires_in"] != float64(90) {
		t.Errorf("expires_in = %v, want 90", resp["expires_in"])
	}

	legacyDoc := domain.NewDocument("corr-c15-txt", time.Now().UTC().Add(30*time.Minute))
	legacyDoc.Status = domain.StatusCompleted
	legacyDoc.TxtRef = "extracted-txt/c15-txt.txt"
	_ = repo.Insert(context.Background(), legacyDoc)

	reqLegacy := httptest.NewRequest(http.MethodGet, "/api/v2/documents/"+legacyDoc.ID+"/download", nil)
	wLegacy := httptest.NewRecorder()
	engine.ServeHTTP(wLegacy, reqLegacy)

	if wLegacy.Code != http.StatusOK {
		t.Fatalf("legacy: status esperado 200, got %d (body: %s)", wLegacy.Code, wLegacy.Body.String())
	}
	if storage.lastLegacyGetExpiry != 90*time.Second {
		t.Errorf("PresignGetLegacyTXT expiry = %s, want 90s", storage.lastLegacyGetExpiry)
	}
}

func TestURLYaEmitidaNoCambia(t *testing.T) {
	engine, _, storage, live := setupTestServerWithConfig(t)
	live.set(&config.Config{
		MaxPDFBytes:        25 * 1024 * 1024,
		PresignUploadTTL:   120 * time.Second,
		PresignDownloadTTL: 5 * time.Minute,
	})

	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{"size_bytes": 1024}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status esperado 201, got %d (body: %s)", w.Code, w.Body.String())
	}
	var first CreateDocumentResponse
	if err := json.Unmarshal(w.Body.Bytes(), &first); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	firstExpiration := storage.lastPostOpts.Expiration
	firstFields := make(map[string]string, len(first.Fields))
	for k, v := range first.Fields {
		firstFields[k] = v
	}

	// Sube el TTL vigente: las URLs ya firmadas no se reescriben.
	live.set(&config.Config{
		MaxPDFBytes:        25 * 1024 * 1024,
		PresignUploadTTL:   600 * time.Second,
		PresignDownloadTTL: 5 * time.Minute,
	})

	if first.ExpiresIn != 120 {
		t.Errorf("expires_in de la URL ya emitida = %d, want 120", first.ExpiresIn)
	}
	if firstExpiration != 2*time.Minute {
		t.Errorf("Expiration de la URL ya emitida = %s, want 2m", firstExpiration)
	}
	for k, v := range firstFields {
		if first.Fields[k] != v {
			t.Errorf("campo %q de la URL ya emitida cambió: %q -> %q", k, v, first.Fields[k])
		}
	}

	reqNew := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{"size_bytes": 1024}`))
	reqNew.Header.Set("Content-Type", "application/json")
	wNew := httptest.NewRecorder()
	engine.ServeHTTP(wNew, reqNew)

	if wNew.Code != http.StatusCreated {
		t.Fatalf("nueva URL: status esperado 201, got %d (body: %s)", wNew.Code, wNew.Body.String())
	}
	var next CreateDocumentResponse
	if err := json.Unmarshal(wNew.Body.Bytes(), &next); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if next.ExpiresIn != 600 {
		t.Errorf("expires_in de la URL nueva = %d, want 600", next.ExpiresIn)
	}
	if storage.lastPostOpts.Expiration != 10*time.Minute {
		t.Errorf("Expiration de la URL nueva = %s, want 10m", storage.lastPostOpts.Expiration)
	}
}

func TestCreateDocument_DefaultsPreservanContrato(t *testing.T) {
	engine, _, storage, _ := setupTestServerWithConfig(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{"size_bytes": 1024}`))
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
	if resp.ExpiresIn != 900 {
		t.Errorf("expires_in = %d, want 900", resp.ExpiresIn)
	}
	if storage.lastPostOpts.Expiration != 15*time.Minute {
		t.Errorf("PresignPost.Expiration = %s, want 15m", storage.lastPostOpts.Expiration)
	}

	reqBig := httptest.NewRequest(http.MethodPost, "/api/v2/documents", bytes.NewBufferString(`{"size_bytes": 26214401}`))
	reqBig.Header.Set("Content-Type", "application/json")
	wBig := httptest.NewRecorder()
	engine.ServeHTTP(wBig, reqBig)

	if wBig.Code != http.StatusBadRequest {
		t.Fatalf("status esperado 400, got %d (body: %s)", wBig.Code, wBig.Body.String())
	}
	var problem map[string]any
	if err := json.Unmarshal(wBig.Body.Bytes(), &problem); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	want := "size_bytes excede el máximo permitido: 26214400 bytes"
	if problem["detail"] != want {
		t.Errorf("detail = %v, want %q", problem["detail"], want)
	}
}

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/gin-gonic/gin"
)

const testInternalToken = "internal-test-auth"

func setupInternalTestServer(t *testing.T) (*gin.Engine, *mockRepo) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	repo := newMockRepo()
	storage := &mockStorage{}

	handler := NewInternalHandler(repo, storage, nil, testInternalToken)

	engine := gin.New()
	handler.RegisterRoutes(engine)

	return engine, repo
}

func internalRequest(engine *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", testInternalToken)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	return w
}

// --- Auth ---

func TestInternal_Auth_SinToken_Unauthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := newMockRepo()
	handler := NewInternalHandler(repo, &mockStorage{}, nil, testInternalToken)
	engine := gin.New()
	handler.RegisterRoutes(engine)

	req := httptest.NewRequest(http.MethodGet, "/internal/documents/x/history", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

func TestInternal_Auth_TokenInvalido_Unauthorized(t *testing.T) {
	engine, _ := setupInternalTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/internal/documents/x/history", nil)
	req.Header.Set("X-Internal-Token", "wrong-token")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}

// --- History (S4-P2-04) ---

func TestInternal_History_OK(t *testing.T) {
	engine, repo := setupInternalTestServer(t)

	doc := domain.NewDocument("corr-h1", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusQueued
	_ = repo.Insert(context.Background(), doc)

	w := internalRequest(engine, http.MethodGet, "/internal/documents/"+doc.ID+"/history", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["document_id"] != doc.ID {
		t.Errorf("document_id = %v, want %s", resp["document_id"], doc.ID)
	}
	history, ok := resp["history"].([]any)
	if !ok || len(history) == 0 {
		t.Errorf("history vacío o inválido: %v", resp["history"])
	}
}

func TestInternal_History_NoExiste_NotFound(t *testing.T) {
	engine, _ := setupInternalTestServer(t)

	w := internalRequest(engine, http.MethodGet, "/internal/documents/no-existe/history", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// --- Retry (S4-P2-05) ---

func TestInternal_Retry_ExtractionFailed_OK(t *testing.T) {
	engine, repo := setupInternalTestServer(t)

	doc := domain.NewDocument("corr-r1", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusExtractionFailed
	doc.ObjectKey = "raw-pdfs/corr-r1.pdf"
	_ = repo.Insert(context.Background(), doc)

	w := internalRequest(engine, http.MethodPost, "/internal/documents/"+doc.ID+"/retry", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != string(domain.StatusRetrying) {
		t.Errorf("status = %v, want RETRYING", resp["status"])
	}

	// Verificar que el historial se actualizó.
	updated, _ := repo.GetByID(context.Background(), doc.ID)
	if updated.Status != domain.StatusRetrying {
		t.Errorf("doc.Status = %s, want RETRYING", updated.Status)
	}
	if len(updated.History) < 2 {
		t.Errorf("history = %d entradas, want >= 2", len(updated.History))
	}
}

func TestInternal_Retry_Retrying_Processing(t *testing.T) {
	engine, repo := setupInternalTestServer(t)

	doc := domain.NewDocument("corr-r2", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusRetrying
	_ = repo.Insert(context.Background(), doc)

	w := internalRequest(engine, http.MethodPost, "/internal/documents/"+doc.ID+"/retry", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != string(domain.StatusProcessing) {
		t.Errorf("status = %v, want PROCESSING", resp["status"])
	}
}

func TestInternal_Retry_EstadoInvalido_Conflict(t *testing.T) {
	engine, repo := setupInternalTestServer(t)

	doc := domain.NewDocument("corr-r3", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusCompleted // terminal
	_ = repo.Insert(context.Background(), doc)

	w := internalRequest(engine, http.MethodPost, "/internal/documents/"+doc.ID+"/retry", nil)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

// --- Cancel (S4-P2-05) ---

func TestInternal_Cancel_PendingUpload_OK(t *testing.T) {
	engine, repo := setupInternalTestServer(t)

	doc := domain.NewDocument("corr-c1", time.Now().UTC().Add(30*time.Minute))
	_ = repo.Insert(context.Background(), doc)

	w := internalRequest(engine, http.MethodPost, "/internal/documents/"+doc.ID+"/cancel", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != string(domain.StatusUploadExpired) {
		t.Errorf("status = %v, want UPLOAD_EXPIRED", resp["status"])
	}

	updated, _ := repo.GetByID(context.Background(), doc.ID)
	if updated.Status != domain.StatusUploadExpired {
		t.Errorf("doc.Status = %s, want UPLOAD_EXPIRED", updated.Status)
	}
	if updated.FailureReason != "CANCELLED_BY_OPERATOR" {
		t.Errorf("failure_reason = %q, want CANCELLED_BY_OPERATOR", updated.FailureReason)
	}
}

func TestInternal_Cancel_EstadoInvalido_Conflict(t *testing.T) {
	engine, repo := setupInternalTestServer(t)

	doc := domain.NewDocument("corr-c2", time.Now().UTC().Add(30*time.Minute))
	doc.Status = domain.StatusUploaded // no es PENDING_UPLOAD
	_ = repo.Insert(context.Background(), doc)

	w := internalRequest(engine, http.MethodPost, "/internal/documents/"+doc.ID+"/cancel", nil)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", w.Code)
	}
}

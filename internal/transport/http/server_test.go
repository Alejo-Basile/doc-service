package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz_Responde200(t *testing.T) {
	server := NewServer(false)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	server.Engine().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d", w.Code)
	}

	var payload healthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("respuesta no es JSON válido: %v", err)
	}
	if payload.Status != "healthy" {
		t.Errorf("status esperado healthy, got %q", payload.Status)
	}
	if payload.Service != "doc-service" {
		t.Errorf("service esperado doc-service, got %q", payload.Service)
	}
	if payload.Timestamp == "" {
		t.Error("timestamp no puede estar vacío")
	}
}

func TestReadyz_Responde200(t *testing.T) {
	server := NewServer(false)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	server.Engine().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status esperado 200, got %d", w.Code)
	}

	var payload healthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("respuesta no es JSON válido: %v", err)
	}
	if payload.Status != "ready" {
		t.Errorf("status esperado ready, got %q", payload.Status)
	}
}

func TestRutasDesconocidas_Devuelven404(t *testing.T) {
	server := NewServer(false)

	req := httptest.NewRequest(http.MethodGet, "/no-existe", nil)
	w := httptest.NewRecorder()
	server.Engine().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status esperado 404, got %d", w.Code)
	}
}

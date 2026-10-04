package httpserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/gin-gonic/gin"
)

// newTestEngine crea un Gin engine de prueba con rutas que ejercitan los handlers de error.
func newTestEngine() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())

	engine.GET("/err/validation", func(c *gin.Context) {
		ValidationErrorHandler(c, "campo obligatorio faltante")
	})
	engine.GET("/err/notfound", func(c *gin.Context) {
		NotFoundErrorHandler(c, "documento 123 no existe")
	})
	engine.GET("/err/conflict", func(c *gin.Context) {
		ConflictErrorHandler(c, "documento ya procesado")
	})
	engine.GET("/err/internal", func(c *gin.Context) {
		InternalErrorHandler(c, "fallo inesperado", errTest)
	})
	engine.GET("/err/dependency", func(c *gin.Context) {
		DependencyErrorHandler(c, "mongo no disponible", errTest)
	})
	engine.GET("/err/apperror-directo", func(c *gin.Context) {
		// Los handlers de dominio devuelven *domain.AppError; el transportor lo responde.
		err := domain.NewNotFound("recurso no encontrado")
		respondProblem(c, err)
	})

	return engine
}

var errTest = domain.NewInternal("error de prueba", nil)

func TestProblemResponse_FormatoRFC9457(t *testing.T) {
	engine := newTestEngine()

	req := httptest.NewRequest(http.MethodGet, "/err/validation", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status esperado 400, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type esperado application/json, got %q", ct)
	}

	var payload problemResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("respuesta no es JSON válido: %v", err)
	}

	if payload.Status != 400 {
		t.Errorf("payload.Status esperado 400, got %d", payload.Status)
	}
	if payload.Title != "Error de validación" {
		t.Errorf("payload.Title esperado 'Error de validación', got %q", payload.Title)
	}
	if payload.Detail != "campo obligatorio faltante" {
		t.Errorf("payload.Detail esperado 'campo obligatorio faltante', got %q", payload.Detail)
	}
	if payload.Type != "https://api.doc-service/errors/validation" {
		t.Errorf("payload.Type esperado '.../validation', got %q", payload.Type)
	}
	if payload.Instance != "/err/validation" {
		t.Errorf("payload.Instance esperado '/err/validation', got %q", payload.Instance)
	}
}

func TestProblemResponse_MapeoDeKinds(t *testing.T) {
	casos := []struct {
		ruta           string
		statusEsperado int
	}{
		{"/err/validation", 400},
		{"/err/notfound", 404},
		{"/err/conflict", 409},
		{"/err/internal", 500},
		{"/err/dependency", 503},
	}

	engine := newTestEngine()
	for _, c := range casos {
		req := httptest.NewRequest(http.MethodGet, c.ruta, nil)
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)

		if w.Code != c.statusEsperado {
			t.Errorf("%s: status esperado %d, got %d", c.ruta, c.statusEsperado, w.Code)
		}

		var payload problemResponse
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Errorf("%s: JSON inválido: %v", c.ruta, err)
			continue
		}
		if payload.Status != c.statusEsperado {
			t.Errorf("%s: payload.Status esperado %d, got %d", c.ruta, c.statusEsperado, payload.Status)
		}
	}
}

func TestProblemResponse_CorrelationIDDelContexto(t *testing.T) {
	engine := gin.New()
	engine.GET("/con-corr", func(c *gin.Context) {
		c.Set("correlation_id", "corr-abc-123")
		NotFoundErrorHandler(c, "no existe")
	})

	req := httptest.NewRequest(http.MethodGet, "/con-corr", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	var payload problemResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("JSON inválido: %v", err)
	}
	if payload.CorrelationID != "corr-abc-123" {
		t.Errorf("CorrelationID esperado 'corr-abc-123', got %q", payload.CorrelationID)
	}
}

func TestStatusForKind_MapeoCompleto(t *testing.T) {
	casos := []struct {
		kind     domain.ErrorKind
		esperado int
	}{
		{domain.KindValidation, 400},
		{domain.KindNotFound, 404},
		{domain.KindConflict, 409},
		{domain.KindDependencyUnavailable, 503},
		{domain.KindInternal, 500},
		{domain.ErrorKind("bogus"), 500},
	}

	for _, c := range casos {
		if got := statusForKind(c.kind); got != c.esperado {
			t.Errorf("statusForKind(%q) esperado %d, got %d", c.kind, c.esperado, got)
		}
	}
}

func TestErrorTitles_CoberturaDeTodosLosKinds(t *testing.T) {
	kinds := []domain.ErrorKind{
		domain.KindValidation,
		domain.KindNotFound,
		domain.KindConflict,
		domain.KindDependencyUnavailable,
		domain.KindInternal,
	}
	for _, k := range kinds {
		if _, ok := errorTitles[k]; !ok {
			t.Errorf("falta título para kind %q", k)
		}
	}
}

package httpserver

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// newBufferHandler crea un handler slog que escribe en un buffer para tests.
func newBufferHandler(buf *bytes.Buffer, level slog.Level) slog.Handler {
	return slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})
}

func TestCorrelationIDMiddleware_GeneraIDNuevo(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(CorrelationIDMiddleware())

	var captured string
	engine.GET("/t", func(c *gin.Context) {
		captured = c.GetString("correlation_id")
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if captured == "" {
		t.Fatal("correlation_id no fue inyectado en el contexto")
	}
	if len(captured) != 36 { // UUID v4 = 36 chars
		t.Errorf("correlation_id longitud esperada 36, got %d (%q)", len(captured), captured)
	}
	// El header de respuesta debe contener el mismo ID.
	if got := w.Header().Get(correlationHeader); got != captured {
		t.Errorf("header %s esperado %q, got %q", correlationHeader, captured, got)
	}
}

func TestCorrelationIDMiddleware_RespetoDeIDInbound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(CorrelationIDMiddleware())

	var captured string
	engine.GET("/t", func(c *gin.Context) {
		captured = c.GetString("correlation_id")
		c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	req.Header.Set(correlationHeader, "corr-custom-123")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if captured != "corr-custom-123" {
		t.Errorf("correlation_id esperado 'corr-custom-123', got %q", captured)
	}
	if got := w.Header().Get(correlationHeader); got != "corr-custom-123" {
		t.Errorf("header response esperado 'corr-custom-123', got %q", got)
	}
}

func TestCorrelationIDMiddleware_IDsDistintosPorRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(CorrelationIDMiddleware())

	ids := make([]string, 0, 2)
	engine.GET("/t", func(c *gin.Context) {
		ids = append(ids, c.GetString("correlation_id"))
		c.String(http.StatusOK, "ok")
	})

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/t", nil)
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
	}

	if len(ids) != 2 || ids[0] == ids[1] {
		t.Errorf("cada request debe generar un correlation_id único, got %v", ids)
	}
}

func TestRequestLoggerMiddleware_RegistraConCorrelationID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	buf := &bytes.Buffer{}
	logger := slog.New(newBufferHandler(buf, slog.LevelDebug))
	old := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(old)

	engine := gin.New()
	engine.Use(CorrelationIDMiddleware())
	engine.Use(RequestLoggerMiddleware())

	engine.GET("/t", func(c *gin.Context) {
		c.String(http.StatusCreated, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/t", nil)
	req.Header.Set(correlationHeader, "corr-req-456")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	// Parsear el JSON del log.
	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("log no es JSON válido: %v\nraw: %s", err, buf.String())
	}

	if entry["msg"] != "http_request" {
		t.Errorf("msg esperado 'http_request', got %v", entry["msg"])
	}
	if entry["correlation_id"] != "corr-req-456" {
		t.Errorf("correlation_id esperado 'corr-req-456', got %v", entry["correlation_id"])
	}
	if entry["method"] != "GET" {
		t.Errorf("method esperado 'GET', got %v", entry["method"])
	}
	if entry["path"] != "/t" {
		t.Errorf("path esperado '/t', got %v", entry["path"])
	}
	// status como float64 en JSON
	if statusF, ok := entry["status"].(float64); !ok || int(statusF) != http.StatusCreated {
		t.Errorf("status esperado 201, got %v", entry["status"])
	}
}

func TestRequestLoggerMiddleware_NivelSegunStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)

	casos := []struct {
		ruta          string
		handler       gin.HandlerFunc
		nivelEsperado slog.Level
	}{
		{"/ok", func(c *gin.Context) { c.String(200, "ok") }, slog.LevelInfo},
		{"/warn", func(c *gin.Context) { c.String(404, "nf") }, slog.LevelWarn},
		{"/error", func(c *gin.Context) { c.String(500, "boom") }, slog.LevelError},
	}

	for _, c := range casos {
		buf := &bytes.Buffer{}
		logger := slog.New(newBufferHandler(buf, slog.LevelDebug))
		old := slog.Default()
		slog.SetDefault(logger)

		engine := gin.New()
		engine.Use(CorrelationIDMiddleware())
		engine.Use(RequestLoggerMiddleware())
		engine.GET(c.ruta, c.handler)

		req := httptest.NewRequest(http.MethodGet, c.ruta, nil)
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)

		slog.SetDefault(old)

		var entry map[string]any
		if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
			t.Errorf("%s: JSON inválido: %v", c.ruta, err)
			continue
		}
		if entry["level"] != c.nivelEsperado.String() {
			t.Errorf("%s: level esperado %s, got %v", c.ruta, c.nivelEsperado, entry["level"])
		}
	}
}

func TestErrorLogMiddleware_RegistraErroresDeGin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	buf := &bytes.Buffer{}
	logger := slog.New(newBufferHandler(buf, slog.LevelDebug))
	old := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(old)

	engine := gin.New()
	engine.Use(CorrelationIDMiddleware())
	engine.Use(ErrorLogMiddleware())

	engine.GET("/boom", func(c *gin.Context) {
		_ = c.Error(errTest) // errTest = NewInternal("error de prueba")
		c.String(http.StatusInternalServerError, "boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	req.Header.Set(correlationHeader, "corr-err-789")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("JSON inválido: %v\nraw: %s", err, buf.String())
	}
	if entry["msg"] != "request_error" {
		t.Errorf("msg esperado 'request_error', got %v", entry["msg"])
	}
	if entry["correlation_id"] != "corr-err-789" {
		t.Errorf("correlation_id esperado 'corr-err-789', got %v", entry["correlation_id"])
	}
	if entry["path"] != "/boom" {
		t.Errorf("path esperado '/boom', got %v", entry["path"])
	}
}

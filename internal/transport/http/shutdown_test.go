package httpserver

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// TestGracefulShutdown_DrenaRequestsEnVuelo verifica que http.Server.Shutdown
// no corte abruptamente una request en curso (DoD de S0-P2-04).
func TestGracefulShutdown_DrenaRequestsEnVuelo(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	started := make(chan struct{})

	// Endpoint lento: tarda 500ms, simulando trabajo en vuelo.
	engine.GET("/slow", func(c *gin.Context) {
		close(started)
		time.Sleep(500 * time.Millisecond)
		c.String(http.StatusOK, "completado")
	})

	srv := &http.Server{Handler: engine}

	// Listener manual en puerto aleatorio para conocer la dirección real.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("no se pudo crear el listener: %v", err)
	}
	actualAddr := ln.Addr().String()

	go func() {
		_ = srv.Serve(ln)
	}()

	// Lanzar una request lenta en background.
	type result struct {
		code int
		body string
	}
	resCh := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + actualAddr + "/slow")
		if err != nil {
			resCh <- result{code: 0, body: err.Error()}
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, 64)
		n, _ := resp.Body.Read(buf)
		resCh <- result{code: resp.StatusCode, body: string(buf[:n])}
	}()

	// Esperar a que la request empiece.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("la request lenta no llegó a empezar")
	}

	// Enviar Shutdown mientras la request está en vuelo.
	shutdownDone := make(chan error, 1)
	go func() {
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		shutdownDone <- srv.Shutdown(sctx)
	}()

	// La request en vuelo debe completarse con 200 y body correcto.
	select {
	case res := <-resCh:
		if res.code != http.StatusOK {
			t.Errorf("la request en vuelo debía retornar 200, got %d (%s)", res.code, res.body)
		}
		if res.body != "completado" {
			t.Errorf("body esperado 'completado', got %q", res.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("la request en vuelo no se completó durante el shutdown")
	}

	// Shutdown debe terminar sin error (drenó correctamente).
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Errorf("Shutdown() no debía fallar: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown() no terminó a tiempo")
	}
}

// TestNewHTTPServer_ConfiguraTimeouts verifica que el servidor tenga
// ReadHeaderTimeout para evitar Slowloris.
func TestNewHTTPServer_ConfiguraTimeouts(t *testing.T) {
	s := NewServer(false)
	httpSrv := s.NewHTTPServer(":0")

	if httpSrv.ReadHeaderTimeout == 0 {
		t.Error("ReadHeaderTimeout debe estar configurado (defensa Slowloris)")
	}
	if httpSrv.Handler == nil {
		t.Error("Handler no puede ser nil")
	}
}

// TestHandler_StreamDeErrores sin conexión externa: solo prueba la cadena de
// error vía httptest, sin abrir puertos reales.
func TestHandler_ErroresNoRequierenRed(t *testing.T) {
	engine := newTestEngine()
	req := httptest.NewRequest(http.MethodGet, "/err/internal", nil)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != 500 {
		t.Errorf("status esperado 500, got %d", w.Code)
	}
}

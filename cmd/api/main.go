// Command api es el punto de entrada del microservicio doc-service.
// Inicializa configuración, inyección de dependencias y graceful shutdown.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpserver "github.com/Alejo-Basile/doc-service/internal/transport/http"
)

const shutdownTimeout = 10 * time.Second

func main() {
	// Logger JSON estructurado (S0-P2-03 completará la propagación de correlation_id).
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Configuración básica de arranque (S0-P2-02 agregará validación completa).
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	debug := os.Getenv("DEBUG") == "true"

	slog.Info("iniciando doc-service",
		"service", "doc-service",
		"addr", addr,
		"debug", debug,
	)

	server := httpserver.NewServer(debug)

	// Canal para capturar señales del SO.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine del servidor HTTP.
	errCh := make(chan error, 1)
	go func() {
		if err := server.Run(addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Esperar señal de apagado o error del servidor.
	select {
	case sig := <-quit:
		slog.Info("recibida señal, iniciando graceful shutdown", "signal", sig.String())
	case err := <-errCh:
		slog.Error("el servidor HTTP falló", "error", err)
	}

	// Graceful shutdown con timeout (S0-P2-04 completará el drenaje del relay).
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := shutdownHTTPServer(server, ctx); err != nil {
		slog.Error("error en graceful shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("doc-service detenido correctamente")
}

// shutdownHTTPServer orquesta el apagado ordenado del servidor HTTP.
func shutdownHTTPServer(server *httpserver.Server, ctx context.Context) error {
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: server.Handler(),
	}
	return httpServer.Shutdown(ctx)
}

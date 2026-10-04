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

	"github.com/Alejo-Basile/doc-service/internal/config"
	httpserver "github.com/Alejo-Basile/doc-service/internal/transport/http"
)

const shutdownTimeout = 10 * time.Second

func main() {
	// Logger JSON estructurado.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Fail-fast: si falta una variable obligatoria, abortamos ANTES de abrir puertos.
	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuración inválida, el servicio no puede arrancar",
			"service", "doc-service",
			"error", err.Error(),
		)
		os.Exit(1)
	}

	slog.Info("iniciando doc-service",
		"service", "doc-service",
		"addr", cfg.Addr(),
		"debug", cfg.Debug,
		"mongo_database", cfg.MongoDatabase,
		"minio_bucket_raw", cfg.MinIOBucketRaw,
		"redis_stream_key", cfg.RedisStreamKey,
		"max_pdf_bytes", cfg.MaxPDFBytes,
	)

	server := httpserver.NewServer(cfg.Debug)

	// Canal para capturar señales del SO.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine del servidor HTTP.
	errCh := make(chan error, 1)
	go func() {
		if err := server.Run(cfg.Addr()); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

	if err := shutdownHTTPServer(server, cfg.Addr(), ctx); err != nil {
		slog.Error("error en graceful shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("doc-service detenido correctamente")
}

// shutdownHTTPServer orquesta el apagado ordenado del servidor HTTP.
func shutdownHTTPServer(server *httpserver.Server, addr string, ctx context.Context) error {
	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Handler(),
	}
	return httpServer.Shutdown(ctx)
}

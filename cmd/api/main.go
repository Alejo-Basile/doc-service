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

// shutdownTimeout es el tiempo máximo que se le da al servidor para drenar
// requests en vuelo antes de forzar el corte.
const shutdownTimeout = 15 * time.Second

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
	httpSrv := server.NewHTTPServer(cfg.Addr())

	// Canal para capturar señales del SO (SIGINT y SIGTERM).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine del servidor HTTP.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("servidor HTTP escuchando", "addr", cfg.Addr())
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Esperar señal de apagado o error del servidor.
	select {
	case sig := <-quit:
		slog.Info("recibida señal, iniciando graceful shutdown",
			"signal", sig.String(),
			"timeout", shutdownTimeout.String(),
		)
	case err := <-errCh:
		slog.Error("el servidor HTTP falló", "error", err)
	}

	// Graceful shutdown: http.Server.Shutdown drena las requests en vuelo
	// y espera a que terminen, hasta agotar el timeout.
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpSrv.Shutdown(ctx); err != nil {
		slog.Error("error en graceful shutdown (se agotó el timeout, forzando cierre)",
			"error", err,
		)
		// ForceClose para no quedar procesos zombies si el drenaje se colgó.
		_ = httpSrv.Close()
		os.Exit(1)
	}

	slog.Info("doc-service detenido correctamente")
}

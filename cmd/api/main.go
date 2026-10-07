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

	"github.com/Alejo-Basile/doc-service/internal/adapters/changestream"
	minioadapter "github.com/Alejo-Basile/doc-service/internal/adapters/minio"
	mongoadapter "github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
	redisadapter "github.com/Alejo-Basile/doc-service/internal/adapters/redis"
	"github.com/Alejo-Basile/doc-service/internal/adapters/relay"
	"github.com/Alejo-Basile/doc-service/internal/api"
	"github.com/Alejo-Basile/doc-service/internal/config"
	"github.com/Alejo-Basile/doc-service/internal/pdfsvc"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/Alejo-Basile/doc-service/internal/reconciler"
	httpserver "github.com/Alejo-Basile/doc-service/internal/transport/http"
	"github.com/Alejo-Basile/doc-service/internal/webhook"
)

// shutdownTimeout es el tiempo máximo que se le da al servidor para drenar
// requests en vuelo antes de forzar el corte.
const shutdownTimeout = 15 * time.Second

// startupTimeout acota la fase de arranque (Mongo, migraciones, MinIO, Redis)
// para que una infraestructura caída no cuelgue el proceso: fail-fast.
const startupTimeout = 30 * time.Second

// workSchemaVersion es la versión del contrato del mensaje en la cola
// (SPEC §11.3: payload JSON liviano con schema_version).
const workSchemaVersion = 1

// objectKeyPrefix es el prefijo contractual de las claves de objeto
// (SPEC §11.3: object_key = raw-pdfs/<id>.pdf).
const objectKeyPrefix = "raw-pdfs"

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

	// --- Fase de arranque: dependencias con fail-fast ---
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), startupTimeout)
	defer cancelStartup()

	mongoClient, err := mongoadapter.NewClient(
		startupCtx, cfg.MongoURI, cfg.MongoDatabase, mongoadapter.DefaultPoolConfig(),
	)
	if err != nil {
		slog.Error("no se pudo conectar a MongoDB", "error", err.Error())
		os.Exit(1)
	}

	// Migraciones de esquema idempotentes (S1-P2-05): índices y colecciones.
	if err := mongoadapter.NewMigrator(mongoClient, mongoadapter.DefaultMigrations()...).Up(startupCtx); err != nil {
		slog.Error("las migraciones de Mongo fallaron", "error", err.Error())
		_ = mongoClient.Close(context.Background())
		os.Exit(1)
	}

	storage, err := minioadapter.New(minioadapter.Config{
		Endpoint:       cfg.MinIOEndpoint,
		PublicEndpoint: cfg.MinIOPublicEndpoint,
		AccessKey:      cfg.MinIOAccessKey,
		SecretKey:      cfg.MinIOSecretKey,
		UseSSL:         false, // TLS termina en el edge (Traefik); el endpoint interno es plano.
		BucketRaw:      cfg.MinIOBucketRaw,
		BucketTXT:      cfg.MinIOBucketTXT,
	})
	if err != nil {
		slog.Error("no se pudo inicializar el adaptador MinIO", "error", err.Error())
		_ = mongoClient.Close(context.Background())
		os.Exit(1)
	}

	// --- Adaptadores y puertos ---
	repo := mongoadapter.NewDocumentRepository(mongoClient, "documents")

	queueCfg := redisadapter.DefaultWorkQueueConfig()
	queueCfg.StreamKey = cfg.RedisStreamKey
	queue := redisadapter.NewWorkQueue(cfg.RedisAddr, queueCfg)
	if err := queue.Ping(startupCtx); err != nil {
		slog.Error("no se pudo conectar a Redis", "error", err.Error())
		_ = mongoClient.Close(context.Background())
		os.Exit(1)
	}
	lease := redisadapter.NewLease(cfg.RedisAddr)

	// Cliente EXCLUSIVO del watcher sin timeout de cliente (Timeout=0).
	// El driver v2 (csot) aplica ClientOptions.Timeout como deadline de toda
	// operación: con el Timeout de 30s del pool por defecto, el change stream
	// moría exactamente a los 30s y entraba en un ciclo reinicio/muerte que
	// perdía eventos (sin resume token persistido). Los CRUD usan el otro
	// cliente, que sí mantiene el bound de 30s.
	watcherPool := mongoadapter.DefaultPoolConfig()
	watcherPool.Timeout = 0
	watcherClient, err := mongoadapter.NewClient(startupCtx, cfg.MongoURI, cfg.MongoDatabase, watcherPool)
	if err != nil {
		slog.Error("no se pudo conectar a MongoDB (cliente del watcher)", "error", err.Error())
		_ = mongoClient.Close(context.Background())
		os.Exit(1)
	}
	tokenStore := mongoadapter.NewResumeTokenStore(watcherClient)

	validator := pdfsvc.NewValidator(storage)

	// --- Handlers ---
	docsHandler := api.NewDocumentHandler(repo, storage, validator, cfg.MaxPDFBytes, cfg.DocUploadGrace)
	internalHandler := api.NewInternalHandler(repo, storage, queue, cfg.InternalToken)
	webhookHandler := webhook.NewHandler(repo, storage, cfg.MinIOWebhookSecret, cfg.MinIOBucketRaw, objectKeyPrefix+"/")
	rel := reconciler.New(
		repo, storage, queue, lease, ports.SystemClock{},
		cfg.ReconcileInterval, 30*time.Minute, cfg.MinSafetyAge,
	)

	// --- Servidor HTTP ---
	server := httpserver.NewServer(cfg.Debug)
	server.RegisterAPI(docsHandler, webhookHandler, internalHandler, rel)
	httpSrv := server.NewHTTPServer(cfg.Addr())

	// --- Goroutines de fondo: Change Stream relay + reconciliador ---
	// Se detienen con cancelBackground() después de drenar el HTTP.
	backgroundCtx, cancelBackground := context.WithCancel(context.Background())
	watcher := changestream.NewWatcher(
		watcherClient, tokenStore, relay.New(queue, workSchemaVersion).Handler(), changestream.DefaultWatcherConfig(),
	)
	go func() {
		// Run reintenta internamente con backoff; solo retorna con el
		// contexto cancelado o si el stream termina limpio. En ese caso
		// (p.ej. cierre server-side) reiniciamos: el relay del SAGA no
		// puede quedar muerto en silencio.
		for {
			if err := watcher.Run(backgroundCtx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("watcher terminó con error", "error", err.Error())
			}
			if backgroundCtx.Err() != nil {
				return
			}
			slog.Warn("watcher terminó inesperadamente, reiniciando en 3s")
			select {
			case <-backgroundCtx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
	go rel.Start(backgroundCtx)

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

	// Graceful shutdown en orden: drenar HTTP → detener fondo → cerrar clientes.
	// 1) http.Server.Shutdown drena las requests en vuelo hasta agotar el timeout.
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	exitCode := 0
	if err := httpSrv.Shutdown(ctx); err != nil {
		slog.Error("error en graceful shutdown (se agotó el timeout, forzando cierre)",
			"error", err,
		)
		// ForceClose para no quedar procesos zombies si el drenaje se colgó.
		_ = httpSrv.Close()
		exitCode = 1
	}

	// 2) Detener watcher y reconciliador (contexto cancelado).
	cancelBackground()

	// 3) Cerrar clientes de datos.
	if err := mongoClient.Close(context.Background()); err != nil {
		slog.Error("error cerrando MongoDB", "error", err.Error())
	}
	if err := watcherClient.Close(context.Background()); err != nil {
		slog.Error("error cerrando MongoDB (cliente del watcher)", "error", err.Error())
	}
	if err := queue.Close(); err != nil {
		slog.Error("error cerrando Redis (queue)", "error", err.Error())
	}
	if err := lease.Close(); err != nil {
		slog.Error("error cerrando Redis (lease)", "error", err.Error())
	}

	if exitCode != 0 {
		os.Exit(exitCode)
	}
	slog.Info("doc-service detenido correctamente")
}

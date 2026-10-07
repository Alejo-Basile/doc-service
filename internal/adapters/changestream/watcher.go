// Package changestream implementa el watcher de MongoDB Change Streams.
// Escucha cambios en la colección `documents` y dispara handlers
// cuando un documento pasa a estado UPLOADED (SPEC §3.6, S1-P2-06/07/08).
package changestream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	mongoadapter "github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Event representa un evento del Change Stream filtrado.
type Event struct {
	DocumentID    string
	Status        string
	ObjectKey     string
	CorrelationID string
	SchemaVersion int
	ResumeToken   bson.Raw
}

// Handler procesa un evento del Change Stream.
type Handler func(ctx context.Context, event Event) error

// Watcher escucha el Change Stream de la colección `documents`.
type Watcher struct {
	client       *mongoadapter.Client
	coll         *mongo.Collection
	tokenStore   *mongoadapter.ResumeTokenStore
	handler      Handler
	pollInterval time.Duration
	retryBackoff time.Duration
}

// WatcherConfig configura el Watcher.
type WatcherConfig struct {
	// Collection es el nombre de la colección a observar (default: documents).
	Collection string
	// PollInterval es el intervalo de reconciliación si no hay eventos.
	PollInterval time.Duration
	// RetryBackoff es el backoff base para reintentos tras error.
	RetryBackoff time.Duration
}

// DefaultWatcherConfig retorna la configuración por defecto.
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{
		Collection:   "documents",
		PollInterval: 30 * time.Second,
		RetryBackoff: 1 * time.Second,
	}
}

// NewWatcher crea un Watcher sobre la colección indicada.
func NewWatcher(
	client *mongoadapter.Client,
	tokenStore *mongoadapter.ResumeTokenStore,
	handler Handler,
	cfg WatcherConfig,
) *Watcher {
	return &Watcher{
		client:       client,
		coll:         client.Collection(cfg.Collection),
		tokenStore:   tokenStore,
		handler:      handler,
		pollInterval: cfg.PollInterval,
		retryBackoff: cfg.RetryBackoff,
	}
}

// Run escucha el Change Stream hasta que el contexto se cancela (SIGTERM).
// Reintenta con backoff exponencial y reanuda desde el último token
// persistido (S1-P2-07, S1-P2-08).
func (w *Watcher) Run(ctx context.Context) error {
	backoff := w.retryBackoff

	for {
		// Verificar cancelación antes de cada intento.
		if ctx.Err() != nil {
			slog.Info("watcher detenido por cancelación de contexto")
			return nil
		}

		err := w.watchOnce(ctx)
		if err == nil || ctx.Err() != nil {
			if ctx.Err() != nil {
				slog.Info("watcher detenido (contexto cancelado)")
				return nil
			}
			return nil
		}

		slog.Error("change stream error, reintentando",
			"error", err,
			"backoff", backoff,
		)

		// Backoff exponencial con tope.
		select {
		case <-ctx.Done():
			slog.Info("watcher detenido durante backoff")
			return nil
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// watchOnce ejecuta un ciclo de escucha del Change Stream.
func (w *Watcher) watchOnce(ctx context.Context) error {
	token, err := w.tokenStore.Get(ctx)
	if err != nil {
		return fmt.Errorf("get resume token: %w", err)
	}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(5 * time.Second)

	if token != "" {
		var raw bson.Raw
		if err := bson.UnmarshalExtJSON([]byte(token), true, &raw); err != nil {
			slog.Warn("resume token inválido, empezando desde el inicio", "error", err)
		} else {
			opts.SetResumeAfter(raw)
		}
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"operationType":       "update",
			"fullDocument.status": "UPLOADED",
		}}},
	}

	stream, err := w.coll.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	defer func() {
		_ = stream.Close(ctx)
	}()

	slog.Info("change stream watcher iniciado",
		"collection", w.coll.Name(),
		"resumed", token != "",
	)

	for stream.Next(ctx) {
		var raw bson.M
		if err := stream.Decode(&raw); err != nil {
			slog.Error("decode change stream event", "error", err)
			continue
		}

		event, err := w.parseEvent(raw)
		if err != nil {
			slog.Error("parse change stream event", "error", err)
			continue
		}

		// Persistir resume token ANTES de procesar (S1-P2-07).
		if err := w.tokenStore.Save(ctx, tokenToString(stream.ResumeToken())); err != nil {
			slog.Error("save resume token", "error", err)
		}

		if err := w.handler(ctx, event); err != nil {
			slog.Error("handler error",
				"document_id", event.DocumentID,
				"error", err,
			)
		}
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	return nil
}

// parseEvent extrae los campos relevantes del evento del Change Stream.
func (w *Watcher) parseEvent(raw bson.M) (Event, error) {
	// fullDocument puede venir como bson.M o bson.D según el decoder.
	var fullDoc bson.M
	switch fd := raw["fullDocument"].(type) {
	case bson.M:
		fullDoc = fd
	case bson.D:
		fullDoc = make(bson.M, len(fd))
		for _, e := range fd {
			fullDoc[e.Key] = e.Value
		}
	default:
		return Event{}, fmt.Errorf("fullDocument no es un documento: %T", raw["fullDocument"])
	}

	docID, _ := fullDoc["_id"].(string)
	status, _ := fullDoc["status"].(string)
	objectKey, _ := fullDoc["object_key"].(string)
	corrID, _ := fullDoc["correlation_id"].(string)

	schemaVersion := 0
	switch v := fullDoc["schema_version"].(type) {
	case int32:
		schemaVersion = int(v)
	case int64:
		schemaVersion = int(v)
	case int:
		schemaVersion = v
	case float64:
		schemaVersion = int(v)
	}

	var resumeToken bson.Raw
	if rt, ok := raw["_id"].(bson.M); ok {
		if data, ok := rt["data"]; ok {
			if rawBytes, ok := data.(bson.Raw); ok {
				resumeToken = rawBytes
			}
		}
	}

	return Event{
		DocumentID:    docID,
		Status:        status,
		ObjectKey:     objectKey,
		CorrelationID: corrID,
		SchemaVersion: schemaVersion,
		ResumeToken:   resumeToken,
	}, nil
}

// tokenToString serializa un resume token bson.Raw a string JSON.
func tokenToString(token bson.Raw) string {
	if len(token) == 0 {
		return ""
	}
	out, err := bson.MarshalExtJSON(token, true, false)
	if err != nil {
		return ""
	}
	return string(out)
}

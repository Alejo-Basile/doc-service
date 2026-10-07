// Package changestream implementa el watcher de MongoDB Change Streams.
// Escucha cambios en la colección `documents` y dispara handlers
// cuando un documento pasa a estado UPLOADED (SPEC §3.6, S1-P2-06/07/08).
package changestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	mongoadapter "github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// errResumeTokenInvalid señala que el resume token persistido fue rechazado
// por el servidor (p.ej. entró en rotación del oplog) y el watcher debe
// descartarlo y reiniciar el stream fresco en vez de reintentar el mismo
// token en loop (S1-P2-07/08).
var errResumeTokenInvalid = errors.New("resume token invalido, descartando y reiniciando stream")

// maxConsecutiveResets limita los reinicios frescos consecutivos disparados
// por un token invalidado, para no entrar en un tight-loop si el fallo persiste
// (p.ej. Mongo inaccesible al mismo tiempo).
const maxConsecutiveResets = 3

// isResumeTokenInvalid detecta los errores fatales de Change Stream que Mongo
// emite cuando no puede reanudar desde el resume token guardado. Se dispara
// ante cualquier error del servidor que implique que el token persistido ya no
// puede usarse:
//
//   - "cannot resume stream; the resume token was not found": token válido
//     estructuralmente pero su opTime ya no está en el oplog (rotación).
//     Error fatal en getMore ("stream error:") o al abrir ("watch:").
//   - "Bad resume token" (Location40647) / "KeyString format error"
//     (Location50810/50811): token corrupto o malformado en disco.
//   - "resume token string was not a valid hex string" (FailedToParse):
//     token ilegible que no deja validar la reanudación.
//
// En todos los casos reintentar el mismo token en loop es inútil: hay que
// descartarlo y reiniciar el stream fresco (S1-P2-07/08).
func isResumeTokenInvalid(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "cannot resume stream") ||
		strings.Contains(msg, "resume token") ||
		strings.Contains(msg, "KeyString format error")
}

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
// persistido (S1-P2-07, S1-P2-08). Ante un resume token invalidado por el
// servidor (rotación del oplog), el watcher descarta el token y reinicia el
// stream fresco en vez de reintentar el mismo token en loop (S1-P2-07/08).
func (w *Watcher) Run(ctx context.Context) error {
	backoff := w.retryBackoff
	resets := 0

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

		// Token invalidado: se descartó dentro de watchOnce. Reiniciar fresco
		// sin backoff creciente, con tope de resets consecutivos para no caer
		// en un tight-loop si el fallo persiste por otra causa.
		if errors.Is(err, errResumeTokenInvalid) {
			if resets < maxConsecutiveResets {
				resets++
				slog.Warn("resume token invalidado: stream reiniciado fresco",
					"reset", resets,
					"max_resets", maxConsecutiveResets,
				)
				backoff = w.retryBackoff
				continue
			}
			slog.Error("demasiados resets por resume token inválido, pasando a backoff normal",
				"resets", resets,
				"error", err,
			)
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
		if token != "" && isResumeTokenInvalid(err) {
			return w.discardToken(ctx, err)
		}
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
		if token != "" && isResumeTokenInvalid(err) {
			return w.discardToken(ctx, err)
		}
		return fmt.Errorf("stream error: %w", err)
	}

	return nil
}

// discardToken elimina el resume token persistido cuando Mongo rechaza
// reanudar desde él (error fatal "cannot resume stream"), permitiendo que Run
// reinicie el stream fresco en vez de reintentar el mismo token en loop.
func (w *Watcher) discardToken(ctx context.Context, cause error) error {
	if err := w.tokenStore.Delete(ctx); err != nil {
		slog.Error("no se pudo eliminar el resume token invalidado", "error", err)
		return errResumeTokenInvalid
	}
	slog.Warn("resume token invalidado: eliminado para reiniciar el stream fresco",
		"error", cause,
	)
	return errResumeTokenInvalid
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

// Package relay implementa el relay Change Stream → Redis Streams.
// Conecta el watcher de MongoDB con la cola de trabajo de Redis
// (SPEC §5.2, S2-P2-03 / S4-P2-05).
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/adapters/changestream"
	"github.com/Alejo-Basile/doc-service/internal/metrics"
	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// Relay conecta el Change Stream de Mongo con la cola Redis Streams.
type Relay struct {
	queue       ports.WorkQueue
	schemaVer   int
	maxRetries  int
	baseBackoff time.Duration
}

// Config configura el comportamiento de reintento del Relay (S4-P2-05).
type Config struct {
	// MaxRetries es el número de reintentos de encolado tras el primer intento
	// (0 = sin reintentos). Default: 4.
	MaxRetries int
	// BaseBackoff es el backoff base entre reintentos (se duplica cada vez).
	// Default: 250 ms.
	BaseBackoff time.Duration
}

// DefaultConfig retorna la configuración por defecto de reintentos.
func DefaultConfig() Config {
	return Config{
		MaxRetries:  4,
		BaseBackoff: 250 * time.Millisecond,
	}
}

// New crea un Relay sobre la cola indicada con la configuración por defecto.
func New(queue ports.WorkQueue, schemaVersion int) *Relay {
	return NewWithConfig(queue, schemaVersion, DefaultConfig())
}

// NewWithConfig crea un Relay con reintentos y backoff explícitos (S4-P2-05).
func NewWithConfig(queue ports.WorkQueue, schemaVersion int, cfg Config) *Relay {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = DefaultConfig().BaseBackoff
	}
	return &Relay{
		queue:       queue,
		schemaVer:   schemaVersion,
		maxRetries:  cfg.MaxRetries,
		baseBackoff: cfg.BaseBackoff,
	}
}

// Handler retorna una función Handler para el Watcher:
// ante un evento UPLOADED, hace XADD a Redis Streams con el payload
// del contrato (SPEC §11.3).
//
// Ante un fallo de encolado reintenta con backoff exponencial hasta agotar
// maxRetries (S4-P2-05). Si persiste, incrementa la métrica de error y
// devuelve el error; el watcher reintenta el mismo evento in-process (#55) y,
// si el fallo es persistente, avanza el token y el reconciliador lo recupera.
func (r *Relay) Handler() changestream.Handler {
	return func(ctx context.Context, event changestream.Event) error {
		// Solo procesar eventos UPLOADED (el pipeline ya filtra, pero defensa en profundidad).
		if event.Status != "UPLOADED" {
			return nil
		}

		msg := ports.WorkMessage{
			DocumentID:    event.DocumentID,
			ObjectKey:     event.ObjectKey,
			CorrelationID: event.CorrelationID,
			EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
			SchemaVersion: r.schemaVer,
		}

		// Si el ObjectKey viene vacío en el evento, derivarlo del document_id
		// (es la regla del contrato: object_key = raw-pdfs/<id>.pdf).
		if msg.ObjectKey == "" {
			msg.ObjectKey = fmt.Sprintf("raw-pdfs/%s.pdf", event.DocumentID)
		}

		err := r.enqueueWithRetry(ctx, msg)
		if err != nil {
			// El token no avanzó: el watcher reintenta este mismo evento (#55).
			// Si el fallo es persistente, el reconciliador cubre el documento.
			metrics.RelayEnqueueErrors.Inc()
			slog.Error("relay: no se pudo encolar tras reintentos; el watcher lo reintentará",
				"document_id", event.DocumentID,
				"object_key", msg.ObjectKey,
				"retries", r.maxRetries,
				"error", err,
			)
			return fmt.Errorf("enqueue document %s: %w", event.DocumentID, err)
		}

		metrics.RelayEnqueued.Inc()
		slog.Info("relay: evento encolado",
			"document_id", event.DocumentID,
			"object_key", msg.ObjectKey,
			"correlation_id", event.CorrelationID,
		)
		return nil
	}
}

// enqueueWithRetry intenta encolar con backoff exponencial.
func (r *Relay) enqueueWithRetry(ctx context.Context, msg ports.WorkMessage) error {
	var lastErr error
	backoff := r.baseBackoff

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := r.queue.Enqueue(ctx, msg); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == r.maxRetries {
			break
		}

		slog.Warn("relay: fallo de encolado, reintentando",
			"document_id", msg.DocumentID,
			"attempt", attempt+1,
			"backoff", backoff.String(),
			"error", lastErr,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}

	return lastErr
}

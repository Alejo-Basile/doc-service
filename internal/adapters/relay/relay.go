// Package relay implementa el relay Change Stream → Redis Streams.
// Conecta el watcher de MongoDB con la cola de trabajo de Redis
// (SPEC §5.2, S2-P2-03).
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/adapters/changestream"
	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// Relay conecta el Change Stream de Mongo con la cola Redis Streams.
type Relay struct {
	queue     ports.WorkQueue
	schemaVer int
}

// New crea un Relay sobre la cola indicada.
func New(queue ports.WorkQueue, schemaVersion int) *Relay {
	return &Relay{
		queue:     queue,
		schemaVer: schemaVersion,
	}
}

// Handler retorna una función Handler para el Watcher:
// ante un evento UPLOADED, hace XADD a Redis Streams con el payload
// del contrato (SPEC §11.3).
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

		if err := r.queue.Enqueue(ctx, msg); err != nil {
			return fmt.Errorf("enqueue document %s: %w", event.DocumentID, err)
		}

		slog.Info("relay: evento encolado",
			"document_id", event.DocumentID,
			"object_key", msg.ObjectKey,
			"correlation_id", msg.CorrelationID,
		)
		return nil
	}
}

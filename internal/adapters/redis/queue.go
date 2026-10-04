// Package redis implementa el adaptador de infraestructura para Redis Streams.
// Cumple el puerto ports.WorkQueue.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/redis/go-redis/v9"
)

// WorkQueue implementa ports.WorkQueue sobre Redis Streams.
// Después de cada XADD ejecuta WAIT 1 para reducir la ventana de pérdida
// en un failover de Sentinel (SPEC §3.5).
type WorkQueue struct {
	client      *redis.Client
	streamKey   string
	waitCount   int
	waitTimeout time.Duration
	dedupTTL    time.Duration
}

// WorkQueueConfig configura el WorkQueue.
type WorkQueueConfig struct {
	StreamKey   string
	WaitCount   int           // réplicas mínimas para WAIT (SPEC: 1)
	WaitTimeout time.Duration // timeout de WAIT
	DedupTTL    time.Duration // TTL de la clave de deduplicación
}

// DefaultWorkQueueConfig retorna la configuración por defecto.
func DefaultWorkQueueConfig() WorkQueueConfig {
	return WorkQueueConfig{
		StreamKey:   "stream:pdf-processing",
		WaitCount:   1,
		WaitTimeout: 500 * time.Millisecond,
		DedupTTL:    10 * time.Minute,
	}
}

// NewWorkQueue crea un WorkQueue sobre la dirección Redis indicada.
func NewWorkQueue(addr string, cfg WorkQueueConfig) *WorkQueue {
	client := redis.NewClient(&redis.Options{Addr: addr})
	return &WorkQueue{
		client:      client,
		streamKey:   cfg.StreamKey,
		waitCount:   cfg.WaitCount,
		waitTimeout: cfg.WaitTimeout,
		dedupTTL:    cfg.DedupTTL,
	}
}

// Enqueue agrega un mensaje a Redis Streams con deduplicación y WAIT.
//
// Pasos:
//  1. Deduplicación: SET dedup:<document_id> 1 NX EX <ttl>. Si ya existe,
//     el mensaje fue encolado previamente (puede haber >1 watcher).
//  2. XADD al stream con el payload del contrato (sin attempts).
//  3. WAIT 1 <timeout> para que el mensaje esté replicado.
func (w *WorkQueue) Enqueue(ctx context.Context, msg ports.WorkMessage) error {
	// 1. Deduplicación.
	dedupKey := fmt.Sprintf("dedup:%s", msg.DocumentID)
	inserted, err := w.client.SetNX(ctx, dedupKey, "1", w.dedupTTL).Result()
	if err != nil {
		return fmt.Errorf("dedup SETNX %s: %w", dedupKey, err)
	}
	if !inserted {
		slog.Debug("mensaje deduplicado, ya encolado", "document_id", msg.DocumentID)
		return nil
	}

	// 2. XADD al stream.
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal work message: %w", err)
	}

	streamID, err := w.client.XAdd(ctx, &redis.XAddArgs{
		Stream: w.streamKey,
		Values: map[string]interface{}{
			"payload": string(payload),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("XADD %s: %w", w.streamKey, err)
	}

	// 3. WAIT para replicación.
	if w.waitCount > 0 {
		replicas, err := w.client.Wait(ctx, w.waitCount, w.waitTimeout).Result()
		if err != nil {
			slog.Warn("WAIT falló (mensaje encolado pero no confirmado réplica)",
				"document_id", msg.DocumentID,
				"stream_id", streamID,
				"error", err,
			)
		} else if int(replicas) < w.waitCount {
			slog.Warn("WAIT: réplicas insuficientes",
				"document_id", msg.DocumentID,
				"stream_id", streamID,
				"replicas", replicas,
				"expected", w.waitCount,
			)
		}
	}

	slog.Info("mensaje encolado",
		"document_id", msg.DocumentID,
		"stream_id", streamID,
		"stream", w.streamKey,
	)
	return nil
}

// Ping verifica la conectividad con Redis.
func (w *WorkQueue) Ping(ctx context.Context) error {
	return w.client.Ping(ctx).Err()
}

// Close cierra la conexión Redis.
func (w *WorkQueue) Close() error {
	return w.client.Close()
}

// Client retorna el cliente Redis subyacente.
func (w *WorkQueue) Client() *redis.Client {
	return w.client
}

// StreamKey retorna el nombre del stream configurado.
func (w *WorkQueue) StreamKey() string {
	return w.streamKey
}

// XLen retorna la longitud actual del stream (para tests y métricas).
func (w *WorkQueue) XLen(ctx context.Context) (int64, error) {
	return w.client.XLen(ctx, w.streamKey).Result()
}

// XRange retorna los mensajes del stream (para tests).
func (w *WorkQueue) XRange(ctx context.Context, start, end string, count int64) ([]redis.XMessage, error) {
	return w.client.XRangeN(ctx, w.streamKey, start, end, count).Result()
}

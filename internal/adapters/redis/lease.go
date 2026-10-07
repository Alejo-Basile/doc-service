// Package redis implementa el adaptador de infraestructura para Redis.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lease implementa ports.DistributedLease sobre Redis.
// Usa SET NX con TTL como lease atómico: si la clave ya existe, otro holder está activo.
type Lease struct {
	client *redis.Client
}

// NewLease crea un Lease sobre la dirección Redis indicada.
func NewLease(addr string) *Lease {
	return &Lease{client: redis.NewClient(&redis.Options{Addr: addr})}
}

// NewLeaseFromClient crea un Lease sobre un cliente Redis existente.
func NewLeaseFromClient(client *redis.Client) *Lease {
	return &Lease{client: client}
}

// Acquire intenta adquirir el lease con la clave y TTL indicados.
// Usa SET NX EX: solo inserta si la clave no existe, con expiración automática.
func (l *Lease) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	leaseKey := fmt.Sprintf("lease:%s", key)
	inserted, err := l.client.SetNX(ctx, leaseKey, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lease SETNX %s: %w", leaseKey, err)
	}
	return inserted, nil
}

// Release libera el lease. Solo borra si la clave existe (podría haber expirado).
func (l *Lease) Release(ctx context.Context, key string) error {
	leaseKey := fmt.Sprintf("lease:%s", key)
	if err := l.client.Del(ctx, leaseKey).Err(); err != nil {
		return fmt.Errorf("lease DEL %s: %w", leaseKey, err)
	}
	return nil
}

// Ping verifica la conectividad con Redis.
func (l *Lease) Ping(ctx context.Context) error {
	return l.client.Ping(ctx).Err()
}

// Close cierra la conexión Redis.
func (l *Lease) Close() error {
	return l.client.Close()
}

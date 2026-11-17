package redis

import (
	"context"
	"fmt"

	"github.com/Alejo-Basile/doc-service/internal/ports"
	goredis "github.com/redis/go-redis/v9"
)

// healthProbe adapts a Redis client to ports.HealthProbe using Ping.
type healthProbe struct {
	client *goredis.Client
}

// NewHealthProbe returns a readiness probe backed by client.Ping.
func NewHealthProbe(client *goredis.Client) ports.HealthProbe {
	return &healthProbe{client: client}
}

// Name identifies the dependency in the readiness payload.
func (p *healthProbe) Name() string { return "redis" }

// Check pings Redis; any error means not ready.
func (p *healthProbe) Check(ctx context.Context) error {
	if err := p.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

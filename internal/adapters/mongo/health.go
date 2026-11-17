package mongo

import (
	"context"
	"fmt"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// healthProbe adapts a Mongo Client to ports.HealthProbe using a server ping.
type healthProbe struct {
	client *Client
}

// NewHealthProbe returns a readiness probe backed by client.Ping.
func NewHealthProbe(client *Client) ports.HealthProbe {
	return &healthProbe{client: client}
}

// Name identifies the dependency in the readiness payload.
func (p *healthProbe) Name() string { return "mongo" }

// Check pings the MongoDB server; any error means not ready.
func (p *healthProbe) Check(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("mongo ping: %w", err)
	}
	return nil
}

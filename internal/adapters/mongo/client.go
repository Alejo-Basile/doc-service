// Package mongo implementa los adaptadores de infraestructura para MongoDB.
// Cumple los puertos definidos en internal/ports.
package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// PoolConfig configura el pool de conexiones de MongoDB (SPEC §3.6, S1-P2-01).
type PoolConfig struct {
	MaxPoolSize     uint64
	MinPoolSize     uint64
	ConnectTimeout  time.Duration
	Timeout         time.Duration // timeout global de operaciones (antes socketTimeoutMS)
	MaxConnIdleTime time.Duration
	RetryWrites     bool
}

// DefaultPoolConfig retorna la configuración por defecto del pool.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxPoolSize:     20,
		MinPoolSize:     2,
		ConnectTimeout:  10 * time.Second,
		Timeout:         30 * time.Second,
		MaxConnIdleTime: 60 * time.Second,
		RetryWrites:     true,
	}
}

// Client encapsula el cliente de MongoDB y la referencia a la base de datos.
type Client struct {
	client *mongo.Client
	db     *mongo.Database
}

// NewClient conecta a MongoDB con la configuración de pool explícita.
// Usa readConcern majority y writeConcern majority (SPEC §3.6).
func NewClient(ctx context.Context, uri, database string, pool PoolConfig) (*Client, error) {
	opts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(pool.ConnectTimeout).
		SetTimeout(pool.Timeout).
		SetMaxPoolSize(pool.MaxPoolSize).
		SetMinPoolSize(pool.MinPoolSize).
		SetMaxConnIdleTime(pool.MaxConnIdleTime).
		SetRetryWrites(pool.RetryWrites).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.Majority())

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	// Verificar conectividad con ping.
	pingCtx, cancel := context.WithTimeout(ctx, pool.ConnectTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	slog.Info("cliente MongoDB conectado",
		"database", database,
		"max_pool_size", pool.MaxPoolSize,
		"min_pool_size", pool.MinPoolSize,
		"retry_writes", pool.RetryWrites,
	)

	return &Client{
		client: client,
		db:     client.Database(database),
	}, nil
}

// DB retorna la referencia a la base de datos.
func (c *Client) DB() *mongo.Database { return c.db }

// Collection retorna una colección por nombre.
func (c *Client) Collection(name string) *mongo.Collection {
	return c.db.Collection(name)
}

// Close cierra la conexión del cliente.
func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

// Ping verifica la conectividad.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

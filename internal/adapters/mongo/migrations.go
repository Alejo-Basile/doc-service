package mongo

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// SchemaVersionStore persiste la versión del esquema aplicada.
type SchemaVersionStore struct {
	coll *mongo.Collection
}

// NewSchemaVersionStore crea el store sobre la colección `schema_versions`.
func NewSchemaVersionStore(client *Client) *SchemaVersionStore {
	return &SchemaVersionStore{
		coll: client.Collection("schema_versions"),
	}
}

// Current retorna la versión del esquema actualmente aplicada.
// Devuelve 0 si no hay registro (esquema nunca migrado).
func (s *SchemaVersionStore) Current(ctx context.Context) (int, error) {
	var doc bson.M
	err := s.coll.FindOne(ctx, bson.M{}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}
		return 0, fmt.Errorf("schema version read: %w", err)
	}
	v, _ := doc["version"].(int32)
	return int(v), nil
}

// Set actualiza la versión del esquema aplicada.
func (s *SchemaVersionStore) Set(ctx context.Context, version int) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.M{},
		bson.M{"$set": bson.M{"version": version}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("schema version set %d: %w", version, err)
	}
	return nil
}

// Migration define una migración versionada e idempotente.
type Migration struct {
	Version int
	Name    string
	Apply   func(ctx context.Context, db *mongo.Database) error
}

// Migrator aplica migraciones de esquema en orden.
type Migrator struct {
	db         *mongo.Database
	versions   *SchemaVersionStore
	migrations []Migration
}

// NewMigrator crea un migrador con la lista de migraciones.
func NewMigrator(client *Client, migrations ...Migration) *Migrator {
	return &Migrator{
		db:         client.DB(),
		versions:   NewSchemaVersionStore(client),
		migrations: migrations,
	}
}

// Up aplica todas las migraciones pendientes. Idempotente: si ya están
// aplicadas, no hace nada (S1-P2-05).
func (m *Migrator) Up(ctx context.Context) error {
	current, err := m.versions.Current(ctx)
	if err != nil {
		return err
	}

	aplicadas := 0
	for _, mig := range m.migrations {
		if mig.Version <= current {
			continue
		}

		slog.Info("aplicando migración", "version", mig.Version, "name", mig.Name)
		if err := mig.Apply(ctx, m.db); err != nil {
			return fmt.Errorf("migración %d (%s): %w", mig.Version, mig.Name, err)
		}

		if err := m.versions.Set(ctx, mig.Version); err != nil {
			return err
		}
		aplicadas++
	}

	if aplicadas > 0 {
		slog.Info("migraciones aplicadas", "count", aplicadas, "current_version", current+aplicadas)
	}
	return nil
}

// CurrentVersion retorna la versión actual del esquema.
func (m *Migrator) CurrentVersion(ctx context.Context) (int, error) {
	return m.versions.Current(ctx)
}

// DefaultMigrations retorna las migraciones estándar del esquema documents.
func DefaultMigrations() []Migration {
	return []Migration{
		{
			Version: 1,
			Name:    "init_documents_schema",
			Apply: func(ctx context.Context, db *mongo.Database) error {
				coll := db.Collection("documents")

				// Índice simple sobre status (reconciliación y queries).
				if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
					Keys: bson.D{{Key: "status", Value: 1}},
				}); err != nil {
					return fmt.Errorf("create index status: %w", err)
				}

				// Índice sobre updated_at (dashboards, ordenamiento).
				if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
					Keys: bson.D{{Key: "updated_at", Value: 1}},
				}); err != nil {
					return fmt.Errorf("create index updated_at: %w", err)
				}

				// Índice TTL sobre expires_at (limpieza automática, SPEC §4).
				if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
					Keys:    bson.D{{Key: "expires_at", Value: 1}},
					Options: options.Index().SetExpireAfterSeconds(0),
				}); err != nil {
					return fmt.Errorf("create index expires_at TTL: %w", err)
				}

				return nil
			},
		},
	}
}

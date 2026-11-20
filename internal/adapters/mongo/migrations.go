package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
		{
			Version: 2,
			Name:    "retention_at_ttl_and_backfill",
			Apply: func(ctx context.Context, db *mongo.Database) error {
				coll := db.Collection("documents")
				const (
					pendingRetention  = int64(60 * 60 * 1000)       // 60 min en ms
					terminalRetention = int64(168 * 60 * 60 * 1000) // 168 h en ms
				)
				now := time.Now().UTC()

				// (a) Backfill de retention_at. El TTL deja de gobernarse por
				// expires_at: ese campo marca cuándo el reconciliador puede
				// decidir, y retention_at cuándo el TTL puede recolectar
				// (SPEC §4: el TTL recolecta estados ya decididos).
				if err := backfillRetention(ctx, coll, []bson.M{
					{"status": string("PENDING_UPLOAD"), "retention_at": bson.M{"$exists": false}},
				}, bson.D{{Key: "$add", Value: bson.A{
					// Garantiza al menos una ventana completa de retención a
					// partir de ahora: un documento ya vencido al migrar debe
					// seguir siendo decidible por el reconciliador.
					bson.D{{Key: "$max", Value: bson.A{"$expires_at", "$$NOW"}}},
					pendingRetention,
				}}}); err != nil {
					return fmt.Errorf("backfill retention pending: %w", err)
				}
				if err := backfillRetention(ctx, coll, []bson.M{
					{"status": bson.M{"$in": bson.A{"COMPLETED", "FAILED", "REJECTED", "UPLOAD_EXPIRED"}}, "retention_at": bson.M{"$exists": false}},
				}, bson.D{{Key: "$add", Value: bson.A{now, terminalRetention}}}); err != nil {
					return fmt.Errorf("backfill retention terminal: %w", err)
				}
				if err := backfillRetention(ctx, coll, []bson.M{
					{"status": bson.M{"$nin": bson.A{"PENDING_UPLOAD", "COMPLETED", "FAILED", "REJECTED", "UPLOAD_EXPIRED"}}, "retention_at": bson.M{"$exists": false}},
				}, bson.D{{Key: "$add", Value: bson.A{now, pendingRetention}}}); err != nil {
					return fmt.Errorf("backfill retention intermediate: %w", err)
				}

				// (b) Índice TTL sobre retention_at.
				if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
					Keys:    bson.D{{Key: "retention_at", Value: 1}},
					Options: options.Index().SetName("retention_at_ttl").SetExpireAfterSeconds(0),
				}); err != nil {
					return fmt.Errorf("create index retention_at TTL: %w", err)
				}

				// (c) Recién con (a) y (b) aplicados, retirar el TTL viejo.
				// Nunca se dropea si el índice nuevo no está presente.
				names, err := indexNames(ctx, coll)
				if err != nil {
					return fmt.Errorf("list indexes: %w", err)
				}
				if names["retention_at_ttl"] && names["expires_at_1"] {
					if err := coll.Indexes().DropOne(ctx, "expires_at_1"); err != nil {
						return fmt.Errorf("drop index expires_at_1: %w", err)
					}
				}

				return nil
			},
		},
	}
}

// backfillRetention aplica el pipeline $set de retention_at a los documentos
// que matchean alguno de los filtros. Idempotente: sólo toca los que no tienen
// el campo.
func backfillRetention(ctx context.Context, coll *mongo.Collection, filters []bson.M, add bson.D) error {
	for _, f := range filters {
		pipeline := mongo.Pipeline{
			{{Key: "$set", Value: bson.D{{Key: "retention_at", Value: add}}}},
		}
		if _, err := coll.UpdateMany(ctx, f, pipeline); err != nil {
			return err
		}
	}
	return nil
}

// indexNames devuelve el conjunto de nombres de índice de la colección.
func indexNames(ctx context.Context, coll *mongo.Collection) (map[string]bool, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := map[string]bool{}
	for cur.Next(ctx) {
		var spec struct {
			Name string `bson:"name"`
		}
		if err := cur.Decode(&spec); err != nil {
			return nil, err
		}
		if spec.Name != "" {
			out[spec.Name] = true
		}
	}
	return out, cur.Err()
}

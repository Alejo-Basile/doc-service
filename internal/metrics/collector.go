// Package metrics implementa el collector de métricas de negocio
// que consulta MongoDB periódicamente (background job) para exponer
// gauges de documentos por estado y antigüedad.
// SPEC §10, plan-S4-P1.md §4.2
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/adapters/mongo"
	"github.com/Alejo-Basile/doc-service/internal/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CollectorConfig configura el collector de métricas de negocio.
type CollectorConfig struct {
	// RefreshInterval es el intervalo entre consultas a MongoDB.
	// Default: 15s. No consultar en cada scrape para no cargar Mongo.
	RefreshInterval time.Duration
}

// DefaultCollectorConfig retorna la configuración por defecto.
func DefaultCollectorConfig() CollectorConfig {
	return CollectorConfig{
		RefreshInterval: 15 * time.Second,
	}
}

// StartCollector inicia el job en background que actualiza las métricas
// de negocio consultando MongoDB. Se detiene cuando se cancela ctx.
// No debe fallar el proceso si Mongo no está disponible: loguea y reintenta.
func StartCollector(ctx context.Context, client *mongo.Client, cfg CollectorConfig) {
	if cfg.RefreshInterval <= 0 {
		cfg = DefaultCollectorConfig()
	}

	slog.Info("iniciando collector de métricas de negocio",
		"refresh_interval", cfg.RefreshInterval.String(),
	)

	ticker := time.NewTicker(cfg.RefreshInterval)
	defer ticker.Stop()

	// Ejecutar una vez al inicio
	collectOnce(ctx, client)

	for {
		select {
		case <-ctx.Done():
			slog.Info("collector de métricas de negocio detenido")
			return
		case <-ticker.C:
			collectOnce(ctx, client)
		}
	}
}

// collectOnce ejecuta una sola iteración de recolección de métricas.
func collectOnce(ctx context.Context, client *mongo.Client) {
	coll := client.Collection("documents")

	// 1) Conteo por estado
	if err := collectByStatus(ctx, coll); err != nil {
		slog.Error("error recolectando métricas por estado", "error", err.Error())
	}

	// 2) Antigüedad del más viejo por estado intermedio operativo
	if err := collectOldestIntermediateAge(ctx, coll); err != nil {
		slog.Error("error recolectando antigüedad de estados intermedios", "error", err.Error())
	}

	// 3) Total no terminales operativos (excluye PENDING_UPLOAD)
	if err := collectNonTerminal(ctx, coll); err != nil {
		slog.Error("error recolectando total no terminales", "error", err.Error())
	}
}

// collectByStatus actualiza docservice_documents_by_status.
func collectByStatus(ctx context.Context, coll *mongodriver.Collection) error {
	pipeline := mongodriver.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$status"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
	}

	cursor, err := coll.Aggregate(ctx, pipeline, options.Aggregate())
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// Resetear todos los gauges conocidos a 0 antes de setear los valores reales
	// para que estados sin documentos muestren 0 y no valor anterior.
	knownStatuses := []string{
		string(domain.StatusPendingUpload),
		string(domain.StatusUploaded),
		string(domain.StatusQueued),
		string(domain.StatusProcessing),
		string(domain.StatusRetrying),
		string(domain.StatusExtractionFailed),
		string(domain.StatusCompensating),
		string(domain.StatusCompleted),
		string(domain.StatusFailed),
		string(domain.StatusRejected),
		string(domain.StatusUploadExpired),
	}
	for _, s := range knownStatuses {
		DocumentsByStatus.WithLabelValues(s).Set(0)
	}

	for cursor.Next(ctx) {
		var result struct {
			Status string `bson:"_id"`
			Count  int64  `bson:"count"`
		}
		if err := cursor.Decode(&result); err != nil {
			slog.Error("error decodificando resultado conteo por estado", "error", err.Error())
			continue
		}
		DocumentsByStatus.WithLabelValues(result.Status).Set(float64(result.Count))
	}

	return cursor.Err()
}

// intermediateOperationalStatuses son los estados intermedios que entran
// en la métrica de antigüedad y en el contador de no terminales.
// EXCLUYE PENDING_UPLOAD (se gobierna por expires_at/reconciliador).
var intermediateOperationalStatuses = []string{
	string(domain.StatusUploaded),
	string(domain.StatusQueued),
	string(domain.StatusProcessing),
	string(domain.StatusRetrying),
	string(domain.StatusExtractionFailed),
	string(domain.StatusCompensating),
}

// collectOldestIntermediateAge actualiza docservice_documents_oldest_intermediate_age_seconds.
func collectOldestIntermediateAge(ctx context.Context, coll *mongodriver.Collection) error {
	// Pipeline: match estados intermedios operativos -> group por status -> min(updated_at)
	// Fallback a created_at si updated_at no existe
	pipeline := mongodriver.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "status", Value: bson.D{{Key: "$in", Value: intermediateOperationalStatuses}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$status"},
			{Key: "oldest_updated", Value: bson.D{{Key: "$min", Value: "$updated_at"}}},
			{Key: "oldest_created", Value: bson.D{{Key: "$min", Value: "$created_at"}}},
		}}},
	}

	cursor, err := coll.Aggregate(ctx, pipeline, options.Aggregate())
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// Resetear gauges conocidos a 0
	for _, s := range intermediateOperationalStatuses {
		DocumentsOldestIntermediateAgeSeconds.WithLabelValues(s).Set(0)
	}

	now := time.Now().UTC()
	for cursor.Next(ctx) {
		var result struct {
			Status        string    `bson:"_id"`
			OldestUpdated time.Time `bson:"oldest_updated"`
			OldestCreated time.Time `bson:"oldest_created"`
		}
		if err := cursor.Decode(&result); err != nil {
			slog.Error("error decodificando resultado antigüedad", "error", err.Error())
			continue
		}

		// Fallback: usar created_at si updated_at es zero
		oldest := result.OldestUpdated
		if oldest.IsZero() {
			oldest = result.OldestCreated
		}
		if oldest.IsZero() {
			continue // sin datos válidos
		}

		ageSeconds := now.Sub(oldest).Seconds()
		if ageSeconds < 0 {
			ageSeconds = 0 // por si hay drift de reloj
		}
		DocumentsOldestIntermediateAgeSeconds.WithLabelValues(result.Status).Set(ageSeconds)
	}

	return cursor.Err()
}

// collectNonTerminal actualiza docservice_documents_non_terminal.
func collectNonTerminal(ctx context.Context, coll *mongodriver.Collection) error {
	filter := bson.D{{Key: "status", Value: bson.D{{Key: "$in", Value: intermediateOperationalStatuses}}}}
	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return err
	}
	DocumentsNonTerminal.Set(float64(count))
	return nil
}

// StartCollectorWithConfig helper para main.go: inicia el collector
// usando el cliente Mongo principal (el mismo que usa el repositorio).
// Recibe el *mongo.Client del adapter (watcherClient o mongoClient).
func StartCollectorWithConfig(ctx context.Context, client *mongo.Client) {
	StartCollector(ctx, client, DefaultCollectorConfig())
}

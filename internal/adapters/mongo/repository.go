package mongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrNotFound se devuelve cuando el documento no existe.
var ErrNotFound = errors.New("documento no encontrado")

// DocumentRepository implementa ports.DocumentRepository sobre MongoDB.
// Toda escritura de estado usa transición condicional (SPEC §4, §11.1).
type DocumentRepository struct {
	coll *mongo.Collection
}

// NewDocumentRepository crea un repositorio sobre la colección indicada.
func NewDocumentRepository(client *Client, collectionName string) *DocumentRepository {
	return &DocumentRepository{
		coll: client.Collection(collectionName),
	}
}

// Insert inserta un documento nuevo. Idempotente por _id: si ya existe,
// devuelve el error de duplicado de MongoDB (E11000).
func (r *DocumentRepository) Insert(ctx context.Context, doc *domain.Document) error {
	_, err := r.coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("documento %s ya existe: %w", doc.ID, err)
		}
		return fmt.Errorf("insert documento %s: %w", doc.ID, err)
	}
	return nil
}

// GetByID busca un documento por su _id.
func (r *DocumentRepository) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	var doc domain.Document
	err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("documento %s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("find documento %s: %w", id, err)
	}
	return &doc, nil
}

// UpdateStatus aplica una transición condicional:
// updateOne({_id, status: from}, {$set: {status: to, ...}}).
// Devuelve (false, nil) si el filtro no matcheó (SPEC §11.1).
func (r *DocumentRepository) UpdateStatus(
	ctx context.Context,
	id string,
	from, to domain.Status,
	fields map[string]any,
) (bool, error) {
	set := bson.M{
		"status":     to,
		"updated_at": time.Now().UTC(),
	}
	// Merge de campos adicionales (object_key, txt_ref, failure_reason, etc.)
	for k, v := range fields {
		set[k] = v
	}

	filter := bson.M{
		"_id":    id,
		"status": from,
	}

	result, err := r.coll.UpdateOne(ctx, filter, bson.M{"$set": set})
	if err != nil {
		return false, fmt.Errorf("update status %s (%s→%s): %w", id, from, to, err)
	}

	if result.MatchedCount == 0 {
		// No hay match: el documento no existe o no está en el estado `from`.
		return false, nil
	}

	slog.Debug("transición aplicada",
		"document_id", id,
		"from", from,
		"to", to,
	)
	return true, nil
}

// List retorna documentos paginados con filtro opcional por status.
func (r *DocumentRepository) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	q := bson.M{}
	if filter.Status != "" {
		q["status"] = filter.Status
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(filter.Limit)

	if filter.Limit <= 0 {
		opts.SetLimit(50)
	}

	cursor, err := r.coll.Find(ctx, q, opts)
	if err != nil {
		return nil, 0, fmt.Errorf("list documents: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []*domain.Document
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, 0, fmt.Errorf("decode list: %w", err)
	}

	total, err := r.coll.CountDocuments(ctx, q)
	if err != nil {
		return nil, 0, fmt.Errorf("count documents: %w", err)
	}

	return docs, total, nil
}

// UpdateStatusWithHistory aplica la transición condicional y agrega una
// entrada al historial en la misma operación (pipeline update).
// Es equivalente a UpdateStatus pero mantiene el historial consistente.
func (r *DocumentRepository) UpdateStatusWithHistory(
	ctx context.Context,
	id string,
	from, to domain.Status,
	entry domain.StatusEntry,
	extraSet bson.M,
) (bool, error) {
	now := time.Now().UTC()
	entry.At = now

	set := bson.M{
		"status":     to,
		"updated_at": now,
	}
	if entry.Reason != "" && (to == domain.StatusFailed ||
		to == domain.StatusRejected ||
		to == domain.StatusExtractionFailed) {
		set["failure_reason"] = entry.Reason
	}
	for k, v := range extraSet {
		set[k] = v
	}

	filter := bson.M{
		"_id":    id,
		"status": from,
	}

	update := bson.M{
		"$set": set,
		"$push": bson.M{
			"history": entry,
		},
	}

	result, err := r.coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, fmt.Errorf("update status+history %s (%s→%s): %w", id, from, to, err)
	}

	return result.MatchedCount > 0, nil
}

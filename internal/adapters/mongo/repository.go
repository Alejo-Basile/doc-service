package mongo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EncodeCursor construye un cursor opaco base64("RFC3339Nano|_id").
func EncodeCursor(createdAt time.Time, id string) string {
	raw := createdAt.Format(time.RFC3339Nano) + "|" + id
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor extrae (createdAt, id) de un cursor opaco.
func DecodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("base64: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("formato inválido: esperado RFC3339Nano|_id")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("parse time: %w", err)
	}
	return createdAt, parts[1], nil
}

// ErrNotFound se devuelve cuando el documento no existe.
// Es alias de ports.ErrNotFound: los consumidores (API) validan con
// errors.Is(err, ports.ErrNotFound), por lo que deben ser el mismo valor.
var ErrNotFound = ports.ErrNotFound

// DocumentRepository implementa ports.DocumentRepository sobre MongoDB.
// Toda escritura de estado usa transición condicional (SPEC §4, §11.1).
type DocumentRepository struct {
	coll *mongo.Collection

	// pendingRetention separa la elegibilidad del reconciliador (expires_at)
	// de la recolección del TTL (retention_at), para que el TTL nunca borre un
	// documento antes de que el reconciliador lo haya decidido (SPEC §4).
	pendingRetention time.Duration
	// terminalRetention mantiene los documentos terminales un tiempo de
	// retención propio: el TTL no debe gobiernarse por expires_at, que es un
	// campo de la ventana de subida y no de la retención (SPEC §4).
	terminalRetention time.Duration
}

const (
	defaultPendingRetention  = 60 * time.Minute
	defaultTerminalRetention = 168 * time.Hour
)

// NewDocumentRepository crea un repositorio sobre la colección indicada.
func NewDocumentRepository(client *Client, collectionName string) *DocumentRepository {
	return &DocumentRepository{
		coll:              client.Collection(collectionName),
		pendingRetention:  defaultPendingRetention,
		terminalRetention: defaultTerminalRetention,
	}
}

// WithRetention configura las ventanas de retención usadas para poblar
// retention_at. Valores <= 0 conservan el default.
func (r *DocumentRepository) WithRetention(pending, terminal time.Duration) *DocumentRepository {
	if pending > 0 {
		r.pendingRetention = pending
	}
	if terminal > 0 {
		r.terminalRetention = terminal
	}
	return r
}

// Insert inserta un documento nuevo. Idempotente por _id: si ya existe,
// devuelve el error de duplicado de MongoDB (E11000).
func (r *DocumentRepository) Insert(ctx context.Context, doc *domain.Document) error {
	if doc.RetentionAt.IsZero() && !doc.ExpiresAt.IsZero() {
		doc.RetentionAt = doc.ExpiresAt.Add(r.pendingRetention)
	}
	_, err := r.coll.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("documento %s: %w", doc.ID, ports.ErrDuplicate)
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

// applyRetention mantiene retention_at coherente con la transición: los
// terminales heredan la retención terminal y un re-presign extiende la
// retención pendiente. Nunca pisa un valor explícito del llamador.
func (r *DocumentRepository) applyRetention(set bson.M, to domain.Status) {
	if _, ok := set["retention_at"]; ok {
		return
	}
	now := time.Now().UTC()
	if to.IsTerminal() {
		set["retention_at"] = now.Add(r.terminalRetention)
		return
	}
	if to == domain.StatusPendingUpload {
		if exp, ok := set["expires_at"].(time.Time); ok && !exp.IsZero() {
			set["retention_at"] = exp.Add(r.pendingRetention)
		}
	}
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
	// Merge de campos adicionales (object_key, md_ref, failure_reason, etc.)
	for k, v := range fields {
		set[k] = v
	}
	r.applyRetention(set, to)

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
	if !filter.ExpiredBefore.IsZero() {
		q["expires_at"] = bson.M{"$lt": filter.ExpiredBefore}
	}
	// Cursor compuesto (expires_at, _id) sólo para el recorrido de vencidos:
	// estrictamente mayor, para no repetir el último documento de la página.
	if filter.SortByExpires && (!filter.ExpiredCursorAt.IsZero() || filter.ExpiredCursorID != "") {
		q["$or"] = bson.A{
			bson.M{"expires_at": bson.M{"$gt": filter.ExpiredCursorAt}},
			bson.M{
				"expires_at": filter.ExpiredCursorAt,
				"_id":        bson.M{"$gt": filter.ExpiredCursorID},
			},
		}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(filter.Limit)

	if filter.SortByExpires {
		// Los vencidos más antiguos primero: con Limit fijo este orden es el
		// único que garantiza que el reconciliador los alcance antes del TTL.
		opts.SetSort(bson.D{
			{Key: "expires_at", Value: 1},
			{Key: "_id", Value: 1},
		})
	}

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

// ListByCursor pagina por cursor compuesto (created_at, _id) en orden descendente (S3-P2-10).
// El filtro es estricto: created_at < cursorTime OR (created_at = cursorTime AND _id < cursorID).
// Esto garantiza que inserciones concurrentes no salten ni repitan registros.
func (r *DocumentRepository) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	q := bson.M{}
	if filter.Status != "" {
		q["status"] = filter.Status
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	// Pedimos limit+1 para saber si hay más páginas.
	opts := options.Find().
		SetSort(bson.D{
			{Key: "created_at", Value: -1},
			{Key: "_id", Value: -1},
		}).
		SetLimit(limit + 1)

	// Aplicar cursor si se provee.
	if filter.Cursor != "" {
		created, id, err := DecodeCursor(filter.Cursor)
		if err != nil {
			return nil, "", fmt.Errorf("%w: %v", ports.ErrInvalidCursor, err)
		}
		q["$or"] = []bson.M{
			{"created_at": bson.M{"$lt": created}},
			{"created_at": created, "_id": bson.M{"$lt": id}},
		}
	}

	cursor, err := r.coll.Find(ctx, q, opts)
	if err != nil {
		return nil, "", fmt.Errorf("list by cursor: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []*domain.Document
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, "", fmt.Errorf("decode list: %w", err)
	}

	// ¿Hay más páginas?
	hasMore := int64(len(docs)) > limit
	if hasMore {
		docs = docs[:limit]
	}

	// Generar next cursor del último documento.
	nextCursor := ""
	if hasMore && len(docs) > 0 {
		last := docs[len(docs)-1]
		nextCursor = EncodeCursor(last.CreatedAt, last.ID)
	}

	return docs, nextCursor, nil
}

// UpdateStatusWithHistory aplica la transición condicional y agrega una
// entrada al historial en la misma operación (pipeline update).
// Es equivalente a UpdateStatus pero mantiene el historial consistente.
func (r *DocumentRepository) UpdateStatusWithHistory(
	ctx context.Context,
	id string,
	from, to domain.Status,
	entry domain.StatusEntry,
	extraSet map[string]any,
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
	r.applyRetention(set, to)

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

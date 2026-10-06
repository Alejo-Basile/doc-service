// Package ports define las interfaces (puertos) del dominio.
// La capa de negocio depende de estas interfaces, nunca de las implementaciones
// concretas (DIP de Clean Architecture).
package ports

import (
	"context"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
)

// DocumentRepository define las operaciones de persistencia del agregado Document.
type DocumentRepository interface {
	Insert(ctx context.Context, doc *domain.Document) error
	GetByID(ctx context.Context, id string) (*domain.Document, error)
	// UpdateStatus aplica una transición condicional:
	// updateOne({_id, status: from}, {$set: {status: to, ...}}).
	// Devuelve (false, nil) si el filtro no matcheó (documento no está en `from`).
	UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error)
	List(ctx context.Context, filter ListFilter) ([]*domain.Document, int64, error)
}

// ListFilter representa los criterios de listado paginado.
type ListFilter struct {
	Status     domain.Status
	Cursor     string
	Limit      int64
	BeforeTime time.Time
	BeforeID   string
}

// ResumeTokenStore persiste el resume token del Change Stream (SPEC §5.3).
type ResumeTokenStore interface {
	Get(ctx context.Context) (string, error)
	Save(ctx context.Context, token string) error
}

// ObjectStorage abstracte el acceso al servidor de objetos (MinIO).
type ObjectStorage interface {
	// PresignPost genera una URL prefirmada POST con política (SPEC §5.1).
	PresignPost(ctx context.Context, objectKey string, opts PresignPostOptions) (*PresignPostResult, error)
	// Stat verifica la existencia de un objeto (HEAD).
	Stat(ctx context.Context, objectKey string) (ObjectInfo, error)
	// GetRange lee un rango de bytes del objeto (para validar %PDF-).
	GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error)
	// Delete elimina un objeto.
	Delete(ctx context.Context, objectKey string) error
	// PresignGet genera una URL prefirmada de lectura.
	PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error)
}

// PresignPostOptions configura la política de la URL prefirmada.
type PresignPostOptions struct {
	MaxSizeBytes   int64
	ContentType    string
	Expiration     time.Duration
	PublicEndpoint string // host público de S3, ej: https://s3.dominio
}

// PresignPostResult contiene los campos del formulario de subida S3 POST.
type PresignPostResult struct {
	UploadURL string
	Fields    map[string]string
	ExpiresIn time.Duration
}

// ObjectInfo representa los metadatos de un objeto.
type ObjectInfo struct {
	Key          string
	Size         int64
	ContentType  string
	LastModified time.Time
}

// WorkQueue abstracte la mensajería (Redis Streams).
type WorkQueue interface {
	// Enqueue agrega un mensaje a la cola de procesamiento.
	Enqueue(ctx context.Context, msg WorkMessage) error
	// Ping verifica la conectividad.
	Ping(ctx context.Context) error
}

// WorkMessage es el payload del mensaje en Redis Streams (SPEC §11.3).
// NO lleva campo `attempts`: lo gestiona Redis vía delivery count de XPENDING.
type WorkMessage struct {
	DocumentID    string `json:"document_id"`
	ObjectKey     string `json:"object_key"`
	CorrelationID string `json:"correlation_id"`
	EnqueuedAt    string `json:"enqueued_at"`
	SchemaVersion int    `json:"schema_version"`
}

// Clock abstrae el tiempo para testeo.
type Clock interface {
	Now() time.Time
}

// SystemClock es la implementación real del reloj.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

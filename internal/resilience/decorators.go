package resilience

import (
	"context"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// Decoradores de puertos (SPEC §7.2): enrutan cada llamada por el breaker
// de su dependencia. Los rechazos del breaker salen tal cual (satisfacen
// IsRejected); los demás errores y resultados pasan sin modificación.

// --- ports.DocumentRepository (breaker de Mongo) ---

type documentRepository struct {
	inner ports.DocumentRepository
	b     *Breaker
}

var _ ports.DocumentRepository = (*documentRepository)(nil)

// NewDocumentRepository envuelve un repositorio de documentos con un
// breaker. La misma instancia de Breaker debe compartirse entre todos los
// consumidores de Mongo.
func NewDocumentRepository(inner ports.DocumentRepository, b *Breaker) ports.DocumentRepository {
	return &documentRepository{inner: inner, b: b}
}

func (d *documentRepository) Insert(ctx context.Context, doc *domain.Document) error {
	return d.b.Do(func() error { return d.inner.Insert(ctx, doc) })
}

func (d *documentRepository) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	var doc *domain.Document
	err := d.b.Do(func() error {
		var err error
		doc, err = d.inner.GetByID(ctx, id)
		return err
	})
	return doc, err
}

func (d *documentRepository) UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error) {
	var ok bool
	err := d.b.Do(func() error {
		var err error
		ok, err = d.inner.UpdateStatus(ctx, id, from, to, fields)
		return err
	})
	return ok, err
}

func (d *documentRepository) UpdateStatusWithHistory(ctx context.Context, id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any) (bool, error) {
	var ok bool
	err := d.b.Do(func() error {
		var err error
		ok, err = d.inner.UpdateStatusWithHistory(ctx, id, from, to, entry, extraSet)
		return err
	})
	return ok, err
}

func (d *documentRepository) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	var (
		docs  []*domain.Document
		total int64
	)
	err := d.b.Do(func() error {
		var err error
		docs, total, err = d.inner.List(ctx, filter)
		return err
	})
	return docs, total, err
}

func (d *documentRepository) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	var (
		docs   []*domain.Document
		cursor string
	)
	err := d.b.Do(func() error {
		var err error
		docs, cursor, err = d.inner.ListByCursor(ctx, filter)
		return err
	})
	return docs, cursor, err
}

// --- ports.ObjectStorage (breaker de MinIO) ---

type objectStorage struct {
	inner ports.ObjectStorage
	b     *Breaker
}

var _ ports.ObjectStorage = (*objectStorage)(nil)

// NewObjectStorage envuelve el almacenamiento de objetos con un breaker.
// La misma instancia de Breaker debe compartirse entre todos los
// consumidores de MinIO.
func NewObjectStorage(inner ports.ObjectStorage, b *Breaker) ports.ObjectStorage {
	return &objectStorage{inner: inner, b: b}
}

func (s *objectStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	var res *ports.PresignPostResult
	err := s.b.Do(func() error {
		var err error
		res, err = s.inner.PresignPost(ctx, objectKey, opts)
		return err
	})
	return res, err
}

func (s *objectStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	var info ports.ObjectInfo
	err := s.b.Do(func() error {
		var err error
		info, err = s.inner.Stat(ctx, objectKey)
		return err
	})
	return info, err
}

func (s *objectStorage) StatTXT(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	var info ports.ObjectInfo
	err := s.b.Do(func() error {
		var err error
		info, err = s.inner.StatTXT(ctx, objectKey)
		return err
	})
	return info, err
}

func (s *objectStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	var data []byte
	err := s.b.Do(func() error {
		var err error
		data, err = s.inner.GetRange(ctx, objectKey, start, end)
		return err
	})
	return data, err
}

func (s *objectStorage) Delete(ctx context.Context, objectKey string) error {
	return s.b.Do(func() error { return s.inner.Delete(ctx, objectKey) })
}

func (s *objectStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	var url string
	err := s.b.Do(func() error {
		var err error
		url, err = s.inner.PresignGet(ctx, objectKey, expiry)
		return err
	})
	return url, err
}

func (s *objectStorage) PresignGetTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	var url string
	err := s.b.Do(func() error {
		var err error
		url, err = s.inner.PresignGetTXT(ctx, objectKey, expiry)
		return err
	})
	return url, err
}

func (s *objectStorage) ListObjects(ctx context.Context) ([]ports.ObjectInfo, error) {
	var list []ports.ObjectInfo
	err := s.b.Do(func() error {
		var err error
		list, err = s.inner.ListObjects(ctx)
		return err
	})
	return list, err
}

func (s *objectStorage) ListObjectsTXT(ctx context.Context) ([]ports.ObjectInfo, error) {
	var list []ports.ObjectInfo
	err := s.b.Do(func() error {
		var err error
		list, err = s.inner.ListObjectsTXT(ctx)
		return err
	})
	return list, err
}

// --- ports.WorkQueue (breaker de Redis) ---

type workQueue struct {
	inner ports.WorkQueue
	b     *Breaker
}

var _ ports.WorkQueue = (*workQueue)(nil)

// NewWorkQueue envuelve la cola de trabajo con un breaker. La misma
// instancia de Breaker debe compartirse entre todos los consumidores de
// Redis. El health probe usa el adaptador concreto, nunca este decorador.
func NewWorkQueue(inner ports.WorkQueue, b *Breaker) ports.WorkQueue {
	return &workQueue{inner: inner, b: b}
}

func (q *workQueue) Enqueue(ctx context.Context, msg ports.WorkMessage) error {
	return q.b.Do(func() error { return q.inner.Enqueue(ctx, msg) })
}

func (q *workQueue) Ping(ctx context.Context) error {
	return q.b.Do(func() error { return q.inner.Ping(ctx) })
}

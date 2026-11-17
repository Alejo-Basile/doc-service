package resilience

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// --- Stubs de grabación: cuentan invocaciones y devuelven valores fijos ---

type stubRepo struct {
	calls []string
	err   error

	doc      *domain.Document
	docs     []*domain.Document
	total    int64
	cursor   string
	inserted *domain.Document
}

func (s *stubRepo) Insert(_ context.Context, doc *domain.Document) error {
	s.calls = append(s.calls, "Insert")
	s.inserted = doc
	return s.err
}

func (s *stubRepo) GetByID(_ context.Context, _ string) (*domain.Document, error) {
	s.calls = append(s.calls, "GetByID")
	return s.doc, s.err
}

func (s *stubRepo) UpdateStatus(_ context.Context, _ string, _, _ domain.Status, _ map[string]any) (bool, error) {
	s.calls = append(s.calls, "UpdateStatus")
	return true, s.err
}

func (s *stubRepo) UpdateStatusWithHistory(_ context.Context, _ string, _, _ domain.Status, _ domain.StatusEntry, _ map[string]any) (bool, error) {
	s.calls = append(s.calls, "UpdateStatusWithHistory")
	return false, s.err
}

func (s *stubRepo) List(_ context.Context, _ ports.ListFilter) ([]*domain.Document, int64, error) {
	s.calls = append(s.calls, "List")
	return s.docs, s.total, s.err
}

func (s *stubRepo) ListByCursor(_ context.Context, _ ports.CursorFilter) ([]*domain.Document, string, error) {
	s.calls = append(s.calls, "ListByCursor")
	return s.docs, s.cursor, s.err
}

type stubStorage struct {
	calls []string
	err   error

	presign *ports.PresignPostResult
	info    ports.ObjectInfo
	data    []byte
	url     string
	list    []ports.ObjectInfo
}

func (s *stubStorage) PresignPost(_ context.Context, _ string, _ ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	s.calls = append(s.calls, "PresignPost")
	return s.presign, s.err
}

func (s *stubStorage) Stat(_ context.Context, _ string) (ports.ObjectInfo, error) {
	s.calls = append(s.calls, "Stat")
	return s.info, s.err
}

func (s *stubStorage) StatTXT(_ context.Context, _ string) (ports.ObjectInfo, error) {
	s.calls = append(s.calls, "StatTXT")
	return s.info, s.err
}

func (s *stubStorage) GetRange(_ context.Context, _ string, _, _ int64) ([]byte, error) {
	s.calls = append(s.calls, "GetRange")
	return s.data, s.err
}

func (s *stubStorage) Delete(_ context.Context, _ string) error {
	s.calls = append(s.calls, "Delete")
	return s.err
}

func (s *stubStorage) DeleteTXT(_ context.Context, _ string) error {
	s.calls = append(s.calls, "DeleteTXT")
	return s.err
}

func (s *stubStorage) PresignGet(_ context.Context, _ string, _ time.Duration) (string, error) {
	s.calls = append(s.calls, "PresignGet")
	return s.url, s.err
}

func (s *stubStorage) PresignGetTXT(_ context.Context, _ string, _ time.Duration) (string, error) {
	s.calls = append(s.calls, "PresignGetTXT")
	return s.url, s.err
}

func (s *stubStorage) ListObjects(_ context.Context) ([]ports.ObjectInfo, error) {
	s.calls = append(s.calls, "ListObjects")
	return s.list, s.err
}

func (s *stubStorage) ListObjectsTXT(_ context.Context) ([]ports.ObjectInfo, error) {
	s.calls = append(s.calls, "ListObjectsTXT")
	return s.list, s.err
}

type stubQueue struct {
	calls []string
	err   error

	enqueued ports.WorkMessage
}

func (s *stubQueue) Enqueue(_ context.Context, msg ports.WorkMessage) error {
	s.calls = append(s.calls, "Enqueue")
	s.enqueued = msg
	return s.err
}

func (s *stubQueue) Ping(_ context.Context) error {
	s.calls = append(s.calls, "Ping")
	return s.err
}

// --- Test 3: reenvío íntegro de resultados y errores ---

func TestDecorators_ForwardResultsAndErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("DocumentRepository", func(t *testing.T) {
		doc := &domain.Document{ID: "doc-1"}
		docs := []*domain.Document{doc}
		inner := &stubRepo{doc: doc, docs: docs, total: 7, cursor: "cursor-1"}
		decorated := NewDocumentRepository(inner, NewBreakerWithSettings(tripAtTres()))

		if err := decorated.Insert(ctx, doc); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		if inner.inserted != doc {
			t.Errorf("Insert: el documento no se reenvió al inner")
		}
		if got, err := decorated.GetByID(ctx, "doc-1"); err != nil || got != doc {
			t.Errorf("GetByID: got (%v, %v), want (%v, nil)", got, err, doc)
		}
		if ok, err := decorated.UpdateStatus(ctx, "doc-1", domain.StatusPendingUpload, domain.StatusQueued, nil); !ok || err != nil {
			t.Errorf("UpdateStatus: got (%v, %v), want (true, nil)", ok, err)
		}
		// (false, nil) del inner no es error: debe pasar sin clasificación.
		if ok, err := decorated.UpdateStatusWithHistory(ctx, "doc-1", domain.StatusQueued, domain.StatusProcessing, domain.StatusEntry{}, nil); ok || err != nil {
			t.Errorf("UpdateStatusWithHistory: got (%v, %v), want (false, nil)", ok, err)
		}
		if gotDocs, total, err := decorated.List(ctx, ports.ListFilter{}); err != nil || total != 7 || !sameDocPtrs(gotDocs, docs) {
			t.Errorf("List: got (%v, %d, %v), want (mismo slice, 7, nil)", gotDocs, total, err)
		}
		if gotDocs, cursor, err := decorated.ListByCursor(ctx, ports.CursorFilter{}); err != nil || cursor != "cursor-1" || !sameDocPtrs(gotDocs, docs) {
			t.Errorf("ListByCursor: got (_, %q, %v), want (_, %q, nil)", cursor, err, "cursor-1")
		}

		wantCalls := []string{"Insert", "GetByID", "UpdateStatus", "UpdateStatusWithHistory", "List", "ListByCursor"}
		if !equalStrings(inner.calls, wantCalls) {
			t.Errorf("métodos invocados: %v, want %v", inner.calls, wantCalls)
		}
	})

	t.Run("ObjectStorage", func(t *testing.T) {
		presign := &ports.PresignPostResult{UploadURL: "https://s3/upload"}
		info := ports.ObjectInfo{Key: "raw-pdfs/doc-1.pdf", Size: 123}
		data := []byte("%PDF-1.4")
		list := []ports.ObjectInfo{info}
		inner := &stubStorage{presign: presign, info: info, data: data, url: "https://s3/get", list: list}
		decorated := NewObjectStorage(inner, NewBreakerWithSettings(tripAtTres()))

		if got, err := decorated.PresignPost(ctx, "raw-pdfs/doc-1.pdf", ports.PresignPostOptions{}); err != nil || got != presign {
			t.Errorf("PresignPost: got (%v, %v), want (%v, nil)", got, err, presign)
		}
		if got, err := decorated.Stat(ctx, "raw-pdfs/doc-1.pdf"); err != nil || got != info {
			t.Errorf("Stat: got (%v, %v), want (%v, nil)", got, err, info)
		}
		if got, err := decorated.StatTXT(ctx, "extracted-txt/doc-1.txt"); err != nil || got != info {
			t.Errorf("StatTXT: got (%v, %v), want (%v, nil)", got, err, info)
		}
		if got, err := decorated.GetRange(ctx, "raw-pdfs/doc-1.pdf", 0, 8); err != nil || !bytes.Equal(got, data) {
			t.Errorf("GetRange: got (%q, %v), want (%q, nil)", got, err, data)
		}
		if err := decorated.Delete(ctx, "raw-pdfs/doc-1.pdf"); err != nil {
			t.Errorf("Delete: %v", err)
		}
		if err := decorated.DeleteTXT(ctx, "extracted-txt/doc-1.txt"); err != nil {
			t.Errorf("DeleteTXT: %v", err)
		}
		if got, err := decorated.PresignGet(ctx, "raw-pdfs/doc-1.pdf", time.Minute); err != nil || got != "https://s3/get" {
			t.Errorf("PresignGet: got (%q, %v), want (%q, nil)", got, err, "https://s3/get")
		}
		if got, err := decorated.PresignGetTXT(ctx, "extracted-txt/doc-1.txt", time.Minute); err != nil || got != "https://s3/get" {
			t.Errorf("PresignGetTXT: got (%q, %v), want (%q, nil)", got, err, "https://s3/get")
		}
		if got, err := decorated.ListObjects(ctx); err != nil || !sameInfo(got, list) {
			t.Errorf("ListObjects: got (%v, %v), want (mismo slice, nil)", got, err)
		}
		if got, err := decorated.ListObjectsTXT(ctx); err != nil || !sameInfo(got, list) {
			t.Errorf("ListObjectsTXT: got (%v, %v), want (mismo slice, nil)", got, err)
		}

		wantCalls := []string{"PresignPost", "Stat", "StatTXT", "GetRange", "Delete", "DeleteTXT", "PresignGet", "PresignGetTXT", "ListObjects", "ListObjectsTXT"}
		if !equalStrings(inner.calls, wantCalls) {
			t.Errorf("métodos invocados: %v, want %v", inner.calls, wantCalls)
		}
	})

	t.Run("WorkQueue", func(t *testing.T) {
		msg := ports.WorkMessage{
			DocumentID:    "doc-1",
			ObjectKey:     "raw-pdfs/doc-1.pdf",
			CorrelationID: "corr-1",
			EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
			SchemaVersion: 1,
		}
		inner := &stubQueue{}
		decorated := NewWorkQueue(inner, NewBreakerWithSettings(tripAtTres()))

		if err := decorated.Enqueue(ctx, msg); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if inner.enqueued != msg {
			t.Errorf("Enqueue: el mensaje no se reenvió intacto: got %+v want %+v", inner.enqueued, msg)
		}
		if err := decorated.Ping(ctx); err != nil {
			t.Errorf("Ping: %v", err)
		}
		if !equalStrings(inner.calls, []string{"Enqueue", "Ping"}) {
			t.Errorf("métodos invocados: %v, want [Enqueue Ping]", inner.calls)
		}
	})
}

// --- Test 4: ningún método se salta el breaker ---

func TestDecorators_AllMethodsRouteThroughBreaker(t *testing.T) {
	ctx := context.Background()
	const tripThreshold = 3

	t.Run("DocumentRepository", func(t *testing.T) {
		inner := &stubRepo{err: transientErr}
		decorated := NewDocumentRepository(inner, NewBreakerWithSettings(tripAtTres()))
		doc := &domain.Document{ID: "doc-1"}
		entry := domain.StatusEntry{}

		ops := []op{
			{"Insert", func() error { return decorated.Insert(ctx, doc) }},
			{"GetByID", func() error { _, err := decorated.GetByID(ctx, "doc-1"); return err }},
			{"UpdateStatus", func() error {
				_, err := decorated.UpdateStatus(ctx, "doc-1", domain.StatusPendingUpload, domain.StatusQueued, nil)
				return err
			}},
			{"UpdateStatusWithHistory", func() error {
				_, err := decorated.UpdateStatusWithHistory(ctx, "doc-1", domain.StatusQueued, domain.StatusProcessing, entry, nil)
				return err
			}},
			{"List", func() error { _, _, err := decorated.List(ctx, ports.ListFilter{}); return err }},
			{"ListByCursor", func() error { _, _, err := decorated.ListByCursor(ctx, ports.CursorFilter{}); return err }},
		}
		runOpsThroughBreaker(t, ops, func() int { return len(inner.calls) }, tripThreshold)
	})

	t.Run("ObjectStorage", func(t *testing.T) {
		inner := &stubStorage{err: transientErr}
		decorated := NewObjectStorage(inner, NewBreakerWithSettings(tripAtTres()))

		ops := []op{
			{"PresignPost", func() error {
				_, err := decorated.PresignPost(ctx, "k", ports.PresignPostOptions{})
				return err
			}},
			{"Stat", func() error { _, err := decorated.Stat(ctx, "k"); return err }},
			{"StatTXT", func() error { _, err := decorated.StatTXT(ctx, "k"); return err }},
			{"GetRange", func() error { _, err := decorated.GetRange(ctx, "k", 0, 1); return err }},
			{"Delete", func() error { return decorated.Delete(ctx, "k") }},
			{"DeleteTXT", func() error { return decorated.DeleteTXT(ctx, "k") }},
			{"PresignGet", func() error { _, err := decorated.PresignGet(ctx, "k", time.Minute); return err }},
			{"PresignGetTXT", func() error { _, err := decorated.PresignGetTXT(ctx, "k", time.Minute); return err }},
			{"ListObjects", func() error { _, err := decorated.ListObjects(ctx); return err }},
			{"ListObjectsTXT", func() error { _, err := decorated.ListObjectsTXT(ctx); return err }},
		}
		runOpsThroughBreaker(t, ops, func() int { return len(inner.calls) }, tripThreshold)
	})

	t.Run("WorkQueue", func(t *testing.T) {
		// Solo 2 métodos: se repiten para superar el umbral de 3.
		inner := &stubQueue{err: transientErr}
		decorated := NewWorkQueue(inner, NewBreakerWithSettings(tripAtTres()))
		msg := ports.WorkMessage{DocumentID: "doc-1", SchemaVersion: 1}

		ops := []op{
			{"Enqueue#1", func() error { return decorated.Enqueue(ctx, msg) }},
			{"Ping#1", func() error { return decorated.Ping(ctx) }},
			{"Enqueue#2", func() error { return decorated.Enqueue(ctx, msg) }},
			{"Ping#2", func() error { return decorated.Ping(ctx) }},
		}
		runOpsThroughBreaker(t, ops, func() int { return len(inner.calls) }, tripThreshold)
	})
}

// op es una invocación genérica de un método del decorador.
type op struct {
	name string
	run  func() error
}

// runOpsThroughBreaker ejecuta cada op en orden y verifica que las primeras
// threshold llamadas llegan al inner (error transitorio) y las siguientes
// son rechazadas por el breaker sin tocar el inner. callCount devuelve la
// cantidad de invocaciones registradas por el stub en cada momento.
func runOpsThroughBreaker(t *testing.T, ops []op, callCount func() int, threshold int) {
	t.Helper()

	innerCalls := 0
	for _, o := range ops {
		err := o.run()
		if innerCalls < threshold {
			if !errors.Is(err, transientErr) {
				t.Fatalf("%s: llamado %d: error debe ser el transitorio del inner, got: %v", o.name, innerCalls+1, err)
			}
			if IsRejected(err) {
				t.Fatalf("%s: llamado %d: el error del inner no debe marcarse como rechazo", o.name, innerCalls+1)
			}
			innerCalls++
		} else {
			if !IsRejected(err) {
				t.Fatalf("%s: llamado %d: se esperaba rechazo del breaker, got: %v", o.name, innerCalls+1, err)
			}
		}
	}

	if got := callCount(); got != threshold {
		t.Errorf("el inner debe ser invocado exactamente %d veces, got: %d", threshold, got)
	}
}

func sameDocPtrs(a, b []*domain.Document) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameInfo(a, b []ports.ObjectInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

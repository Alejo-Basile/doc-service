package reconciler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/metrics"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
	minio "github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// mockClock implementa ports.Clock para tests.
type mockClock struct{ t time.Time }

func (c *mockClock) Now() time.Time { return c.t }

// mockRepo implementa ports.DocumentRepository.
type mockRepo struct {
	docs        map[string]*domain.Document
	updateCalls []updateCall
	// order, si no es nil, registra "transition" para probar el orden
	// transición → encolado en los sweeps del reconciliador.
	order *[]string
}

type updateCall struct {
	id   string
	from domain.Status
	to   domain.Status
}

func newMockRepo() *mockRepo {
	return &mockRepo{docs: make(map[string]*domain.Document)}
}

func (m *mockRepo) Insert(ctx context.Context, doc *domain.Document) error {
	m.docs[doc.ID] = doc
	return nil
}

func (m *mockRepo) GetByID(ctx context.Context, id string) (*domain.Document, error) {
	doc, ok := m.docs[id]
	if !ok {
		return nil, ports.ErrNotFound
	}
	return doc, nil
}

func (m *mockRepo) UpdateStatus(ctx context.Context, id string, from, to domain.Status, fields map[string]any) (bool, error) {
	m.updateCalls = append(m.updateCalls, updateCall{id: id, from: from, to: to})
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	doc.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (m *mockRepo) List(ctx context.Context, filter ports.ListFilter) ([]*domain.Document, int64, error) {
	var result []*domain.Document
	for _, doc := range m.docs {
		if filter.Status == "" || doc.Status == filter.Status {
			result = append(result, doc)
		}
	}
	return result, int64(len(result)), nil
}

func (m *mockRepo) ListByCursor(ctx context.Context, filter ports.CursorFilter) ([]*domain.Document, string, error) {
	var result []*domain.Document
	for _, doc := range m.docs {
		if filter.Status == "" || doc.Status == filter.Status {
			result = append(result, doc)
		}
	}
	return result, "", nil
}

func (m *mockRepo) UpdateStatusWithHistory(ctx context.Context, id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any) (bool, error) {
	m.updateCalls = append(m.updateCalls, updateCall{id: id, from: from, to: to})
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	if m.order != nil {
		*m.order = append(*m.order, "transition")
	}
	doc.Status = to
	doc.UpdatedAt = time.Now().UTC()
	// Espeja al adaptador Mongo: failure_reason se deriva de entry.Reason
	// cuando el destino es un estado de fallo.
	if entry.Reason != "" && (to == domain.StatusFailed ||
		to == domain.StatusRejected || to == domain.StatusExtractionFailed) {
		doc.FailureReason = entry.Reason
	}
	entry.At = time.Now().UTC()
	doc.History = append(doc.History, entry)
	return true, nil
}

// mockStorage implementa ports.ObjectStorage (y el listado opcional).
type mockStorage struct {
	objects    map[string]bool // key → exists
	mdObjects  map[string]bool
	txtObjects map[string]bool // objetos del bucket legacy extracted-txt (D5)
	pdfData    []byte
	// statErr permite simular errores de infraestructura (p. ej. NoSuchBucket).
	statErr error
	// listObjects soporta la aserción `lister` de purgeOrphanObjects (S4-P2-02).
	listObjects   []ports.ObjectInfo
	listObjectsMD []ports.ObjectInfo
	deleted       []string
	deletedMD     []string // purgas del bucket extracted-md (T2, SPEC §6.5)
}

func newMockStorage() *mockStorage {
	return &mockStorage{objects: make(map[string]bool)}
}

// mockQueue implementa ports.WorkQueue para tests.
type mockQueue struct {
	enqueued []ports.WorkMessage
	// order, si no es nil, registra "enqueue" para probar el orden de efectos.
	order *[]string
}

func (m *mockQueue) Enqueue(_ context.Context, msg ports.WorkMessage) error {
	if m.order != nil {
		*m.order = append(*m.order, "enqueue")
	}
	m.enqueued = append(m.enqueued, msg)
	return nil
}

func (m *mockQueue) Ping(_ context.Context) error { return nil }

// mockLease implementa ports.DistributedLease para tests (S4-P2-03).
type mockLease struct {
	acquired   bool
	acquireErr error
	acquires   int
	releases   int
}

func (m *mockLease) Acquire(_ context.Context, _ string, _ time.Duration) (bool, error) {
	m.acquires++
	return m.acquired, m.acquireErr
}

func (m *mockLease) Release(_ context.Context, _ string) error {
	m.releases++
	return nil
}

func (m *mockStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	return &ports.PresignPostResult{}, nil
}

func (m *mockStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	if m.statErr != nil {
		return ports.ObjectInfo{}, m.statErr
	}
	if m.objects[objectKey] {
		return ports.ObjectInfo{Key: objectKey}, nil
	}
	return ports.ObjectInfo{}, errNotFound
}

func (m *mockStorage) StatMD(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	if m.statErr != nil {
		return ports.ObjectInfo{}, m.statErr
	}
	bucket := m.objects
	if m.mdObjects != nil {
		bucket = m.mdObjects
	}
	if bucket[objectKey] {
		return ports.ObjectInfo{Key: objectKey}, nil
	}
	return ports.ObjectInfo{}, errNotFound
}

func (m *mockStorage) StatLegacyTXT(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	if m.statErr != nil {
		return ports.ObjectInfo{}, m.statErr
	}
	if m.txtObjects != nil && m.txtObjects[objectKey] {
		return ports.ObjectInfo{Key: objectKey}, nil
	}
	return ports.ObjectInfo{}, errNotFound
}

func (m *mockStorage) PresignGetLegacyTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) ListObjectsMD(ctx context.Context) ([]ports.ObjectInfo, error) {
	return m.listObjectsMD, nil
}

func (m *mockStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	if m.pdfData != nil {
		return m.pdfData, nil
	}
	return []byte("%PDF-"), nil
}

func (m *mockStorage) Delete(ctx context.Context, objectKey string) error {
	m.deleted = append(m.deleted, objectKey)
	delete(m.objects, objectKey)
	return nil
}

func (m *mockStorage) DeleteMD(ctx context.Context, objectKey string) error {
	m.deletedMD = append(m.deletedMD, objectKey)
	delete(m.mdObjects, objectKey)
	return nil
}

func (m *mockStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) PresignGetMD(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) ListObjects(ctx context.Context) ([]ports.ObjectInfo, error) {
	return m.listObjects, nil
}

// errNotFound simula "not found" del SDK MinIO.
type notFoundError struct{}

func (notFoundError) Error() string { return "Object not found" }

var errNotFound = notFoundError{}

func TestRunOnce_ExpiredPending_ObjectAbsent(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage() // sin objetos: todo "no existe"

	doc := &domain.Document{
		ID:        "doc-expired",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-expired.pdf",
		ExpiresAt: now.Add(-time.Hour), // vencido hace 1h
	}
	repo.docs["doc-expired"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.ExpiredPending != 1 {
		t.Errorf("ExpiredPending = %d, want 1", result.ExpiredPending)
	}
	if result.RecoveredUploaded != 0 {
		t.Errorf("RecoveredUploaded = %d, want 0", result.RecoveredUploaded)
	}

	// Verificar transición a UPLOAD_EXPIRED.
	found := false
	for _, call := range repo.updateCalls {
		if call.id == "doc-expired" && call.to == domain.StatusUploadExpired {
			found = true
		}
	}
	if !found {
		t.Errorf("no se transicionó a UPLOAD_EXPIRED; calls=%+v", repo.updateCalls)
	}
}

// TestRunOnce_ExpiredPending_BucketAusente_NoExpira fija que un 404 de
// infraestructura (NoSuchBucket) no se confunda con "objeto ausente": el
// documento debe quedar intacto para que el próximo sweep lo reintente.
func TestRunOnce_ExpiredPending_BucketAusente_NoExpira(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	storage.statErr = minio.ErrorResponse{
		Code:       "NoSuchBucket",
		Message:    "The specified bucket does not exist.",
		StatusCode: http.StatusNotFound,
	}

	doc := &domain.Document{
		ID:        "doc-bucket",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-bucket.pdf",
		ExpiresAt: now.Add(-time.Hour),
	}
	repo.docs["doc-bucket"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.ExpiredPending != 0 {
		t.Errorf("ExpiredPending = %d, want 0 (NoSuchBucket no es objeto ausente)", result.ExpiredPending)
	}
	for _, call := range repo.updateCalls {
		if call.id == "doc-bucket" && call.to == domain.StatusUploadExpired {
			t.Errorf("el documento no debía expirar por un 404 de bucket; calls=%+v", repo.updateCalls)
		}
	}
}

func TestRunOnce_ExpiredPending_ObjectExists_RecoversToUploaded(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	storage.objects["raw-pdfs/doc-1.pdf"] = true // objeto existe

	doc := &domain.Document{
		ID:        "doc-1",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-1.pdf",
		ExpiresAt: now.Add(-time.Hour),
	}
	repo.docs["doc-1"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.RecoveredUploaded != 1 {
		t.Errorf("RecoveredUploaded = %d, want 1", result.RecoveredUploaded)
	}

	// Verificar transición a UPLOADED.
	found := false
	for _, call := range repo.updateCalls {
		if call.id == "doc-1" && call.to == domain.StatusUploaded {
			found = true
		}
	}
	if !found {
		t.Errorf("no se transicionó a UPLOADED; calls=%+v", repo.updateCalls)
	}
}

func TestRunOnce_ExpiredPending_ObjectExists_NotPDF_Rejected(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	storage.objects["raw-pdfs/doc-1.pdf"] = true
	storage.pdfData = []byte("NOTPDF") // no es PDF

	doc := &domain.Document{
		ID:        "doc-1",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-1.pdf",
		ExpiresAt: now.Add(-time.Hour),
	}
	repo.docs["doc-1"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.RecoveredUploaded != 0 {
		t.Errorf("RecoveredUploaded = %d, want 0 (no es PDF)", result.RecoveredUploaded)
	}

	// Verificar transición a REJECTED.
	found := false
	for _, call := range repo.updateCalls {
		if call.id == "doc-1" && call.to == domain.StatusRejected {
			found = true
		}
	}
	if !found {
		t.Errorf("no se transicionó a REJECTED; calls=%+v", repo.updateCalls)
	}
}

func TestRunOnce_NotExpired_Skipped(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	doc := &domain.Document{
		ID:        "doc-active",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-active.pdf",
		ExpiresAt: now.Add(time.Hour), // aún válido
	}
	repo.docs["doc-active"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.ExpiredPending != 0 {
		t.Errorf("ExpiredPending = %d, want 0", result.ExpiredPending)
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0", len(repo.updateCalls))
	}
}

func TestRunOnce_StuckIntermediate_QueuedSoloDetecta(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// QUEUED queda fuera de T3 (su sweep dedicado lo reencola sin transicionar):
	// el sweep de estados intermedios solo lo detecta y loguea.
	doc := &domain.Document{
		ID:        "doc-stuck",
		Status:    domain.StatusQueued,
		ObjectKey: "raw-pdfs/doc-stuck.pdf",
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs["doc-stuck"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.StuckDetected != 1 {
		t.Errorf("StuckDetected = %d, want 1", result.StuckDetected)
	}

	// QUEUED no se muta desde este sweep.
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (QUEUED fuera de scope T3)", len(repo.updateCalls))
	}
	if doc.Status != domain.StatusQueued {
		t.Errorf("estado final = %s, want QUEUED (sin cambios)", doc.Status)
	}
}

func TestRunOnce_StuckIntermediate_Recent_NotDetected(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento en PROCESSING con updated_at hace 5min (umbral: 30min).
	doc := &domain.Document{
		ID:        "doc-recent",
		Status:    domain.StatusProcessing,
		ObjectKey: "raw-pdfs/doc-recent.pdf",
		UpdatedAt: now.Add(-5 * time.Minute),
	}
	repo.docs["doc-recent"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.StuckDetected != 0 {
		t.Errorf("StuckDetected = %d, want 0", result.StuckDetected)
	}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "error de red genérico", err: errors.New("connection refused"), want: false},
		{name: "SDK NoSuchKey (type-assert del error del SDK)", err: minio.ErrorResponse{Code: "NoSuchKey", Message: "The specified key does not exist.", StatusCode: 404}, want: true},
		{name: "SDK NoSuchBucket no es objeto ausente", err: minio.ErrorResponse{Code: "NoSuchBucket", StatusCode: http.StatusNotFound}, want: false},
		{name: "SDK AccessDenied 404 no es objeto ausente", err: minio.ErrorResponse{Code: "AccessDenied", StatusCode: http.StatusNotFound}, want: false},
		{name: "SDK 404 sin código (fallback) es objeto ausente", err: minio.ErrorResponse{StatusCode: http.StatusNotFound}, want: true},
		{name: "SDK error distinto", err: minio.ErrorResponse{Code: "InternalError", StatusCode: http.StatusInternalServerError}, want: false},
		{name: "SDK NoSuchKey envuelto", err: fmt.Errorf("stat bucket/key: %w", minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}), want: true},
		{name: "mensaje not found", err: notFoundError{}, want: true},
		{name: "mensaje Object not found", err: errString("Object not found"), want: true},
		{name: "mensaje NoSuchKey", err: errString("NoSuchKey"), want: true},
		{name: "mensaje 404 sin NoSuchKey", err: errString("unexpected 404"), want: false},
		{name: "mensaje connection refused", err: errString("connection refused"), want: false},
	}

	for _, tt := range tests {
		if got := isNotFound(tt.err); got != tt.want {
			t.Errorf("isNotFound(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// --- Safety age (S6-P2-02) ---

func TestRunOnce_SafetyAge_NoTocaDocumentosRecientes(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage() // sin objetos

	// Documento vencido pero creado hace 5 min (menor que safety age de 15 min).
	doc := &domain.Document{
		ID:        "doc-young",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-young.pdf",
		ExpiresAt: now.Add(-time.Hour),       // vencido
		CreatedAt: now.Add(-5 * time.Minute), // pero muy reciente
	}
	repo.docs["doc-young"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	// No debe tocar el documento por safety age.
	if result.ExpiredPending != 0 {
		t.Errorf("ExpiredPending = %d, want 0 (safety age)", result.ExpiredPending)
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (safety age)", len(repo.updateCalls))
	}
}

// --- Completed sin .md (S5-P2-03) ---

func TestRunOnce_CompletedSinMd_Detectado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento COMPLETED sin md_ref, creado hace 1h (pasó safety age).
	doc := &domain.Document{
		ID:        "doc-no-md",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-no-md.pdf",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-no-md"] = doc

	// Segundo documento COMPLETED con md_ref que no existe en MinIO.
	doc2 := &domain.Document{
		ID:        "doc-md-missing",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-md-missing.pdf",
		MdRef:     "extracted-md/doc-md-missing.md",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-md-missing"] = doc2

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	// Ambos deben ser detectados.
	if result.CompletedNoMd != 2 {
		t.Errorf("CompletedNoMd = %d, want 2", result.CompletedNoMd)
	}

	// No debe transicionar (solo detección).
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (solo detección)", len(repo.updateCalls))
	}
}

// --- Cross-check (S6-P2-01) ---

func TestCrossCheck_GeneraReporte(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento FAILED sin failure_reason.
	docFailed := &domain.Document{
		ID:        "doc-failed",
		Status:    domain.StatusFailed,
		CreatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-failed"] = docFailed

	// Documento COMPLETED sin md_ref.
	docCompleted := &domain.Document{
		ID:        "doc-completed",
		Status:    domain.StatusCompleted,
		CreatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-completed"] = docCompleted

	// Documento joven (no tocar).
	docYoung := &domain.Document{
		ID:        "doc-young",
		Status:    domain.StatusPendingUpload,
		CreatedAt: now.Add(-2 * time.Minute),
	}
	repo.docs["doc-young"] = docYoung

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	report, err := r.crossCheck(context.Background(), now)
	if err != nil {
		t.Fatalf("crossCheck error: %v", err)
	}

	if report.TotalDocuments != 3 {
		t.Errorf("TotalDocuments = %d, want 3", report.TotalDocuments)
	}
	if len(report.FailedNoReason) != 1 {
		t.Errorf("FailedNoReason = %v, want 1 doc", report.FailedNoReason)
	}
	if len(report.CompletedNoMd) != 1 {
		t.Errorf("CompletedNoMd = %v, want 1 doc", report.CompletedNoMd)
	}
	if report.YoungUntouched != 1 {
		t.Errorf("YoungUntouched = %d, want 1", report.YoungUntouched)
	}
}

// TestCrossCheck_DatosSanos_ReporteVacio verifica el caso "todo consistente":
// MongoDB, MinIO y Redis cuadran, así que ninguna lista de discrepancias debe
// tener elementos (S6-P2-01). Es la señal de alerta: si este reporte deja de
// estar vacío en el entorno real, hay una inconsistencia que investigar.
func TestCrossCheck_DatosSanos_ReporteVacio(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	// Documento COMPLETED sano: tiene object_key y md_ref, ambos existen.
	completedKey := "raw-pdfs/doc-completed.pdf"
	completedMd := "extracted-md/doc-completed.md"
	repo.docs["doc-completed"] = &domain.Document{
		ID:        "doc-completed",
		Status:    domain.StatusCompleted,
		ObjectKey: completedKey,
		MdRef:     completedMd,
		CreatedAt: now.Add(-time.Hour),
	}

	// Documento activo (PROCESSING) con su objeto presente.
	activeKey := "raw-pdfs/doc-active.pdf"
	repo.docs["doc-active"] = &domain.Document{
		ID:        "doc-active",
		Status:    domain.StatusProcessing,
		ObjectKey: activeKey,
		CreatedAt: now.Add(-time.Hour),
	}

	// MinIO tiene exactamente los mismos objetos que referencian los documentos.
	storage.objects[completedKey] = true
	storage.objects[completedMd] = true
	storage.objects[activeKey] = true
	storage.listObjects = []ports.ObjectInfo{
		{Key: completedKey},
		{Key: activeKey},
	}

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	report, err := r.crossCheck(context.Background(), now)
	if err != nil {
		t.Fatalf("crossCheck error: %v", err)
	}

	if report.TotalDocuments != 2 {
		t.Errorf("TotalDocuments = %d, want 2", report.TotalDocuments)
	}
	if report.RawBucketObjects != 2 {
		t.Errorf("RawBucketObjects = %d, want 2", report.RawBucketObjects)
	}
	if report.ByStatus[string(domain.StatusCompleted)] != 1 ||
		report.ByStatus[string(domain.StatusProcessing)] != 1 {
		t.Errorf("ByStatus = %v, want COMPLETED=1 PROCESSING=1", report.ByStatus)
	}

	if len(report.OrphanRawObjects) != 0 {
		t.Errorf("OrphanRawObjects = %v, want 0", report.OrphanRawObjects)
	}
	if len(report.MissingRawObjects) != 0 {
		t.Errorf("MissingRawObjects = %v, want 0", report.MissingRawObjects)
	}
	if len(report.CompletedNoMd) != 0 {
		t.Errorf("CompletedNoMd = %v, want 0", report.CompletedNoMd)
	}
	if len(report.FailedNoReason) != 0 {
		t.Errorf("FailedNoReason = %v, want 0", report.FailedNoReason)
	}
	if report.YoungUntouched != 0 {
		t.Errorf("YoungUntouched = %d, want 0 (ambos pasan el umbral de seguridad)", report.YoungUntouched)
	}
}

// --- Failure reasons normalizados (S5-P2-02) ---

func TestIsValidFailureReason(t *testing.T) {
	valid := []string{
		domain.FailureReasonObjectMissing,
		domain.FailureReasonNotAPDF,
		domain.FailureReasonUploadExpired,
		domain.FailureReasonCancelled,
		domain.FailureReasonExtractionError,
		domain.FailureReasonCompensateFail,
		domain.FailureReasonTxtMissing,
	}
	for _, r := range valid {
		if !domain.IsValidFailureReason(r) {
			t.Errorf("IsValidFailureReason(%q) = false, want true", r)
		}
	}

	invalid := []string{"", "SOME_RANDOM_ERROR", "not_a_reason"}
	for _, r := range invalid {
		if domain.IsValidFailureReason(r) {
			t.Errorf("IsValidFailureReason(%q) = true, want false", r)
		}
	}
}

// TestRegisterRoutes_ReconcileRequiereToken verifica que el endpoint manual
// POST /internal/reconcile quede protegido por el token interno (SPEC §9):
// sin token responde 401 y no ejecuta el sweep; con token responde 200.
func TestRegisterRoutes_ReconcileRequiereToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const token = "internal-test-token"

	repo := newMockRepo()
	storage := newMockStorage()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := New(repo, storage, nil, nil, &mockClock{now},
		10*time.Minute, 30*time.Minute, 15*time.Minute, WithInternalToken(token))

	engine := gin.New()
	r.RegisterRoutes(engine)

	do := func(tok string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/internal/reconcile", nil)
		if tok != "" {
			req.Header.Set("X-Internal-Token", tok)
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, req)
		return w
	}

	if w := do(""); w.Code != http.StatusUnauthorized {
		t.Fatalf("sin token: status = %d, want 401", w.Code)
	}
	if w := do("token-incorrecto"); w.Code != http.StatusUnauthorized {
		t.Fatalf("token inválido: status = %d, want 401", w.Code)
	}
	if w := do(token); w.Code != http.StatusOK {
		t.Fatalf("token válido: status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
}

// --- Modo dry-run (SPEC §6.5, T1) ---

// TestRunOnce_DryRun_DetectaPeroNoMuta cubre el contrato central del dry-run:
// la corrida detecta igual que en modo normal (contenedores del Result en
// verde) pero no ejecuta NINGUNA mutación: 0 updates, 0 enqueues, 0 deletes.
// El reporte debe distinguir acciones planificadas de ejecutadas.
func TestRunOnce_DryRun_DetectaPeroNoMuta(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	// 1) PENDING_UPLOAD vencido sin objeto → planifica UPLOAD_EXPIRED.
	repo.docs["doc-expired"] = &domain.Document{
		ID:        "doc-expired",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-expired.pdf",
		ExpiresAt: now.Add(-time.Hour),
		CreatedAt: now.Add(-2 * time.Hour),
	}
	// 2) PENDING_UPLOAD vencido con PDF válido → planifica UPLOADED.
	repo.docs["doc-recover"] = &domain.Document{
		ID:        "doc-recover",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-recover.pdf",
		ExpiresAt: now.Add(-time.Hour),
		CreatedAt: now.Add(-2 * time.Hour),
	}
	storage.objects["raw-pdfs/doc-recover.pdf"] = true
	// 3) UPLOADED estancado con objeto → planifica QUEUED + Enqueue.
	repo.docs["doc-uploaded"] = &domain.Document{
		ID:        "doc-uploaded",
		Status:    domain.StatusUploaded,
		ObjectKey: "raw-pdfs/doc-uploaded.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	storage.objects["raw-pdfs/doc-uploaded.pdf"] = true
	// 4) QUEUED estancado → planifica Enqueue (sin transición).
	repo.docs["doc-queued"] = &domain.Document{
		ID:        "doc-queued",
		Status:    domain.StatusQueued,
		ObjectKey: "raw-pdfs/doc-queued.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	// 5) Huérfano antiguo en MinIO → planifica Delete.
	storage.listObjects = []ports.ObjectInfo{
		{Key: "raw-pdfs/huerfano-viejo.pdf", LastModified: now.Add(-30 * 24 * time.Hour)},
	}

	r := New(repo, storage, queue, nil, &mockClock{now},
		10*time.Minute, 30*time.Minute, 15*time.Minute, WithDryRun(true))
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	// Detección intacta: los contadores reportan lo que se habría hecho.
	if result.ExpiredPending != 1 || result.RecoveredUploaded != 1 {
		t.Errorf("detección PENDING_UPLOAD = (expired %d, recovered %d), want (1, 1)",
			result.ExpiredPending, result.RecoveredUploaded)
	}
	if result.Reenqueued != 2 {
		t.Errorf("Reenqueued = %d, want 2 (UPLOADED + QUEUED estancados)", result.Reenqueued)
	}
	if result.OrphansPurged != 1 {
		t.Errorf("OrphansPurged = %d, want 1 (huérfano antiguo detectado)", result.OrphansPurged)
	}

	// Cero mutaciones.
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %+v, want 0 en dry-run", repo.updateCalls)
	}
	if len(queue.enqueued) != 0 {
		t.Errorf("enqueued = %d, want 0 en dry-run", len(queue.enqueued))
	}
	if len(storage.deleted) != 0 {
		t.Errorf("deleted = %v, want 0 en dry-run", storage.deleted)
	}

	// Reporte planificado vs ejecutado.
	if !result.DryRun {
		t.Error("DryRun = false en el reporte, want true")
	}
	if result.PlannedActions == 0 {
		t.Error("PlannedActions = 0, want > 0 (el plan debe reportarse)")
	}
	if result.ExecutedActions != 0 {
		t.Errorf("ExecutedActions = %d, want 0 en dry-run", result.ExecutedActions)
	}

	// Los documentos quedan exactamente como estaban.
	if doc := repo.docs["doc-expired"]; doc.Status != domain.StatusPendingUpload {
		t.Errorf("doc-expired status = %s, want PENDING_UPLOAD (intacto)", doc.Status)
	}
	if doc := repo.docs["doc-uploaded"]; doc.Status != domain.StatusUploaded {
		t.Errorf("doc-uploaded status = %s, want UPLOADED (intacto)", doc.Status)
	}
}

// TestRunOnce_ModoEjecucion_ReportaPlanificadoYEjecutado fija el otro lado del
// contrato: sin dry-run, el reporte cuenta las acciones efectivamente ejecutadas.
func TestRunOnce_ModoEjecucion_ReportaPlanificadoYEjecutado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:        "doc-uploaded",
		Status:    domain.StatusUploaded,
		ObjectKey: "raw-pdfs/doc-uploaded.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs["doc-uploaded"] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.DryRun {
		t.Error("DryRun = true sin configurarlo")
	}
	// Transición UPLOADED→QUEUED + un Enqueue.
	if result.PlannedActions != 2 {
		t.Errorf("PlannedActions = %d, want 2 (transición + enqueue)", result.PlannedActions)
	}
	if result.ExecutedActions != 2 {
		t.Errorf("ExecutedActions = %d, want 2 (ambas acciones se ejecutaron)", result.ExecutedActions)
	}
	if doc.Status != domain.StatusQueued || len(queue.enqueued) != 1 {
		t.Errorf("estado/encolado = (%s, %d), want (QUEUED, 1)", doc.Status, len(queue.enqueued))
	}
}

// TestRegisterRoutes_ReconcileDryRunOverride verifica que ?dry_run=1 fuerza el
// modo planificado aunque la config esté en modo ejecución, y que la respuesta
// serializa el reporte (dry_run + contadores).
func TestRegisterRoutes_ReconcileDryRunOverride(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const token = "internal-test-token"

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}
	repo.docs["doc-expired"] = &domain.Document{
		ID:        "doc-expired",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-expired.pdf",
		ExpiresAt: now.Add(-time.Hour),
		CreatedAt: now.Add(-2 * time.Hour),
	}

	// Config en modo ejecución (DryRun=false): el query param debe ganar.
	r := New(repo, storage, queue, nil, &mockClock{now},
		10*time.Minute, 30*time.Minute, 15*time.Minute, WithInternalToken(token))
	engine := gin.New()
	r.RegisterRoutes(engine)

	req := httptest.NewRequest(http.MethodPost, "/internal/reconcile?dry_run=1", nil)
	req.Header.Set("X-Internal-Token", token)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %+v, want 0 con ?dry_run=1", repo.updateCalls)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"dry_run":true`) {
		t.Errorf("respuesta sin dry_run=true: %s", body)
	}
	if !strings.Contains(body, `"planned_actions"`) {
		t.Errorf("respuesta sin planned_actions: %s", body)
	}
}

// TestRegisterRoutes_ReconcileDryRunOverrideFalse verifica que ?dry_run=0
// desactiva el dry-run configurado para esa invocación (override en ambos sentidos).
func TestRegisterRoutes_ReconcileDryRunOverrideFalse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const token = "internal-test-token"

	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	repo.docs["doc-expired"] = &domain.Document{
		ID:        "doc-expired",
		Status:    domain.StatusPendingUpload,
		ObjectKey: "raw-pdfs/doc-expired.pdf",
		ExpiresAt: now.Add(-time.Hour),
		CreatedAt: now.Add(-2 * time.Hour),
	}

	r := New(repo, storage, nil, nil, &mockClock{now},
		10*time.Minute, 30*time.Minute, 15*time.Minute,
		WithInternalToken(token), WithDryRun(true))
	engine := gin.New()
	r.RegisterRoutes(engine)

	req := httptest.NewRequest(http.MethodPost, "/internal/reconcile?dry_run=0", nil)
	req.Header.Set("X-Internal-Token", token)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	if len(repo.updateCalls) != 1 {
		t.Errorf("updateCalls = %+v, want 1 con ?dry_run=0", repo.updateCalls)
	}
	if repo.docs["doc-expired"].Status != domain.StatusUploadExpired {
		t.Errorf("status = %s, want UPLOAD_EXPIRED (?dry_run=0 ejecuta)", repo.docs["doc-expired"].Status)
	}
}

// TestRegisterRoutes_ReconcileDryRunInvalido verifica que un valor no booleano
// de ?dry_run se rechaza con 400 en vez de caer silenciosamente al modo configurado.
func TestRegisterRoutes_ReconcileDryRunInvalido(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const token = "internal-test-token"

	repo := newMockRepo()
	storage := newMockStorage()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := New(repo, storage, nil, nil, &mockClock{now},
		10*time.Minute, 30*time.Minute, 15*time.Minute, WithInternalToken(token))
	engine := gin.New()
	r.RegisterRoutes(engine)

	req := httptest.NewRequest(http.MethodPost, "/internal/reconcile?dry_run=quizas", nil)
	req.Header.Set("X-Internal-Token", token)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%s)", w.Code, w.Body.String())
	}
}

// --- Reencolado de UPLOADED estancados (S4-P2-01) ---

func TestSweepStuckUploaded_ReencolaSiObjetoExiste(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	// UPLOADED estancado (updated_at hace 2h) y creado hace 2h (pasa safety age).
	doc := &domain.Document{
		ID:            "doc-stuck",
		Status:        domain.StatusUploaded,
		ObjectKey:     "raw-pdfs/doc-stuck.pdf",
		CorrelationID: "corr-1",
		SchemaVersion: 2,
		CreatedAt:     now.Add(-2 * time.Hour),
		UpdatedAt:     now.Add(-2 * time.Hour),
	}
	repo.docs["doc-stuck"] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.Reenqueued != 1 {
		t.Fatalf("Reenqueued = %d, want 1", result.Reenqueued)
	}
	if len(queue.enqueued) != 1 {
		t.Fatalf("enqueued = %d, want 1", len(queue.enqueued))
	}
	if got := queue.enqueued[0].DocumentID; got != "doc-stuck" {
		t.Errorf("DocumentID encolado = %q, want doc-stuck", got)
	}
	if got := queue.enqueued[0].CorrelationID; got != "corr-1" {
		t.Errorf("CorrelationID encolado = %q, want corr-1", got)
	}
	if doc.Status != domain.StatusQueued {
		t.Errorf("estado final = %s, want QUEUED (no se reencola dos veces)", doc.Status)
	}
}

func TestSweepStuckUploaded_MarcaFailedSiObjetoAusente(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage() // objeto NO existe
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:        "doc-lost",
		Status:    domain.StatusUploaded,
		ObjectKey: "raw-pdfs/doc-lost.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs["doc-lost"] = doc

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.Reenqueued != 0 {
		t.Errorf("Reenqueued = %d, want 0 (objeto ausente)", result.Reenqueued)
	}
	if len(queue.enqueued) != 0 {
		t.Errorf("enqueued = %d, want 0 (objeto ausente)", len(queue.enqueued))
	}
	if doc.Status != domain.StatusFailed {
		t.Errorf("estado final = %s, want FAILED (object_missing)", doc.Status)
	}
}

func TestSweepStuckUploaded_NoReencolaReciente(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	// Updated hace 5 min (menor a stuckAfter de 30 min): no está estancado.
	doc := &domain.Document{
		ID:        "doc-fresh",
		Status:    domain.StatusUploaded,
		ObjectKey: "raw-pdfs/doc-fresh.pdf",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-5 * time.Minute),
	}
	repo.docs["doc-fresh"] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.Reenqueued != 0 || len(queue.enqueued) != 0 {
		t.Errorf("reencoló un documento no estancado: Reenqueued=%d, enqueued=%d",
			result.Reenqueued, len(queue.enqueued))
	}
}

func TestSweepStuckUploaded_RespetaSafetyAge(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	// Estancado (updated hace 2h) pero creado hace 5 min (< safety age 15 min).
	doc := &domain.Document{
		ID:        "doc-young",
		Status:    domain.StatusUploaded,
		ObjectKey: "raw-pdfs/doc-young.pdf",
		CreatedAt: now.Add(-5 * time.Minute),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs["doc-young"] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.Reenqueued != 0 || len(queue.enqueued) != 0 {
		t.Errorf("no respetó safety age: Reenqueued=%d, enqueued=%d",
			result.Reenqueued, len(queue.enqueued))
	}
}

// --- Purga de huérfanos (S4-P2-02) ---

func TestSweepOrphans_PurgaSoloHuerfanosAntiguos(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento válido que referencia su objeto.
	doc := &domain.Document{
		ID:        "doc-1",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-1.pdf",
		CreatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-1"] = doc

	storage.listObjects = []ports.ObjectInfo{
		// Válido: tiene documento → NO se borra aunque sea antiguo.
		{Key: "raw-pdfs/doc-1.pdf", LastModified: now.Add(-30 * 24 * time.Hour)},
		// Huérfano antiguo (> 7 días) → SÍ se borra.
		{Key: "raw-pdfs/huerfano-viejo.pdf", LastModified: now.Add(-30 * 24 * time.Hour)},
		// Huérfano reciente (< 7 días) → NO se borra todavía.
		{Key: "raw-pdfs/huerfano-nuevo.pdf", LastModified: now.Add(-time.Hour)},
	}

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.OrphansPurged != 1 {
		t.Fatalf("OrphansPurged = %d, want 1", result.OrphansPurged)
	}
	if len(storage.deleted) != 1 || storage.deleted[0] != "raw-pdfs/huerfano-viejo.pdf" {
		t.Fatalf("deleted = %v, want [raw-pdfs/huerfano-viejo.pdf]", storage.deleted)
	}
	// Nunca borra objetos con documento asociado.
	for _, k := range storage.deleted {
		if k == "raw-pdfs/doc-1.pdf" {
			t.Error("borró un objeto con documento asociado")
		}
	}
}

func TestSweepOrphans_SinObjetos_NoPurga(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage() // sin listObjects

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if result.OrphansPurged != 0 {
		t.Errorf("OrphansPurged = %d, want 0", result.OrphansPurged)
	}
}

// --- Huérfanos en extracted-md (T2, SPEC §6.5) ---

// TestSweepOrphans_PurgaMdHuerfano cubre el segundo pass sobre el bucket
// extracted-md: un objeto sin documento que lo referencie (MdRef) y más
// antiguo que el umbral se purga, mientras el md referenciado y el huérfano
// reciente quedan intactos. Espeja la semántica del pass de raw.
func TestSweepOrphans_PurgaMdHuerfano(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento que referencia su .md: no debe purgarse.
	referenced := "extracted-md/doc-1.md"
	repo.docs["doc-1"] = &domain.Document{
		ID:        "doc-1",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-1.pdf",
		MdRef:     referenced,
		CreatedAt: now.Add(-time.Hour),
	}

	storage.listObjectsMD = []ports.ObjectInfo{
		// Referenciado y antiguo → NO se borra.
		{Key: referenced, LastModified: now.Add(-30 * 24 * time.Hour)},
		// Huérfano antiguo (> 7 días) → SÍ se borra.
		{Key: "extracted-md/huerfano-viejo.md", LastModified: now.Add(-30 * 24 * time.Hour)},
		// Huérfano reciente (< 7 días) → NO se borra todavía.
		{Key: "extracted-md/huerfano-nuevo.md", LastModified: now.Add(-time.Hour)},
	}

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.OrphansPurged != 1 {
		t.Fatalf("OrphansPurged = %d, want 1", result.OrphansPurged)
	}
	if len(storage.deletedMD) != 1 || storage.deletedMD[0] != "extracted-md/huerfano-viejo.md" {
		t.Fatalf("deletedMD = %v, want [extracted-md/huerfano-viejo.md]", storage.deletedMD)
	}
	for _, k := range storage.deletedMD {
		if k == referenced {
			t.Errorf("borró un md referenciado por un documento: %s", k)
		}
	}
	if len(storage.deleted) != 0 {
		t.Errorf("deleted (raw) = %v, want vacío: el pass md no debe tocar raw", storage.deleted)
	}
}

// TestSweepOrphans_NoPurgaMdReferenciado fija la garantía de seguridad: un
// objeto de extracted-md referenciado por cualquier documento (aunque sea
// antiguo) jamás se borra.
func TestSweepOrphans_NoPurgaMdReferenciado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	referenced := "extracted-md/doc-ref.md"
	repo.docs["doc-ref"] = &domain.Document{
		ID:        "doc-ref",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-ref.pdf",
		MdRef:     referenced,
		CreatedAt: now.Add(-time.Hour),
	}
	storage.listObjectsMD = []ports.ObjectInfo{
		{Key: referenced, LastModified: now.Add(-30 * 24 * time.Hour)},
	}

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.OrphansPurged != 0 {
		t.Errorf("OrphansPurged = %d, want 0 (md referenciado)", result.OrphansPurged)
	}
	if len(storage.deletedMD) != 0 {
		t.Errorf("deletedMD = %v, want vacío (md referenciado)", storage.deletedMD)
	}
}

// TestSweepOrphans_DryRun_PlanificaMdSinBorrar prueba que la purga de md pasa
// por el choke point de dry-run (deleteMDObject): se planifica y cuenta, pero
// no se ejecuta ningún borrado (SPEC §6.5, T1).
func TestSweepOrphans_DryRun_PlanificaMdSinBorrar(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	storage.listObjectsMD = []ports.ObjectInfo{
		{Key: "extracted-md/huerfano-dry.md", LastModified: now.Add(-30 * 24 * time.Hour)},
	}

	r := New(repo, storage, nil, nil, &mockClock{now},
		10*time.Minute, 30*time.Minute, 15*time.Minute, WithDryRun(true))
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.OrphansPurged != 1 {
		t.Errorf("OrphansPurged = %d, want 1 (planificado)", result.OrphansPurged)
	}
	if len(storage.deletedMD) != 0 {
		t.Errorf("deletedMD = %v, want 0 en dry-run", storage.deletedMD)
	}
	if result.PlannedActions == 0 {
		t.Error("PlannedActions = 0, want > 0 (el plan debe reportarse)")
	}
	if result.ExecutedActions != 0 {
		t.Errorf("ExecutedActions = %d, want 0 en dry-run", result.ExecutedActions)
	}
}

// --- Bloqueo distribuido multi-réplica (S4-P2-03) ---

func TestRunOnce_SinLease_NoBloquea(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if result.LockAcquired {
		t.Error("LockAcquired = true sin lease configurado")
	}
}

func TestRunOnce_LeaseNoAdquirido_OtraReplicaActiva(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}
	lease := &mockLease{acquired: false} // otra réplica tiene el lease

	// Documento que, de correr, se reencolaría: no debe tocarse.
	doc := &domain.Document{
		ID:        "doc-1",
		Status:    domain.StatusUploaded,
		ObjectKey: "raw-pdfs/doc-1.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs["doc-1"] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, lease, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.LockAcquired {
		t.Error("LockAcquired = true con lease no adquirido")
	}
	if len(queue.enqueued) != 0 || len(repo.updateCalls) != 0 {
		t.Error("otra réplica activa: no debía reencolar ni transicionar")
	}
	if lease.releases != 0 {
		t.Error("no debe liberar un lease que no adquirió")
	}
}

func TestRunOnce_LeaseAdquirido_EjecutaYLibera(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	lease := &mockLease{acquired: true}

	r := New(repo, storage, nil, lease, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if !result.LockAcquired {
		t.Error("LockAcquired = false con lease adquirido")
	}
	if lease.acquires != 1 {
		t.Errorf("acquires = %d, want 1", lease.acquires)
	}
	if lease.releases != 1 {
		t.Errorf("releases = %d, want 1 (debe liberar al terminar)", lease.releases)
	}
}

// --- Historial en las transiciones del reconciliador (S4-P2-04) ---

func TestReconciler_Transiciones_RegistranHistorial(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage() // sin objetos: PENDING_UPLOAD vencido → UPLOAD_EXPIRED

	doc := &domain.Document{
		ID:            "doc-hist",
		Status:        domain.StatusPendingUpload,
		ObjectKey:     "raw-pdfs/doc-hist.pdf",
		CorrelationID: "corr-hist",
		ExpiresAt:     now.Add(-time.Hour),
		CreatedAt:     now.Add(-2 * time.Hour),
	}
	repo.docs["doc-hist"] = doc

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if len(doc.History) != 1 {
		t.Fatalf("historial = %d entradas, want 1", len(doc.History))
	}
	entry := doc.History[0]
	if entry.Actor != domain.ActorReconciler {
		t.Errorf("actor = %s, want reconciler", entry.Actor)
	}
	if entry.Status != domain.StatusUploadExpired {
		t.Errorf("status = %s, want UPLOAD_EXPIRED", entry.Status)
	}
	if entry.CorrelationID != "corr-hist" {
		t.Errorf("correlation_id = %q, want corr-hist", entry.CorrelationID)
	}
	if entry.Reason != domain.FailureReasonUploadExpired {
		t.Errorf("reason = %s, want UPLOAD_WINDOW_EXPIRED", entry.Reason)
	}
}

func TestSweepStuckUploaded_ReencolaRegistraHistorial(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:            "doc-stuck",
		Status:        domain.StatusUploaded,
		ObjectKey:     "raw-pdfs/doc-stuck.pdf",
		CorrelationID: "corr-stuck",
		CreatedAt:     now.Add(-2 * time.Hour),
		UpdatedAt:     now.Add(-2 * time.Hour),
	}
	repo.docs["doc-stuck"] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	// UPLOADED→QUEUED debe dejar una entrada de historial del reconciler.
	if len(doc.History) != 1 {
		t.Fatalf("historial = %d entradas, want 1", len(doc.History))
	}
	if doc.History[0].Actor != domain.ActorReconciler || doc.History[0].Status != domain.StatusQueued {
		t.Errorf("entrada = %+v, want reconciler/QUEUED", doc.History[0])
	}
}

// TestCrossCheck_IgnoraEntradasDePrefijo verifica que el listado de MinIO no
// confunda un prefijo de carpeta ("raw-pdfs/") con un objeto huérfano ni lo
// cuente como objeto del bucket (S6-P2-01, cross-check del reconciliador).
func TestCrossCheck_IgnoraEntradasDePrefijo(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	doc := &domain.Document{
		ID:        "doc-a",
		Status:    domain.StatusCompleted,
		CreatedAt: now, // joven: salta la rama de Stat de completed_no_md
		UpdatedAt: now,
		ObjectKey: "raw-pdfs/doc-a.pdf",
		MdRef:     "extracted-md/doc-a.md",
	}
	repo.docs["doc-a"] = doc

	storage := newMockStorage()
	storage.objects["raw-pdfs/doc-a.pdf"] = true
	storage.listObjects = []ports.ObjectInfo{
		{Key: "raw-pdfs/doc-a.pdf"},
		{Key: "raw-pdfs/"}, // entrada de prefijo: no es un objeto real
	}
	queue := &mockQueue{}

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	report, err := r.crossCheck(context.Background(), now)
	if err != nil {
		t.Fatalf("crossCheck error: %v", err)
	}

	if report.RawBucketObjects != 1 {
		t.Errorf("RawBucketObjects = %d, want 1 (prefijo no cuenta como objeto)", report.RawBucketObjects)
	}
	if len(report.OrphanRawObjects) != 0 {
		t.Errorf("OrphanRawObjects = %v, want vacíos (no hay huérfanos reales)", report.OrphanRawObjects)
	}
	if len(storage.deleted) != 0 {
		t.Errorf("storage.deleted = %v, want vacío (no se purga nada)", storage.deleted)
	}
}

func TestSweepStuckUploaded_TransicionaAntesDeEncolar(t *testing.T) {
	repo := newMockRepo()
	now := time.Now().UTC()
	doc := &domain.Document{ID: "d1", Status: domain.StatusUploaded, ObjectKey: "k", CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)}
	repo.docs["d1"] = doc
	var order []string
	repo.order = &order
	q := &mockQueue{order: &order}
	st := newMockStorage()
	st.objects["k"] = true
	r := New(repo, st, q, nil, &mockClock{t: now}, time.Minute, 30*time.Minute, time.Minute)
	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Reenqueued != 1 {
		t.Fatalf("Reenqueued = %d", res.Reenqueued)
	}
	if repo.docs["d1"].Status != domain.StatusQueued {
		t.Fatalf("status = %v", repo.docs["d1"].Status)
	}
	if len(order) != 2 || order[0] != "transition" || order[1] != "enqueue" {
		t.Fatalf("orden = %v, want [transition enqueue]", order)
	}
}

func TestSweepStuckQueued_ReencolaSinCambiarEstado(t *testing.T) {
	repo := newMockRepo()
	now := time.Now().UTC()
	doc := &domain.Document{ID: "d1", Status: domain.StatusQueued, ObjectKey: "k", CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)}
	repo.docs["d1"] = doc
	q := &mockQueue{}
	st := newMockStorage()
	r := New(repo, st, q, nil, &mockClock{t: now}, time.Minute, 30*time.Minute, time.Minute)
	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Reenqueued != 1 {
		t.Fatalf("Reenqueued = %d", res.Reenqueued)
	}
	if len(q.enqueued) != 1 {
		t.Fatalf("enqueued = %d", len(q.enqueued))
	}
	if repo.docs["d1"].Status != domain.StatusQueued {
		t.Fatalf("status changed")
	}
}

func TestSweepStuckQueued_RespetaSafetyAge(t *testing.T) {
	repo := newMockRepo()
	now := time.Now().UTC()
	doc := &domain.Document{ID: "d1", Status: domain.StatusQueued, ObjectKey: "k", CreatedAt: now.Add(-1 * time.Minute), UpdatedAt: now.Add(-2 * time.Hour)}
	repo.docs["d1"] = doc
	q := &mockQueue{}
	st := newMockStorage()
	r := New(repo, st, q, nil, &mockClock{t: now}, time.Minute, 30*time.Minute, 15*time.Minute)
	_, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(q.enqueued) != 0 {
		t.Fatalf("enqueued = %d", len(q.enqueued))
	}
}

func TestSweepStuckQueued_NoReencolaReciente(t *testing.T) {
	repo := newMockRepo()
	now := time.Now().UTC()
	doc := &domain.Document{ID: "d1", Status: domain.StatusQueued, ObjectKey: "k", CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-5 * time.Minute)}
	repo.docs["d1"] = doc
	q := &mockQueue{}
	st := newMockStorage()
	r := New(repo, st, q, nil, &mockClock{t: now}, time.Minute, 30*time.Minute, time.Minute)
	_, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(q.enqueued) != 0 {
		t.Fatalf("enqueued = %d", len(q.enqueued))
	}
}

func TestSweepStuckQueued_IgnoraSinObjectKey(t *testing.T) {
	repo := newMockRepo()
	now := time.Now().UTC()
	doc := &domain.Document{ID: "d1", Status: domain.StatusQueued, ObjectKey: "", CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)}
	repo.docs["d1"] = doc
	q := &mockQueue{}
	st := newMockStorage()
	r := New(repo, st, q, nil, &mockClock{t: now}, time.Minute, 30*time.Minute, time.Minute)
	_, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(q.enqueued) != 0 {
		t.Fatalf("enqueued = %d", len(q.enqueued))
	}
}

// --- Estados intermedios trabados: recuperación real (T3, SPEC §6.5) ---

// TestRunOnce_StuckProcessing_ObjetoPresente_ReencolaComoRetrying cubre la
// arista "fallo transitorio": PROCESSING estancado con el objeto raw presente
// pasa a RETRYING y se reencola con el mensaje de SPEC §11.3 (T3).
func TestRunOnce_StuckProcessing_ObjetoPresente_ReencolaComoRetrying(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:            "doc-proc",
		Status:        domain.StatusProcessing,
		ObjectKey:     "raw-pdfs/doc-proc.pdf",
		CorrelationID: "corr-proc",
		SchemaVersion: 2,
		CreatedAt:     now.Add(-2 * time.Hour),
		UpdatedAt:     now.Add(-2 * time.Hour),
	}
	repo.docs[doc.ID] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if doc.Status != domain.StatusRetrying {
		t.Fatalf("estado final = %s, want RETRYING", doc.Status)
	}
	if result.StuckDetected != 1 {
		t.Errorf("StuckDetected = %d, want 1", result.StuckDetected)
	}
	if result.Reenqueued != 1 {
		t.Errorf("Reenqueued = %d, want 1", result.Reenqueued)
	}
	if len(queue.enqueued) != 1 {
		t.Fatalf("enqueued = %d, want 1", len(queue.enqueued))
	}
	msg := queue.enqueued[0]
	if msg.DocumentID != doc.ID || msg.ObjectKey != doc.ObjectKey {
		t.Errorf("mensaje = %+v, want document_id=%s object_key=%s", msg, doc.ID, doc.ObjectKey)
	}
	if msg.CorrelationID != "corr-proc" || msg.SchemaVersion != 2 || msg.EnqueuedAt == "" {
		t.Errorf("mensaje = %+v, want correlation_id/schema_version/enqueued_at completos", msg)
	}

	// Transición auditada: PROCESSING→RETRYING por el reconciliador.
	if len(doc.History) != 1 {
		t.Fatalf("historial = %d entradas, want 1", len(doc.History))
	}
	entry := doc.History[0]
	if entry.Status != domain.StatusRetrying || entry.Actor != domain.ActorReconciler {
		t.Errorf("entrada = %+v, want reconciler/RETRYING", entry)
	}
	if entry.CorrelationID != "corr-proc" {
		t.Errorf("correlation_id = %q, want corr-proc", entry.CorrelationID)
	}
	if len(repo.updateCalls) != 1 || repo.updateCalls[0].from != domain.StatusProcessing ||
		repo.updateCalls[0].to != domain.StatusRetrying {
		t.Errorf("updateCalls = %+v, want [PROCESSING→RETRYING]", repo.updateCalls)
	}
}

// TestRunOnce_StuckRetrying_ObjetoPresente_SoloReencola fija la semántica del
// retry handler: RETRYING no transiciona (RETRYING→PROCESSING es del worker),
// solo se reencola (T3).
func TestRunOnce_StuckRetrying_ObjetoPresente_SoloReencola(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:            "doc-retry",
		Status:        domain.StatusRetrying,
		ObjectKey:     "raw-pdfs/doc-retry.pdf",
		CorrelationID: "corr-retry",
		SchemaVersion: 1,
		CreatedAt:     now.Add(-2 * time.Hour),
		UpdatedAt:     now.Add(-2 * time.Hour),
	}
	repo.docs[doc.ID] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if doc.Status != domain.StatusRetrying {
		t.Errorf("estado final = %s, want RETRYING (sin transición)", doc.Status)
	}
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %+v, want 0 (RETRYING solo reencola)", repo.updateCalls)
	}
	if len(doc.History) != 0 {
		t.Errorf("historial = %+v, want vacío (sin transición)", doc.History)
	}
	if len(queue.enqueued) != 1 || result.Reenqueued != 1 {
		t.Errorf("enqueued=%d Reenqueued=%d, want (1, 1)", len(queue.enqueued), result.Reenqueued)
	}
}

// TestRunOnce_StuckIntermedio_ObjetoAusente_CompletaViaDeFallo cubre el caso
// sin objeto raw: la SAGA se completa con aristas declaradas
// (→EXTRACTION_FAILED(OBJECT_MISSING) →COMPENSATING →FAILED) y deja las tres
// transiciones auditadas, sin inventar PROCESSING/RETRYING→FAILED (T3).
func TestRunOnce_StuckIntermedio_ObjetoAusente_CompletaViaDeFallo(t *testing.T) {
	froms := []domain.Status{domain.StatusProcessing, domain.StatusRetrying}
	for _, from := range froms {
		t.Run(string(from), func(t *testing.T) {
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			repo := newMockRepo()
			storage := newMockStorage() // sin objetos: el raw no existe
			queue := &mockQueue{}

			doc := &domain.Document{
				ID:            "doc-lost",
				Status:        from,
				ObjectKey:     "raw-pdfs/doc-lost.pdf",
				CorrelationID: "corr-lost",
				CreatedAt:     now.Add(-2 * time.Hour),
				UpdatedAt:     now.Add(-2 * time.Hour),
			}
			repo.docs[doc.ID] = doc

			r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
			result, err := r.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce error: %v", err)
			}

			if doc.Status != domain.StatusFailed {
				t.Fatalf("estado final = %s, want FAILED", doc.Status)
			}
			if doc.FailureReason != domain.FailureReasonObjectMissing {
				t.Errorf("failure_reason = %q, want %q", doc.FailureReason, domain.FailureReasonObjectMissing)
			}
			if len(queue.enqueued) != 0 || result.Reenqueued != 0 {
				t.Errorf("enqueued=%d Reenqueued=%d, want (0, 0) sin objeto", len(queue.enqueued), result.Reenqueued)
			}

			// Historial completo de la vía de fallo, en orden y del reconciliador.
			got := make([]domain.Status, 0, len(doc.History))
			for _, e := range doc.History {
				if e.Actor != domain.ActorReconciler {
					t.Errorf("actor = %s, want reconciler", e.Actor)
				}
				if e.CorrelationID != "corr-lost" {
					t.Errorf("correlation_id = %q, want corr-lost", e.CorrelationID)
				}
				got = append(got, e.Status)
			}
			want := []domain.Status{
				domain.StatusExtractionFailed,
				domain.StatusCompensating,
				domain.StatusFailed,
			}
			if len(got) != len(want) {
				t.Fatalf("historial = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("historial = %v, want %v", got, want)
				}
			}

			// Cada salto fue condicional desde el estado esperado.
			wantCalls := []updateCall{
				{id: doc.ID, from: from, to: domain.StatusExtractionFailed},
				{id: doc.ID, from: domain.StatusExtractionFailed, to: domain.StatusCompensating},
				{id: doc.ID, from: domain.StatusCompensating, to: domain.StatusFailed},
			}
			if len(repo.updateCalls) != len(wantCalls) {
				t.Fatalf("updateCalls = %+v, want %+v", repo.updateCalls, wantCalls)
			}
			for i := range wantCalls {
				if repo.updateCalls[i] != wantCalls[i] {
					t.Fatalf("updateCalls[%d] = %+v, want %+v", i, repo.updateCalls[i], wantCalls[i])
				}
			}
		})
	}
}

// TestRunOnce_StuckCompensating_TerminaFailedCompensationFailed cubre
// COMPENSATING trabado: COMPENSATING→FAILED con COMPENSATION_FAILED (T3).
func TestRunOnce_StuckCompensating_TerminaFailedCompensationFailed(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:            "doc-comp",
		Status:        domain.StatusCompensating,
		ObjectKey:     "raw-pdfs/doc-comp.pdf",
		CorrelationID: "corr-comp",
		CreatedAt:     now.Add(-2 * time.Hour),
		UpdatedAt:     now.Add(-2 * time.Hour),
	}
	repo.docs[doc.ID] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if doc.Status != domain.StatusFailed {
		t.Fatalf("estado final = %s, want FAILED", doc.Status)
	}
	if doc.FailureReason != domain.FailureReasonCompensateFail {
		t.Errorf("failure_reason = %q, want %q", doc.FailureReason, domain.FailureReasonCompensateFail)
	}
	if result.StuckDetected != 1 {
		t.Errorf("StuckDetected = %d, want 1", result.StuckDetected)
	}
	if len(queue.enqueued) != 0 {
		t.Errorf("enqueued = %d, want 0 (COMPENSATING no se reencola)", len(queue.enqueued))
	}
	if len(repo.updateCalls) != 1 || repo.updateCalls[0].from != domain.StatusCompensating ||
		repo.updateCalls[0].to != domain.StatusFailed {
		t.Errorf("updateCalls = %+v, want [COMPENSATING→FAILED]", repo.updateCalls)
	}
	if len(doc.History) != 1 || doc.History[0].Status != domain.StatusFailed ||
		doc.History[0].Actor != domain.ActorReconciler {
		t.Errorf("historial = %+v, want [reconciler/FAILED]", doc.History)
	}
}

// TestRunOnce_DryRun_EstadosTrabados_PlanificaSinMutar cubre el choke point de
// dry-run para las cuatro ramas de T3: planifica transiciones y encolados, pero
// no ejecuta ninguna mutación (SPEC §6.5).
func TestRunOnce_DryRun_EstadosTrabados_PlanificaSinMutar(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	stale := now.Add(-2 * time.Hour)
	// PROCESSING con objeto → RETRYING + enqueue (2 acciones planificadas).
	repo.docs["doc-proc"] = &domain.Document{
		ID: "doc-proc", Status: domain.StatusProcessing,
		ObjectKey: "raw-pdfs/doc-proc.pdf", CreatedAt: stale, UpdatedAt: stale,
	}
	storage.objects["raw-pdfs/doc-proc.pdf"] = true
	// RETRYING con objeto → solo enqueue (1).
	repo.docs["doc-retry"] = &domain.Document{
		ID: "doc-retry", Status: domain.StatusRetrying,
		ObjectKey: "raw-pdfs/doc-retry.pdf", CreatedAt: stale, UpdatedAt: stale,
	}
	storage.objects["raw-pdfs/doc-retry.pdf"] = true
	// PROCESSING sin objeto → 3 transiciones de la vía de fallo.
	repo.docs["doc-lost"] = &domain.Document{
		ID: "doc-lost", Status: domain.StatusProcessing,
		ObjectKey: "raw-pdfs/doc-lost.pdf", CreatedAt: stale, UpdatedAt: stale,
	}
	// COMPENSATING → FAILED (1).
	repo.docs["doc-comp"] = &domain.Document{
		ID: "doc-comp", Status: domain.StatusCompensating,
		ObjectKey: "raw-pdfs/doc-comp.pdf", CreatedAt: stale, UpdatedAt: stale,
	}

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute,
		WithDryRun(true))
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.StuckDetected != 4 {
		t.Errorf("StuckDetected = %d, want 4", result.StuckDetected)
	}
	// Plan: 2 (PROCESSING presente) + 1 (RETRYING) + 3 (vía de fallo) + 1 (COMPENSATING).
	if result.PlannedActions != 7 {
		t.Errorf("PlannedActions = %d, want 7", result.PlannedActions)
	}
	if result.ExecutedActions != 0 {
		t.Errorf("ExecutedActions = %d, want 0 en dry-run", result.ExecutedActions)
	}

	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %+v, want 0 en dry-run", repo.updateCalls)
	}
	if len(queue.enqueued) != 0 {
		t.Errorf("enqueued = %d, want 0 en dry-run", len(queue.enqueued))
	}
	wantStatus := map[string]domain.Status{
		"doc-proc":  domain.StatusProcessing,
		"doc-retry": domain.StatusRetrying,
		"doc-lost":  domain.StatusProcessing,
		"doc-comp":  domain.StatusCompensating,
	}
	for id, status := range wantStatus {
		if got := repo.docs[id].Status; got != status {
			t.Errorf("%s status = %s, want %s (intacto)", id, got, status)
		}
	}
}

// TestRunOnce_StuckIntermediate_Reciente_NoSeToca fija que el umbral protege a
// los documentos frescos: ni transiciones ni encolados (T3).
func TestRunOnce_StuckIntermediate_Reciente_NoSeToca(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:        "doc-fresh",
		Status:    domain.StatusProcessing,
		ObjectKey: "raw-pdfs/doc-fresh.pdf",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-5 * time.Minute), // < stuckAfter (30m)
	}
	repo.docs[doc.ID] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if result.StuckDetected != 0 {
		t.Errorf("StuckDetected = %d, want 0", result.StuckDetected)
	}
	if doc.Status != domain.StatusProcessing {
		t.Errorf("estado = %s, want PROCESSING (intacto)", doc.Status)
	}
	if len(repo.updateCalls) != 0 || len(queue.enqueued) != 0 {
		t.Errorf("mutaciones = (updates %d, enqueues %d), want (0, 0)",
			len(repo.updateCalls), len(queue.enqueued))
	}
}

// TestRunOnce_StuckProcessing_DosCorridas_NoDuplican fija la idempotencia de la
// rama respaldada por transición: la primera corrida mueve a RETRYING (lo que
// bumpea updated_at) y la segunda no vuelve a transicionar ni a encolar (T3).
func TestRunOnce_StuckProcessing_DosCorridas_NoDuplican(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}

	doc := &domain.Document{
		ID:        "doc-proc",
		Status:    domain.StatusProcessing,
		ObjectKey: "raw-pdfs/doc-proc.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs[doc.ID] = doc
	storage.objects[doc.ObjectKey] = true

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("primera corrida: %v", err)
	}
	if doc.Status != domain.StatusRetrying || len(queue.enqueued) != 1 {
		t.Fatalf("primera corrida: estado=%s enqueued=%d, want RETRYING/1", doc.Status, len(queue.enqueued))
	}

	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("segunda corrida: %v", err)
	}
	if doc.Status != domain.StatusRetrying {
		t.Errorf("segunda corrida: estado = %s, want RETRYING", doc.Status)
	}
	if len(repo.updateCalls) != 1 {
		t.Errorf("segunda corrida: updateCalls = %+v, want 1 (sin transición nueva)", repo.updateCalls)
	}
	if len(queue.enqueued) != 1 {
		t.Errorf("segunda corrida: enqueued = %d, want 1 (sin duplicado)", len(queue.enqueued))
	}
}

// TestRunOnce_StuckProcessing_ErrorDeInfraestructura_NoMarcaFailed fija la
// garantía de seguridad espejada del sweep de PENDING_UPLOAD: un 404 de
// infraestructura (NoSuchBucket) en el HEAD no es "objeto ausente", así que el
// documento intermedio queda intacto para el próximo sweep (T3).
func TestRunOnce_StuckProcessing_ErrorDeInfraestructura_NoMarcaFailed(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()
	queue := &mockQueue{}
	storage.statErr = minio.ErrorResponse{
		Code:       "NoSuchBucket",
		Message:    "The specified bucket does not exist.",
		StatusCode: http.StatusNotFound,
	}

	doc := &domain.Document{
		ID:        "doc-infra",
		Status:    domain.StatusProcessing,
		ObjectKey: "raw-pdfs/doc-infra.pdf",
		CreatedAt: now.Add(-2 * time.Hour),
		UpdatedAt: now.Add(-2 * time.Hour),
	}
	repo.docs[doc.ID] = doc

	r := New(repo, storage, queue, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	if doc.Status != domain.StatusProcessing {
		t.Errorf("estado = %s, want PROCESSING (intacto ante error de infraestructura)", doc.Status)
	}
	if doc.FailureReason != "" {
		t.Errorf("failure_reason = %q, want vacío", doc.FailureReason)
	}
	if len(repo.updateCalls) != 0 || len(queue.enqueued) != 0 {
		t.Errorf("mutaciones = (updates %d, enqueues %d), want (0, 0)",
			len(repo.updateCalls), len(queue.enqueued))
	}
}

// --- Fallback legacy txt_ref en artefacto COMPLETED (D5, cambiomd Fase 3) ---

// TestRunOnce_CompletedConTxtRefLegacy_NoAlerta: un COMPLETED solo con txt_ref
// cuyo .txt existe en el bucket legacy NO debe contarse como faltante.
func TestRunOnce_CompletedConTxtRefLegacy_NoAlerta(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	repo.docs["doc-legacy"] = &domain.Document{
		ID:        "doc-legacy",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-legacy.pdf",
		TxtRef:    "extracted-txt/doc-legacy.txt",
		CreatedAt: now.Add(-time.Hour),
	}
	storage.txtObjects = map[string]bool{"extracted-txt/doc-legacy.txt": true}

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if result.CompletedNoMd != 0 {
		t.Errorf("CompletedNoMd = %d, want 0 (txt_ref legacy cubre el artefacto)", result.CompletedNoMd)
	}
}

// TestRunOnce_CompletedTxtRefLegacyFaltante_Detectado: el .txt legacy ausente sí alerta.
func TestRunOnce_CompletedTxtRefLegacyFaltante_Detectado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	repo.docs["doc-legacy-missing"] = &domain.Document{
		ID:        "doc-legacy-missing",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-legacy-missing.pdf",
		TxtRef:    "extracted-txt/doc-legacy-missing.txt",
		CreatedAt: now.Add(-time.Hour),
	}
	storage.txtObjects = map[string]bool{} // el .txt no existe

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if result.CompletedNoMd != 1 {
		t.Errorf("CompletedNoMd = %d, want 1 (txt_ref legacy sin objeto)", result.CompletedNoMd)
	}
}

// TestRunOnce_CompletedMdRefFaltanteSigueDetectando: con md_ref presente, su
// ausencia en MinIO sigue alertando (md tiene prioridad; no cae al legacy).
func TestRunOnce_CompletedMdRefFaltanteSigueDetectando(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	repo.docs["doc-md-missing"] = &domain.Document{
		ID:        "doc-md-missing",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-md-missing.pdf",
		MdRef:     "extracted-md/doc-md-missing.md",
		TxtRef:    "extracted-txt/doc-md-missing.txt", // legacy existe…
		CreatedAt: now.Add(-time.Hour),
	}
	storage.mdObjects = map[string]bool{} // …pero md_ref manda
	storage.txtObjects = map[string]bool{"extracted-txt/doc-md-missing.txt": true}

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if result.CompletedNoMd != 1 {
		t.Errorf("CompletedNoMd = %d, want 1 (md_ref presente pero objeto ausente)", result.CompletedNoMd)
	}
}

func TestRunOnce_ConfigSource_DryRunVivo(t *testing.T) {
	repo := newMockRepo()
	storage := newMockStorage()
	now := time.Now().UTC()
	repo.docs["d1"] = &domain.Document{ID: "d1", Status: domain.StatusPendingUpload, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-1 * time.Hour), ObjectKey: "k"}
	src := func() LiveConfig {
		return LiveConfig{Interval: 10 * time.Minute, StuckAfter: 30 * time.Minute, DryRun: true}
	}
	r := New(repo, storage, nil, nil, &mockClock{t: now}, 10*time.Minute, 30*time.Minute, 10*time.Minute, WithDryRun(false), WithConfigSource(src))
	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(repo.updateCalls) != 0 {
		t.Fatalf("expected no update calls, got %d", len(repo.updateCalls))
	}
	if !res.DryRun || res.ExecutedActions != 0 || res.PlannedActions <= 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	// flip
	src2 := func() LiveConfig {
		return LiveConfig{Interval: 10 * time.Minute, StuckAfter: 30 * time.Minute, DryRun: false}
	}
	r2 := New(repo, storage, nil, nil, &mockClock{t: now}, 10*time.Minute, 30*time.Minute, 10*time.Minute, WithDryRun(false), WithConfigSource(src2))
	_, err = r2.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(repo.updateCalls) == 0 {
		t.Fatal("expected mutations after flip")
	}
}

func TestRunOnce_ConfigSource_StuckAfterVivo(t *testing.T) {
	repo := newMockRepo()
	storage := newMockStorage()
	q := &mockQueue{}
	now := time.Now().UTC()
	doc := &domain.Document{ID: "d2", Status: domain.StatusQueued, CreatedAt: now.Add(-20 * time.Minute), UpdatedAt: now.Add(-10 * time.Minute), ObjectKey: "k"}
	repo.docs[doc.ID] = doc
	src := func() LiveConfig {
		return LiveConfig{Interval: 10 * time.Minute, StuckAfter: 1 * time.Minute, DryRun: false}
	}
	r := New(repo, storage, q, nil, &mockClock{t: now}, 10*time.Minute, 2*time.Hour, 10*time.Minute, WithConfigSource(src))
	res, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Reenqueued < 1 || len(q.enqueued) < 1 {
		t.Fatalf("expected reenqueue, got %+v", res)
	}
}

func TestStart_RearmaTickerAlCambiarIntervalo(t *testing.T) {
	repo := newMockRepo()
	lease := &mockLease{}
	ch := make(chan struct{}, 1)
	var iv time.Duration = 200 * time.Millisecond
	src := func() LiveConfig { return LiveConfig{Interval: iv, StuckAfter: 30 * time.Minute, DryRun: false} }
	r := New(repo, nil, nil, lease, &mockClock{t: time.Now()}, 200*time.Millisecond, 30*time.Minute, 10*time.Minute, WithConfigSource(src), WithConfigChanges(ch))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(60 * time.Millisecond)
		iv = 20 * time.Millisecond
		ch <- struct{}{}
	}()
	r.Start(ctx)
	if lease.acquires < 5 {
		t.Fatalf("expected >=5 acquires, got %d", lease.acquires)
	}
}

func TestRunOnce_DryRunMetric_UnaVezPorSweep(t *testing.T) {
	repo := newMockRepo()
	storage := newMockStorage()
	now := time.Now().UTC()
	repo.docs["d3"] = &domain.Document{ID: "d3", Status: domain.StatusPendingUpload, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-1 * time.Hour), ObjectKey: "k"}
	src := func() LiveConfig {
		return LiveConfig{Interval: 10 * time.Minute, StuckAfter: 30 * time.Minute, DryRun: true}
	}
	r := New(repo, storage, nil, nil, &mockClock{t: now}, 10*time.Minute, 30*time.Minute, 10*time.Minute, WithConfigSource(src))
	before := testutil.ToFloat64(metrics.ReconcileDryRun)
	_, _ = r.RunOnce(context.Background())
	after := testutil.ToFloat64(metrics.ReconcileDryRun)
	if after != before+1 {
		t.Fatalf("metric did not increment: %v -> %v", before, after)
	}
	// non-dry-run
	src2 := func() LiveConfig {
		return LiveConfig{Interval: 10 * time.Minute, StuckAfter: 30 * time.Minute, DryRun: false}
	}
	r2 := New(repo, storage, nil, nil, &mockClock{t: now}, 10*time.Minute, 30*time.Minute, 10*time.Minute, WithConfigSource(src2))
	before2 := testutil.ToFloat64(metrics.ReconcileDryRun)
	_, _ = r2.RunOnce(context.Background())
	after2 := testutil.ToFloat64(metrics.ReconcileDryRun)
	if after2 != before2 {
		t.Fatalf("metric moved on non-dry-run: %v -> %v", before2, after2)
	}
}

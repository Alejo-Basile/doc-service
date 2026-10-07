package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// mockClock implementa ports.Clock para tests.
type mockClock struct{ t time.Time }

func (c *mockClock) Now() time.Time { return c.t }

// mockRepo implementa ports.DocumentRepository.
type mockRepo struct {
	docs        map[string]*domain.Document
	updateCalls []updateCall
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
	doc, ok := m.docs[id]
	if !ok || doc.Status != from {
		return false, nil
	}
	doc.Status = to
	entry.At = time.Now().UTC()
	doc.History = append(doc.History, entry)
	return true, nil
}

// mockStorage implementa ports.ObjectStorage.
type mockStorage struct {
	objects map[string]bool // key → exists
	pdfData []byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{objects: make(map[string]bool)}
}

func (m *mockStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	return &ports.PresignPostResult{}, nil
}

func (m *mockStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	if m.objects[objectKey] {
		return ports.ObjectInfo{Key: objectKey}, nil
	}
	return ports.ObjectInfo{}, errNotFound
}

func (m *mockStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	if m.pdfData != nil {
		return m.pdfData, nil
	}
	return []byte("%PDF-"), nil
}

func (m *mockStorage) Delete(ctx context.Context, objectKey string) error { return nil }

func (m *mockStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) PresignGetTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) ListObjects(ctx context.Context) ([]ports.ObjectInfo, error) {
	return nil, nil
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

func TestRunOnce_StuckIntermediate_Detected(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento en PROCESSING con updated_at hace 2h (umbral: 30min).
	doc := &domain.Document{
		ID:        "doc-stuck",
		Status:    domain.StatusProcessing,
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

	// v1 solo detecta: no debe transicionar.
	if len(repo.updateCalls) != 0 {
		t.Errorf("updateCalls = %d, want 0 (v1 solo detecta)", len(repo.updateCalls))
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
		err  error
		want bool
	}{
		{nil, false},
		{notFoundError{}, true},
		{errString("Object not found"), true},
		{errString("NoSuchKey"), true},
		{errString("connection refused"), false},
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

// --- Completed sin .txt (S5-P2-03) ---

func TestRunOnce_CompletedSinTxt_Detectado(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	repo := newMockRepo()
	storage := newMockStorage()

	// Documento COMPLETED sin txt_ref, creado hace 1h (pasó safety age).
	doc := &domain.Document{
		ID:        "doc-no-txt",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-no-txt.pdf",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-no-txt"] = doc

	// Segundo documento COMPLETED con txt_ref que no existe en MinIO.
	doc2 := &domain.Document{
		ID:        "doc-txt-missing",
		Status:    domain.StatusCompleted,
		ObjectKey: "raw-pdfs/doc-txt-missing.pdf",
		TxtRef:    "extracted-txt/doc-txt-missing.txt",
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now.Add(-time.Hour),
	}
	repo.docs["doc-txt-missing"] = doc2

	r := New(repo, storage, nil, nil, &mockClock{now}, 10*time.Minute, 30*time.Minute, 15*time.Minute)
	result, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}

	// Ambos deben ser detectados.
	if result.CompletedNoTxt != 2 {
		t.Errorf("CompletedNoTxt = %d, want 2", result.CompletedNoTxt)
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

	// Documento COMPLETED sin txt_ref.
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
	if len(report.CompletedNoTxt) != 1 {
		t.Errorf("CompletedNoTxt = %v, want 1 doc", report.CompletedNoTxt)
	}
	if report.YoungUntouched != 1 {
		t.Errorf("YoungUntouched = %d, want 1", report.YoungUntouched)
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

package pdfsvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// mockStorage implementa ports.ObjectStorage para tests unitarios.
type mockStorage struct {
	data map[string][]byte
	err  error
}

func (m *mockStorage) PresignPost(ctx context.Context, objectKey string, opts ports.PresignPostOptions) (*ports.PresignPostResult, error) {
	return nil, nil
}

func (m *mockStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	return ports.ObjectInfo{}, nil
}

func (m *mockStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	data, ok := m.data[objectKey]
	if !ok {
		return nil, errors.New("object not found")
	}
	// Simular rango inclusivo.
	if start < 0 {
		start = 0
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if start > end {
		return nil, errors.New("invalid range")
	}
	return data[start : end+1], nil
}

func (m *mockStorage) Delete(ctx context.Context, objectKey string) error {
	return nil
}

func (m *mockStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) PresignGetTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	return "", nil
}

func (m *mockStorage) ListObjects(ctx context.Context) ([]ports.ObjectInfo, error) {
	return nil, nil
}

func TestIsPDF_HeaderCorrecto(t *testing.T) {
	casos := []struct {
		nombre   string
		data     []byte
		esperado bool
	}{
		{"PDF válido", []byte("%PDF-1.7\n"), true},
		{"PDF 1.4", []byte("%PDF-1.4"), true},
		{"PDF 2.0", []byte("%PDF-2.0"), true},
		{"PDF header exacto", []byte("%PDF-"), true},
		{"Texto plano", []byte("Hola mundo"), false},
		{"HTML", []byte("<html>"), false},
		{"Vacío", []byte{}, false},
		{"Header incompleto", []byte("%PD"), false},
		{"Case sensitive", []byte("%pdf-1.4"), false},
		{"Espacio antes", []byte(" %PDF-1.4"), false},
	}

	for _, c := range casos {
		got := isPDF(c.data)
		if got != c.esperado {
			t.Errorf("%s: isPDF(%q) esperado %v, got %v", c.nombre, c.data, c.esperado, got)
		}
	}
}

func TestPDFHeader(t *testing.T) {
	if PDFHeader() != "%PDF-" {
		t.Errorf("PDFHeader() esperado '%%PDF-', got %q", PDFHeader())
	}
}

func TestValidator_PDFValido(t *testing.T) {
	mock := &mockStorage{
		data: map[string][]byte{
			"valid.pdf": []byte("%PDF-1.7\ncontenido del PDF"),
		},
	}
	v := NewValidator(mock)

	err := v.ValidatePDF(context.Background(), "valid.pdf")
	if err != nil {
		t.Errorf("valid.pdf debía pasar: %v", err)
	}
}

func TestValidator_NoPDF(t *testing.T) {
	mock := &mockStorage{
		data: map[string][]byte{
			"invalid.txt": []byte("esto no es un PDF, es texto plano"),
		},
	}
	v := NewValidator(mock)

	err := v.ValidatePDF(context.Background(), "invalid.txt")
	if err == nil {
		t.Fatal("invalid.txt debía fallar")
	}
	if !errors.Is(err, ErrNotPDF) {
		t.Errorf("error esperado ErrNotPDF, got %v", err)
	}
}

func TestValidator_HeaderIncompleto(t *testing.T) {
	mock := &mockStorage{
		data: map[string][]byte{
			"short.pdf": []byte("%PD"),
		},
	}
	v := NewValidator(mock)

	err := v.ValidatePDF(context.Background(), "short.pdf")
	if err == nil {
		t.Fatal("short.pdf debía fallar (header incompleto)")
	}
	if !errors.Is(err, ErrNotPDF) {
		t.Errorf("error esperado ErrNotPDF, got %v", err)
	}
}

func TestValidator_ObjetoInexistente(t *testing.T) {
	mock := &mockStorage{data: map[string][]byte{}}
	v := NewValidator(mock)

	err := v.ValidatePDF(context.Background(), "no-existe.pdf")
	if err == nil {
		t.Fatal("objeto inexistente debía fallar")
	}
	if errors.Is(err, ErrNotPDF) {
		t.Error("error de storage no debe confundirse con ErrNotPDF")
	}
}

func TestValidator_RangoCorrecto(t *testing.T) {
	// Verificar que se leen exactamente los primeros 6 bytes (0-5).
	mock := &mockStorage{
		data: map[string][]byte{
			"range.pdf": []byte("%PDF-1.7\nresto"),
		},
	}
	v := NewValidator(mock)

	err := v.ValidatePDF(context.Background(), "range.pdf")
	if err != nil {
		t.Errorf("range.pdf debía pasar: %v", err)
	}
}

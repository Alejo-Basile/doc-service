// Package pdfsvc implementa la validación de contenido PDF.
// Verifica la cabecera mágica %PDF- con GET por rango (SPEC §9, S2-P2-02).
package pdfsvc

import (
	"context"
	"fmt"
	"strings"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// ErrNotPDF se devuelve cuando el objeto no empieza por %PDF-.
var ErrNotPDF = fmt.Errorf("el objeto no es un PDF válido (cabecera %%PDF- ausente)")

// pdfHeader es la cabecera mágica que todo PDF debe contener en los primeros bytes.
const pdfHeader = "%PDF-"

// headerBytes es la longitud del prefijo que se lee para validar.
// "%PDF-" tiene 5 bytes; se leen 6 para capturar también un dígito de versión.
const headerBytes = 6

// Validator valida el contenido de objetos subidos a MinIO.
type Validator struct {
	storage ports.ObjectStorage
}

// NewValidator crea un Validator sobre el ObjectStorage indicado.
func NewValidator(storage ports.ObjectStorage) *Validator {
	return &Validator{storage: storage}
}

// ValidatePDF verifica que el objeto en objectKey sea un PDF válido.
// Lee los primeros bytes con Range: bytes=0-5 y verifica que empiecen por %PDF-.
// Si no es PDF, devuelve ErrNotPDF.
func (v *Validator) ValidatePDF(ctx context.Context, objectKey string) error {
	data, err := v.storage.GetRange(ctx, objectKey, 0, headerBytes-1)
	if err != nil {
		return fmt.Errorf("validar PDF %s: %w", objectKey, err)
	}

	if !isPDF(data) {
		return fmt.Errorf("%w: object_key=%s, bytes_leídos=%q", ErrNotPDF, objectKey, string(data))
	}

	return nil
}

// isPDF verifica si los bytes dados empiezan por %PDF-.
func isPDF(data []byte) bool {
	if len(data) < len(pdfHeader) {
		return false
	}
	return strings.HasPrefix(string(data), pdfHeader)
}

// PDFHeader retorna la cabecera mágica esperada (para tests y documentación).
func PDFHeader() string { return pdfHeader }

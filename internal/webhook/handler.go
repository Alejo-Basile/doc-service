// Package webhook implementa el endpoint interno para recibir eventos
// de MinIO Bucket Notifications (SPEC §5.2, S3-P2-05/06/07/08).
package webhook

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// Evento mínimo de MinIO Bucket Notifications que nos interesa.
type minioEvent struct {
	EventName string `json:"EventName"`
	S3        struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key string `json:"key"`
		} `json:"object"`
	} `json:"s3"`
}

// Handler procesa los eventos del webhook de MinIO.
type Handler struct {
	repo          ports.DocumentRepository
	storage       ports.ObjectStorage
	webhookSecret string
	bucketRaw     string
	keyPrefix     string // prefijo esperado en la clave (ej: raw-pdfs/)
}

// NewHandler crea el handler del webhook.
func NewHandler(
	repo ports.DocumentRepository,
	storage ports.ObjectStorage,
	webhookSecret string,
	bucketRaw string,
	keyPrefix string,
) *Handler {
	return &Handler{
		repo:          repo,
		storage:       storage,
		webhookSecret: webhookSecret,
		bucketRaw:     bucketRaw,
		keyPrefix:     keyPrefix,
	}
}

// RegisterRoutes registra la ruta del webhook en el engine.
func (h *Handler) RegisterRoutes(engine *gin.Engine) {
	engine.POST("/internal/storage/events", h.HandleEvent)
}

// HandleEvent procesa un evento ObjectCreated de MinIO (S3-P2-05/06/07).
//
// Pasos:
//  1. Autenticación con token compartido (S3-P2-06).
//  2. Parseo del evento y filtro por bucket y prefijo (S3-P2-05).
//  3. Extracción del document_id de la clave del objeto.
//  4. Transición condicional PENDING_UPLOAD → UPLOADED (S3-P2-07).
//  5. Validación %PDF- post-subida (S2-P2-02) — si no es PDF → REJECTED.
func (h *Handler) HandleEvent(c *gin.Context) {
	// 1. Autenticación (S3-P2-06).
	token := c.GetHeader("X-Minio-Webhook-Token")
	if token == "" {
		// También aceptar Authorization: Bearer <token>.
		auth := c.GetHeader("Authorization")
		if strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		}
	}

	if !h.verifyToken(token) {
		slog.Warn("webhook: token inválido o ausente")
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"error": "unauthorized",
		})
		return
	}

	// Leer body.
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		slog.Error("webhook: error leyendo body", "error", err)
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "bad request"})
		return
	}

	// 2. Parsear evento.
	var event minioEvent
	if err := json.Unmarshal(body, &event); err != nil {
		slog.Error("webhook: JSON inválido", "error", err)
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	// Verificar que es un evento ObjectCreated.
	if !strings.Contains(event.EventName, "ObjectCreated") {
		slog.Debug("webhook: evento ignorado (no ObjectCreated)", "event", event.EventName)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "not ObjectCreated"})
		return
	}

	// 3. Filtrar por bucket y prefijo (S3-P2-05).
	if event.S3.Bucket.Name != h.bucketRaw {
		slog.Debug("webhook: evento de bucket distinto ignorado",
			"bucket", event.S3.Bucket.Name, "esperado", h.bucketRaw)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "wrong bucket"})
		return
	}

	objectKey := event.S3.Object.Key
	if !strings.HasPrefix(objectKey, h.keyPrefix) {
		slog.Debug("webhook: clave con prefijo inesperado ignorada",
			"key", objectKey, "prefijo_esperado", h.keyPrefix)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "wrong prefix"})
		return
	}

	// 4. Extraer document_id de la clave: raw-pdfs/<id>.pdf → <id>.
	docID := h.extractDocumentID(objectKey)
	if docID == "" {
		slog.Warn("webhook: no se pudo extraer document_id", "key", objectKey)
		c.JSON(http.StatusOK, gin.H{"status": "ignored", "reason": "cannot extract document_id"})
		return
	}

	slog.Info("webhook: evento procesado",
		"document_id", docID,
		"object_key", objectKey,
		"event", event.EventName,
	)

	// 5. Transición condicional PENDING_UPLOAD → UPLOADED (S3-P2-07).
	ctx := c.Request.Context()
	ok, err := h.repo.UpdateStatus(ctx, docID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": objectKey},
	)
	if err != nil {
		slog.Error("webhook: error actualizando estado",
			"document_id", docID, "error", err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	if !ok {
		// El documento no estaba en PENDING_UPLOAD: puede ser un evento duplicado
		// o el documento ya fue procesado. Idempotente: no es un error.
		slog.Debug("webhook: transición no aplicada (documento no en PENDING_UPLOAD)",
			"document_id", docID)
		c.JSON(http.StatusOK, gin.H{"status": "no_transition", "document_id": docID})
		return
	}

	// 6. Validar %PDF- post-subida (S2-P2-02, defensa en profundidad).
	// Si no es PDF → REJECTED (terminal, sin reintentos).
	if err := h.validatePDF(ctx, docID, objectKey); err != nil {
		slog.Warn("webhook: PDF inválido, marcando REJECTED",
			"document_id", docID, "error", err)
		c.JSON(http.StatusOK, gin.H{"status": "rejected", "document_id": docID})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "document_id": docID})
}

// validatePDF verifica la cabecera %PDF- del objeto recién subido.
// Si no es PDF, transiciona a REJECTED con causa NOT_A_PDF.
func (h *Handler) validatePDF(ctx context.Context, docID, objectKey string) error {
	data, err := h.storage.GetRange(ctx, objectKey, 0, 5)
	if err != nil {
		return fmt.Errorf("get range: %w", err)
	}

	if !isPDF(data) {
		// Transición UPLOADED → REJECTED (terminal, sin reintentos).
		_, err := h.repo.UpdateStatus(ctx, docID,
			domain.StatusUploaded, domain.StatusRejected,
			map[string]any{"failure_reason": "NOT_A_PDF"},
		)
		if err != nil {
			return fmt.Errorf("marcar REJECTED: %w", err)
		}
		return ErrNotPDF
	}

	return nil
}

// verifyToken compara el token con el secreto en tiempo constante (S3-P2-06).
func (h *Handler) verifyToken(token string) bool {
	if h.webhookSecret == "" {
		// Sin secreto configurado: rechazar por seguridad.
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(h.webhookSecret)) == 1
}

// extractDocumentID extrae el document_id de la clave del objeto.
// raw-pdfs/<id>.pdf → <id>.
func (h *Handler) extractDocumentID(objectKey string) string {
	// Remover el prefijo.
	rest := strings.TrimPrefix(objectKey, h.keyPrefix)
	// Remover la extensión .pdf.
	rest = strings.TrimSuffix(rest, ".pdf")
	// Remover cualquier subdirectorio extra.
	if idx := strings.LastIndex(rest, "/"); idx >= 0 {
		rest = rest[idx+1:]
	}
	return rest
}

// ErrNotPDF se devuelve cuando el objeto no es un PDF válido.
var ErrNotPDF = errors.New("el objeto no es un PDF válido")

// isPDF verifica si los bytes empiezan por %PDF-.
func isPDF(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return string(data[:5]) == "%PDF-"
}

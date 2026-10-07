// Package api implementa los endpoints operativos internos del doc-service (S4-P2-04/05/06).
// Están protegidos por un token compartido (header X-Internal-Token).
package api

import (
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// InternalHandler implementa los endpoints operativos internos.
type InternalHandler struct {
	repo          ports.DocumentRepository
	storage       ports.ObjectStorage
	queue         ports.WorkQueue
	internalToken string
}

// NewInternalHandler crea el handler con las dependencias inyectadas.
func NewInternalHandler(
	repo ports.DocumentRepository,
	storage ports.ObjectStorage,
	queue ports.WorkQueue,
	internalToken string,
) *InternalHandler {
	return &InternalHandler{
		repo:          repo,
		storage:       storage,
		queue:         queue,
		internalToken: internalToken,
	}
}

// RegisterRoutes registra las rutas operativas internas.
func (h *InternalHandler) RegisterRoutes(engine *gin.Engine) {
	internal := engine.Group("/internal")
	internal.Use(h.authMiddleware())
	{
		internal.GET("/documents/:id/history", h.GetHistory)
		internal.POST("/documents/:id/retry", h.RetryDocument)
		internal.POST("/documents/:id/cancel", h.CancelDocument)
	}
}

// authMiddleware verifica el token interno en tiempo constante (S4-P2-06).
func (h *InternalHandler) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Internal-Token")
		if token == "" {
			auth := c.GetHeader("Authorization")
			if len(auth) > 7 && auth[:7] == "Bearer " {
				token = auth[7:]
			}
		}

		if h.internalToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(h.internalToken)) != 1 {
			slog.Warn("internal: token inválido o ausente", "path", c.Request.URL.Path)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

// GetHistory maneja GET /internal/documents/:id/history (S4-P2-04).
// Devuelve la cronología completa de transiciones del documento.
func (h *InternalHandler) GetHistory(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id requerido"})
		return
	}

	doc, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "documento no encontrado"})
			return
		}
		slog.Error("internal history: get document", "error", err, "document_id", id)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "error interno"})
		return
	}

	entries := make([]gin.H, 0, len(doc.History))
	for _, e := range doc.History {
		entries = append(entries, gin.H{
			"status":         string(e.Status),
			"actor":          string(e.Actor),
			"correlation_id": e.CorrelationID,
			"at":             e.At,
			"reason":         e.Reason,
		})
	}

	slog.Info("internal history: consulta de cronología",
		"document_id", id, "entries", len(entries),
	)

	c.JSON(http.StatusOK, gin.H{
		"document_id": doc.ID,
		"status":      string(doc.Status),
		"history":     entries,
		"count":       len(entries),
	})
}

// RetryDocument maneja POST /internal/documents/:id/retry (S4-P2-05).
// Reintenta un documento en estado EXTRACTION_FAILED → COMPENSATING → (worker decide).
// Solo aplica a documentos en EXTRACTION_FAILED.
func (h *InternalHandler) RetryDocument(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id requerido"})
		return
	}

	doc, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "documento no encontrado"})
			return
		}
		slog.Error("internal retry: get document", "error", err, "document_id", id)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "error interno"})
		return
	}

	// Solo EXTRACTION_FAILED puede ser reintentado directamente.
	// RETRYING es transición desde EXTRACTION_FAILED según la matriz.
	if doc.Status != domain.StatusExtractionFailed && doc.Status != domain.StatusRetrying {
		c.JSON(http.StatusConflict, gin.H{
			"error":          "estado no permite reintento",
			"current_status": string(doc.Status),
			"allowed_from":   []string{string(domain.StatusExtractionFailed), string(domain.StatusRetrying)},
		})
		return
	}

	// Transicionar a RETRYING (o PROCESSING si ya está en RETRYING).
	to := domain.StatusRetrying
	if doc.Status == domain.StatusRetrying {
		to = domain.StatusProcessing
	}

	entry := domain.StatusEntry{
		Status:        to,
		Actor:         domain.ActorDocService,
		CorrelationID: doc.CorrelationID,
	}

	ok, err := h.repo.UpdateStatusWithHistory(c.Request.Context(), id,
		doc.Status, to, entry, map[string]any{},
	)
	if err != nil {
		slog.Error("internal retry: update status", "error", err, "document_id", id)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "error actualizando estado"})
		return
	}
	if !ok {
		c.JSON(http.StatusConflict, gin.H{"error": "transición no aplicada (documento cambió de estado)"})
		return
	}

	// Reencolar si hay queue disponible.
	if h.queue != nil && doc.ObjectKey != "" {
		msg := ports.WorkMessage{
			DocumentID:    doc.ID,
			ObjectKey:     doc.ObjectKey,
			CorrelationID: doc.CorrelationID,
			EnqueuedAt:    time.Now().UTC().Format(time.RFC3339),
			SchemaVersion: doc.SchemaVersion,
		}
		if err := h.queue.Enqueue(c.Request.Context(), msg); err != nil {
			slog.Error("internal retry: enqueue failed", "error", err, "document_id", id)
			// No fallar el request: la transición ya se aplicó.
		}
	}

	slog.Info("internal retry: documento reintentado",
		"document_id", id, "from", doc.Status, "to", to,
	)

	c.JSON(http.StatusOK, gin.H{
		"document_id": id,
		"status":      string(to),
		"reenqueued":  h.queue != nil,
	})
}

// CancelDocument maneja POST /internal/documents/:id/cancel (S4-P2-05).
// Cancela un documento en PENDING_UPLOAD → UPLOAD_EXPIRED (terminal).
func (h *InternalHandler) CancelDocument(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id requerido"})
		return
	}

	doc, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "documento no encontrado"})
			return
		}
		slog.Error("internal cancel: get document", "error", err, "document_id", id)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "error interno"})
		return
	}

	// Solo PENDING_UPLOAD puede ser cancelado.
	if doc.Status != domain.StatusPendingUpload {
		c.JSON(http.StatusConflict, gin.H{
			"error":          "estado no permite cancelación",
			"current_status": string(doc.Status),
			"allowed_from":   []string{string(domain.StatusPendingUpload)},
		})
		return
	}

	entry := domain.StatusEntry{
		Status:        domain.StatusUploadExpired,
		Actor:         domain.ActorDocService,
		CorrelationID: doc.CorrelationID,
		Reason:        "CANCELLED_BY_OPERATOR",
	}

	ok, err := h.repo.UpdateStatusWithHistory(c.Request.Context(), id,
		domain.StatusPendingUpload, domain.StatusUploadExpired,
		entry, map[string]any{"failure_reason": domain.FailureReasonCancelled},
	)
	if err != nil {
		slog.Error("internal cancel: update status", "error", err, "document_id", id)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "error actualizando estado"})
		return
	}
	if !ok {
		c.JSON(http.StatusConflict, gin.H{"error": "transición no aplicada (documento cambió de estado)"})
		return
	}

	slog.Info("internal cancel: documento cancelado",
		"document_id", id,
	)

	c.JSON(http.StatusOK, gin.H{
		"document_id": id,
		"status":      string(domain.StatusUploadExpired),
	})
}

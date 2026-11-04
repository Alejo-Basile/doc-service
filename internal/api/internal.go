// Package api implementa los endpoints operativos internos del doc-service (S4-P2-04/05/06).
// Están protegidos por un token compartido (header X-Internal-Token).
package api

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	httpserver "github.com/Alejo-Basile/doc-service/internal/transport/http"
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
	internal.Use(httpserver.InternalTokenMiddleware(h.internalToken))
	{
		internal.GET("/documents/:id/history", h.GetHistory)
		internal.POST("/documents/:id/retry", h.RetryDocument)
		internal.POST("/documents/:id/cancel", h.CancelDocument)
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

// RetryDocument maneja POST /internal/documents/:id/retry (S4-P2-06).
// Reintenta un documento en EXTRACTION_FAILED (→ RETRYING) o en RETRYING
// (reencola sin cambiar de estado: la transición a PROCESSING la hace el worker
// al reclamar el mensaje).
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

	if doc.Status != domain.StatusExtractionFailed && doc.Status != domain.StatusRetrying {
		c.JSON(http.StatusConflict, gin.H{
			"error":          "estado no permite reintento",
			"current_status": string(doc.Status),
			"allowed_from":   []string{string(domain.StatusExtractionFailed), string(domain.StatusRetrying)},
		})
		return
	}

	to := domain.StatusRetrying
	if doc.Status == domain.StatusExtractionFailed {
		// Defensa en profundidad: la arista del operador está en la matriz,
		// pero la API no debe escribir transiciones que el dominio rechace.
		if !domain.CanTransitionFrom(doc.Status, to) {
			c.JSON(http.StatusConflict, gin.H{
				"error":          "transición no permitida por la máquina de estados",
				"current_status": string(doc.Status),
				"target_status":  string(to),
			})
			return
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
	}
	// Si ya estaba en RETRYING, no se transiciona: sólo se reencola.

	reenqueued := false
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
			// La transición ya se aplicó, pero el reintento no está en curso.
			// El documento queda en RETRYING, desde donde se puede reintentar.
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"document_id": id,
				"status":      string(to),
				"reenqueued":  false,
				"error":       "no se pudo encolar el reintento; reintentar la operación",
			})
			return
		}
		reenqueued = true
	}

	slog.Info("internal retry: documento reintentado",
		"document_id", id, "from", doc.Status, "to", to, "reenqueued", reenqueued,
	)

	c.JSON(http.StatusOK, gin.H{
		"document_id": id,
		"status":      string(to),
		"reenqueued":  reenqueued,
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

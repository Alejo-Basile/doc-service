// Package api implementa los handlers HTTP de la API v2 del doc-service.
package api

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/pdfsvc"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// DocumentHandler implementa los endpoints de la API v2.
type DocumentHandler struct {
	repo        ports.DocumentRepository
	storage     ports.ObjectStorage
	validator   *pdfsvc.Validator
	maxPDFBytes int64
	uploadGrace time.Duration
}

// NewDocumentHandler crea el handler con las dependencias inyectadas.
func NewDocumentHandler(
	repo ports.DocumentRepository,
	storage ports.ObjectStorage,
	validator *pdfsvc.Validator,
	maxPDFBytes int64,
	uploadGrace time.Duration,
) *DocumentHandler {
	return &DocumentHandler{
		repo:        repo,
		storage:     storage,
		validator:   validator,
		maxPDFBytes: maxPDFBytes,
		uploadGrace: uploadGrace,
	}
}

// RegisterRoutes registra las rutas de la API v2.
func (h *DocumentHandler) RegisterRoutes(engine *gin.Engine) {
	v2 := engine.Group("/api/v2")
	{
		v2.POST("/documents", h.CreateDocument)
		v2.GET("/documents/:id", h.GetDocument)
	}
}

// CreateDocumentRequest es el body del POST /api/v2/documents.
type CreateDocumentRequest struct {
	// IdempotencyKey es la clave de idempotencia del cliente.
	// Si se provee, se usa como _id del documento (S3-P2-01).
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// SizeBytes es el tamaño declarado del PDF (opcional, para validar límite).
	SizeBytes int64 `json:"size_bytes,omitempty"`
}

// CreateDocumentResponse es la respuesta del POST /api/v2/documents.
type CreateDocumentResponse struct {
	DocumentID string            `json:"document_id"`
	Status     string            `json:"status"`
	UploadURL  string            `json:"upload_url"`
	Method     string            `json:"method"`
	Fields     map[string]string `json:"form_fields"`
	ExpiresIn  int               `json:"expires_in"` // segundos
}

// CreateDocument maneja POST /api/v2/documents (S3-P2-01, S3-P2-02).
// Idempotente: si ya existe un documento con la misma clave, devuelve el existente (200).
func (h *DocumentHandler) CreateDocument(c *gin.Context) {
	var req CreateDocumentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondProblem(c, http.StatusBadRequest, "validation", "JSON inválido", err.Error())
		return
	}

	// Validar tamaño declarado si se provee.
	if req.SizeBytes > 0 && h.maxPDFBytes > 0 && req.SizeBytes > h.maxPDFBytes {
		respondProblem(c, http.StatusBadRequest, "validation",
			"Error de validación",
			"size_bytes excede el máximo permitido: "+itoa(h.maxPDFBytes)+" bytes")
		return
	}

	corrID := c.GetString("correlation_id")

	// Generar o usar el idempotency key como document_id.
	docID := req.IdempotencyKey

	if docID != "" {
		// Buscar si ya existe (idempotencia).
		existing, err := h.repo.GetByID(c.Request.Context(), docID)
		if err == nil && existing != nil {
			// Documento existente: devolver 200 con el estado actual.
			h.respondExisting(c, existing)
			return
		}
		if err != nil && !errors.Is(err, ports.ErrNotFound) {
			// Error real de Mongo.
			slog.Error("get document for idempotency", "error", err, "document_id", docID)
			respondProblem(c, http.StatusInternalServerError, "internal", "Error consultando documento", "")
			return
		}
		// No existe: crear con ese _id.
	} else {
		// Sin idempotency key: generar ULID en NewDocument.
		docID = ""
	}

	// Crear documento.
	expiresAt := time.Now().UTC().Add(h.uploadGrace)
	var doc *domain.Document
	if docID != "" {
		doc = &domain.Document{
			ID:            docID,
			Status:        domain.StatusPendingUpload,
			CorrelationID: corrID,
			SchemaVersion: 1,
			ExpiresAt:     expiresAt,
			CreatedAt:     time.Now().UTC(),
			UpdatedAt:     time.Now().UTC(),
			History: []domain.StatusEntry{
				{Status: domain.StatusPendingUpload, Actor: domain.ActorClient, CorrelationID: corrID, At: time.Now().UTC()},
			},
		}
	} else {
		doc = domain.NewDocument(corrID, expiresAt)
	}

	// Fijar object_key derivado del document_id.
	doc.SetObjectKey("raw-pdfs")

	// Insertar (idempotente por _id).
	if err := h.repo.Insert(c.Request.Context(), doc); err != nil {
		if isDuplicate(err) {
			// Carrera: otro request creó el mismo documento.
			existing, getErr := h.repo.GetByID(c.Request.Context(), doc.ID)
			if getErr == nil {
				h.respondExisting(c, existing)
				return
			}
		}
		slog.Error("insert document", "error", err, "document_id", doc.ID)
		respondProblem(c, http.StatusInternalServerError, "internal", "Error creando documento", "")
		return
	}

	// Generar URL prefirmada POST.
	presignOpts := ports.PresignPostOptions{
		Bucket:       "raw-pdfs",
		MaxSizeBytes: h.maxPDFBytes,
		ContentType:  "application/pdf",
		Expiration:   15 * time.Minute,
	}

	result, err := h.storage.PresignPost(c.Request.Context(), doc.ObjectKey, presignOpts)
	if err != nil {
		slog.Error("presign post", "error", err, "object_key", doc.ObjectKey)
		respondProblem(c, http.StatusInternalServerError, "internal", "Error generando URL de subida", "")
		return
	}

	slog.Info("documento creado",
		"document_id", doc.ID,
		"correlation_id", corrID,
		"object_key", doc.ObjectKey,
	)

	c.JSON(http.StatusCreated, CreateDocumentResponse{
		DocumentID: doc.ID,
		Status:     string(doc.Status),
		UploadURL:  result.UploadURL,
		Method:     "POST",
		Fields:     result.Fields,
		ExpiresIn:  int(result.ExpiresIn.Seconds()),
	})
}

// GetDocument maneja GET /api/v2/documents/:id (S2-P2-05).
func (h *DocumentHandler) GetDocument(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		respondProblem(c, http.StatusBadRequest, "validation", "id de documento requerido", "")
		return
	}

	doc, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			respondProblem(c, http.StatusNotFound, "not_found", "Recurso no encontrado", "id="+id)
			return
		}
		slog.Error("get document", "error", err, "document_id", id)
		respondProblem(c, http.StatusInternalServerError, "internal", "Error interno del servidor", "")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"document_id":    doc.ID,
		"status":         string(doc.Status),
		"object_key":     doc.ObjectKey,
		"txt_ref":        doc.TxtRef,
		"correlation_id": doc.CorrelationID,
		"schema_version": doc.SchemaVersion,
		"failure_reason": doc.FailureReason,
		"created_at":     doc.CreatedAt,
		"updated_at":     doc.UpdatedAt,
		"expires_at":     doc.ExpiresAt,
	})
}

// respondExisting responde con un documento existente (idempotencia).
func (h *DocumentHandler) respondExisting(c *gin.Context, doc *domain.Document) {
	slog.Info("documento existente devuelto (idempotencia)",
		"document_id", doc.ID,
		"status", doc.Status,
	)

	// Regenerar URL prefirmada para el documento existente.
	presignOpts := ports.PresignPostOptions{
		Bucket:       "raw-pdfs",
		MaxSizeBytes: h.maxPDFBytes,
		ContentType:  "application/pdf",
		Expiration:   15 * time.Minute,
	}

	result, err := h.storage.PresignPost(c.Request.Context(), doc.ObjectKey, presignOpts)
	if err != nil {
		slog.Error("presign post (existing)", "error", err)
		respondProblem(c, http.StatusInternalServerError, "internal", "Error generando URL de subida", "")
		return
	}

	c.JSON(http.StatusOK, CreateDocumentResponse{
		DocumentID: doc.ID,
		Status:     string(doc.Status),
		UploadURL:  result.UploadURL,
		Method:     "POST",
		Fields:     result.Fields,
		ExpiresIn:  int(result.ExpiresIn.Seconds()),
	})
}

// --- helpers ---

// respondProblem responde con formato RFC 9457 problem+json.
func respondProblem(c *gin.Context, status int, kind, title, detail string) {
	c.AbortWithStatusJSON(status, gin.H{
		"type":           "https://api.doc-service/errors/" + kind,
		"title":          title,
		"status":         status,
		"detail":         detail,
		"instance":       c.Request.URL.Path,
		"correlation_id": c.GetString("correlation_id"),
	})
}

// isDuplicate verifica si el error es de duplicado de MongoDB.
func isDuplicate(err error) bool {
	// El repositorio envuelve el error; verificamos por string ya que
	// no importamos mongo en esta capa.
	return err != nil && (contains(err.Error(), "duplicate key") || contains(err.Error(), "ya existe"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// Package reconciler implementa el job de reconciliación (SPEC §5.2, S2-P2-06).
//
// v1 (detección + transición condicional):
//   - Documentos PENDING_UPLOAD con expires_at vencido:
//   - Si el objeto NO existe en MinIO → UPLOAD_EXPIRED.
//   - Si el objeto SÍ existe (webhook perdido) → UPLOADED (con validación %PDF-).
//   - Documentos en estado intermedio con updated_at antiguo → solo log (v1 no actúa).
//
// El reconciliador es la red de seguridad: nunca debe borrar datos,
// solo transicionar estados de forma condicional y auditable.
package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// Reconciler ejecuta el sweep periódico de documentos inconsistentes.
type Reconciler struct {
	repo       ports.DocumentRepository
	storage    ports.ObjectStorage
	clock      ports.Clock
	interval   time.Duration
	stuckAfter time.Duration // umbral para detectar estados intermedios colgados
}

// New crea un Reconciler con dependencias inyectadas.
func New(
	repo ports.DocumentRepository,
	storage ports.ObjectStorage,
	clock ports.Clock,
	interval time.Duration,
	stuckAfter time.Duration,
) *Reconciler {
	if stuckAfter <= 0 {
		stuckAfter = 30 * time.Minute
	}
	return &Reconciler{
		repo:       repo,
		storage:    storage,
		clock:      clock,
		interval:   interval,
		stuckAfter: stuckAfter,
	}
}

// RegisterRoutes expone el endpoint manual de reconciliación (para testing/ops).
func (r *Reconciler) RegisterRoutes(engine *gin.Engine) {
	engine.POST("/internal/reconcile", func(c *gin.Context) {
		result, err := r.RunOnce(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, result)
	})
}

// Result resume lo que hizo un sweep.
type Result struct {
	ExpiredPending    int `json:"expired_pending"`
	RecoveredUploaded int `json:"recovered_uploaded"`
	StuckDetected     int `json:"stuck_detected"`
	Errors            int `json:"errors"`
}

// Start ejecuta el sweep en loop con el intervalo configurado.
// Bloquea hasta que el contexto se cancele (graceful shutdown).
func (r *Reconciler) Start(ctx context.Context) {
	slog.Info("reconciler iniciado",
		"interval", r.interval,
		"stuck_after", r.stuckAfter,
	)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("reconciler detenido (context cancelado)")
			return
		case <-ticker.C:
			result, err := r.RunOnce(ctx)
			if err != nil {
				slog.Error("reconciler sweep falló", "error", err)
				continue
			}
			if result.ExpiredPending > 0 || result.RecoveredUploaded > 0 || result.StuckDetected > 0 {
				slog.Info("reconciler sweep completado",
					"expired_pending", result.ExpiredPending,
					"recovered_uploaded", result.RecoveredUploaded,
					"stuck_detected", result.StuckDetected,
					"errors", result.Errors,
				)
			}
		}
	}
}

// RunOnce ejecuta un sweep completo y devuelve el resultado.
func (r *Reconciler) RunOnce(ctx context.Context) (*Result, error) {
	result := &Result{}
	now := r.clock.Now()

	// 1. Documentos PENDING_UPLOAD con expires_at vencido.
	expiredPending, err := r.sweepExpiredPending(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("sweep expired pending: %w", err)
	}
	result.ExpiredPending = expiredPending.expired
	result.RecoveredUploaded = expiredPending.recovered

	// 2. Documentos en estado intermedio con updated_at antiguo (solo detección).
	stuck, err := r.sweepStuckIntermediate(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("sweep stuck: %w", err)
	}
	result.StuckDetected = stuck

	return result, nil
}

// sweepResult acumula contadores de un sweep parcial.
type sweepResult struct {
	expired   int
	recovered int
}

// sweepExpiredPending busca documentos PENDING_UPLOAD vencidos y actúa.
func (r *Reconciler) sweepExpiredPending(ctx context.Context, now time.Time) (*sweepResult, error) {
	result := &sweepResult{}

	// Buscar documentos PENDING_UPLOAD con expires_at < now.
	// Usamos List con filtro de status; el filtro de expires_at se aplica
	// en memoria porque ListFilter no tiene ese campo todavía.
	// En producción se ampliaría con un índice TTL o filtro Mongo.
	docs, _, err := r.repo.List(ctx, ports.ListFilter{
		Status: domain.StatusPendingUpload,
		Limit:  200,
	})
	if err != nil {
		return nil, fmt.Errorf("list pending upload: %w", err)
	}

	for _, doc := range docs {
		if !doc.ExpiresAt.Before(now) {
			continue // aún está dentro de la ventana
		}

		// Verificar si el objeto existe en MinIO.
		exists, err := r.objectExists(ctx, doc.ObjectKey)
		if err != nil {
			slog.Error("reconciler: error verificando objeto",
				"document_id", doc.ID, "object_key", doc.ObjectKey, "error", err)
			continue
		}

		if exists {
			// Objeto existe pero el webhook no llegó: recuperar → UPLOADED.
			// Validar %PDF- antes de transicionar (defensa en profundidad).
			recovered, err := r.validateAndRecover(ctx, doc)
			if err != nil {
				slog.Error("reconciler: error validando/recuperando",
					"document_id", doc.ID, "error", err)
				continue
			}
			if recovered {
				result.recovered++
				slog.Info("reconciler: documento recuperado (webhook perdido)",
					"document_id", doc.ID, "object_key", doc.ObjectKey)
			}
			// Si recovered=false: el PDF era inválido y se marcó REJECTED.
		} else {
			// Objeto no existe → UPLOAD_EXPIRED.
			ok, err := r.repo.UpdateStatus(ctx, doc.ID,
				domain.StatusPendingUpload, domain.StatusUploadExpired,
				map[string]any{"failure_reason": "UPLOAD_WINDOW_EXPIRED"},
			)
			if err != nil {
				slog.Error("reconciler: error marcando UPLOAD_EXPIRED",
					"document_id", doc.ID, "error", err)
				continue
			}
			if ok {
				result.expired++
				slog.Info("reconciler: documento expirado (objeto no encontrado)",
					"document_id", doc.ID)
			}
		}
	}

	return result, nil
}

// validateAndRecover verifica el PDF y transiciona PENDING_UPLOAD → UPLOADED.
// Devuelve (true, nil) si se recuperó; (false, nil) si el PDF era inválido (REJECTED).
func (r *Reconciler) validateAndRecover(ctx context.Context, doc *domain.Document) (bool, error) {
	// Leer cabecera %PDF-.
	data, err := r.storage.GetRange(ctx, doc.ObjectKey, 0, 5)
	if err != nil {
		return false, fmt.Errorf("get range: %w", err)
	}

	if len(data) < 5 || string(data[:5]) != "%PDF-" {
		// No es PDF → REJECTED (terminal).
		_, err := r.repo.UpdateStatus(ctx, doc.ID,
			domain.StatusPendingUpload, domain.StatusRejected,
			map[string]any{"failure_reason": "NOT_A_PDF"},
		)
		return false, err
	}

	// Es PDF válido → UPLOADED.
	_, err = r.repo.UpdateStatus(ctx, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		map[string]any{"object_key": doc.ObjectKey},
	)
	return err == nil, err
}

// objectExists verifica si el objeto existe en MinIO (HEAD).
func (r *Reconciler) objectExists(ctx context.Context, objectKey string) (bool, error) {
	if objectKey == "" {
		return false, nil
	}
	_, err := r.storage.Stat(ctx, objectKey)
	if err != nil {
		// Si el error es de "not found", devolver false sin error.
		// MinIO SDK devuelve errores específicos; tratamos cualquier error
		// como "no existe" solo si es claramente de no encontrado.
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// sweepStuckIntermediate detecta documentos en estados intermedios con
// updated_at antiguo. v1 solo detecta y loguea; no transiciona.
func (r *Reconciler) sweepStuckIntermediate(ctx context.Context, now time.Time) (int, error) {
	stuckCount := 0
	cutoff := now.Add(-r.stuckAfter)

	// Estados intermedios que pueden quedar colgados.
	intermediateStatuses := []domain.Status{
		domain.StatusQueued,
		domain.StatusProcessing,
		domain.StatusRetrying,
		domain.StatusCompensating,
	}

	for _, status := range intermediateStatuses {
		docs, _, err := r.repo.List(ctx, ports.ListFilter{
			Status: status,
			Limit:  100,
		})
		if err != nil {
			return stuckCount, fmt.Errorf("list %s: %w", status, err)
		}

		for _, doc := range docs {
			if doc.UpdatedAt.Before(cutoff) {
				stuckCount++
				slog.Warn("reconciler: documento en estado intermedio colgado",
					"document_id", doc.ID,
					"status", doc.Status,
					"updated_at", doc.UpdatedAt,
					"stuck_since", r.stuckAfter,
				)
			}
		}
	}

	return stuckCount, nil
}

// isNotFound verifica si un error indica que el objeto no existe.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "not found") ||
		contains(msg, "NoSuchKey") ||
		contains(msg, "404") ||
		contains(msg, "NotFound")
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

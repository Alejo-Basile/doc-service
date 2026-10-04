// Package reconciler implementa el job de reconciliación (SPEC §5.2, S2-P2-06, S4-P2-01/02/03/07).
//
// v2 (operativo):
//   - Documentos PENDING_UPLOAD con expires_at vencido:
//   - Si el objeto NO existe en MinIO → UPLOAD_EXPIRED.
//   - Si el objeto SÍ existe (webhook perdido) → UPLOADED (con validación %PDF-).
//   - Documentos UPLOADED estancados cuyo objeto exista → reencolan a Redis Streams.
//   - Objetos huérfanos en MinIO sin documento terminal → purga tras umbral.
//   - Bloqueo distribuido (lease Redis con TTL) para evitar ejecución concurrente.
//
// El reconciliador es la red de seguridad: nunca debe borrar documentos de
// MongoDB, solo transicionar estados de forma condicional y auditable.
// La purga de objetos es operación separada con su propio lease.
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
	queue      ports.WorkQueue
	lease      ports.DistributedLease
	clock      ports.Clock
	interval   time.Duration
	stuckAfter time.Duration // umbral para UPLOADED estancados / reencolado
}

// New crea un Reconciler con dependencias inyectadas.
// queue y lease pueden ser nil (funciona sin reencolado ni bloqueo).
func New(
	repo ports.DocumentRepository,
	storage ports.ObjectStorage,
	queue ports.WorkQueue,
	lease ports.DistributedLease,
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
		queue:      queue,
		lease:      lease,
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
	ExpiredPending    int  `json:"expired_pending"`
	RecoveredUploaded int  `json:"recovered_uploaded"`
	Reenqueued        int  `json:"reenqueued"`
	StuckDetected     int  `json:"stuck_detected"`
	OrphansPurged     int  `json:"orphans_purged"`
	Errors            int  `json:"errors"`
	LockAcquired      bool `json:"lock_acquired"`
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
			if result.ExpiredPending > 0 || result.RecoveredUploaded > 0 ||
				result.Reenqueued > 0 || result.StuckDetected > 0 || result.OrphansPurged > 0 {
				slog.Info("reconciler sweep completado",
					"expired_pending", result.ExpiredPending,
					"recovered_uploaded", result.RecoveredUploaded,
					"reenqueued", result.Reenqueued,
					"stuck_detected", result.StuckDetected,
					"orphans_purged", result.OrphansPurged,
					"errors", result.Errors,
					"lock_acquired", result.LockAcquired,
				)
			}
		}
	}
}

// RunOnce ejecuta un sweep completo y devuelve el resultado.
// Si hay lease configurado y no se puede adquirir, retorna resultado vacío sin error.
func (r *Reconciler) RunOnce(ctx context.Context) (*Result, error) {
	result := &Result{}
	now := r.clock.Now()

	// Bloqueo distribuido (S4-P2-03): evitar ejecución concurrente entre réplicas.
	if r.lease != nil {
		acquired, err := r.lease.Acquire(ctx, "reconciler:sweep", r.interval*2)
		if err != nil {
			return nil, fmt.Errorf("adquirir lease: %w", err)
		}
		if !acquired {
			slog.Debug("reconciler: lease no adquirido (otra réplica activa)")
			return result, nil // otra réplica está trabajando
		}
		result.LockAcquired = true
		defer func() {
			if err := r.lease.Release(ctx, "reconciler:sweep"); err != nil {
				slog.Warn("reconciler: error liberando lease", "error", err)
			}
		}()
	}

	// 1. Documentos PENDING_UPLOAD con expires_at vencido.
	expiredPending, err := r.sweepExpiredPending(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("sweep expired pending: %w", err)
	}
	result.ExpiredPending = expiredPending.expired
	result.RecoveredUploaded = expiredPending.recovered

	// 2. Documentos UPLOADED estancados → reencolar (S4-P2-01/07).
	reenqueued, err := r.sweepStuckUploaded(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("sweep stuck uploaded: %w", err)
	}
	result.Reenqueued = reenqueued

	// 3. Documentos en estado intermedio con updated_at antiguo (solo detección).
	stuck, err := r.sweepStuckIntermediate(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("sweep stuck: %w", err)
	}
	result.StuckDetected = stuck

	// 4. Purga de objetos huérfanos en MinIO (S4-P2-02).
	// Solo si hay lease y storage disponibles; con umbral conservador.
	orphans, err := r.sweepOrphans(ctx, now)
	if err != nil {
		slog.Warn("reconciler: sweep orphans falló (no crítico)", "error", err)
	}
	result.OrphansPurged = orphans

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

		exists, err := r.objectExists(ctx, doc.ObjectKey)
		if err != nil {
			slog.Error("reconciler: error verificando objeto",
				"document_id", doc.ID, "object_key", doc.ObjectKey, "error", err)
			continue
		}

		if exists {
			// Objeto existe pero el webhook no llegó: recuperar → UPLOADED.
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

// sweepStuckUploaded busca documentos UPLOADED estancados cuyo objeto exista
// y los reencola a Redis Streams (S4-P2-01/07).
func (r *Reconciler) sweepStuckUploaded(ctx context.Context, now time.Time) (int, error) {
	if r.queue == nil {
		return 0, nil // sin queue configurado
	}

	reenqueued := 0
	cutoff := now.Add(-r.stuckAfter)

	docs, _, err := r.repo.List(ctx, ports.ListFilter{
		Status: domain.StatusUploaded,
		Limit:  100,
	})
	if err != nil {
		return 0, fmt.Errorf("list uploaded: %w", err)
	}

	for _, doc := range docs {
		// Solo documentos estancados (updated_at antiguo).
		if !doc.UpdatedAt.Before(cutoff) {
			continue
		}

		// Verificar que el objeto exista en MinIO antes de reencolar.
		exists, err := r.objectExists(ctx, doc.ObjectKey)
		if err != nil {
			slog.Error("reconciler: error verificando objeto para reencolar",
				"document_id", doc.ID, "error", err)
			continue
		}
		if !exists {
			// Objeto no existe: marcar FAILED con causa.
			_, _ = r.repo.UpdateStatus(ctx, doc.ID,
				domain.StatusUploaded, domain.StatusFailed,
				map[string]any{"failure_reason": "OBJECT_MISSING"},
			)
			continue
		}

		// Reencolar a Redis Streams.
		msg := ports.WorkMessage{
			DocumentID:    doc.ID,
			ObjectKey:     doc.ObjectKey,
			CorrelationID: doc.CorrelationID,
			EnqueuedAt:    now.Format(time.RFC3339),
			SchemaVersion: doc.SchemaVersion,
		}
		if err := r.queue.Enqueue(ctx, msg); err != nil {
			slog.Error("reconciler: error reencolando documento",
				"document_id", doc.ID, "error", err)
			continue
		}

		// Transicionar UPLOADED → QUEUED (para que no se reencole en el próximo ciclo).
		ok, err := r.repo.UpdateStatus(ctx, doc.ID,
			domain.StatusUploaded, domain.StatusQueued,
			map[string]any{},
		)
		if err != nil {
			slog.Error("reconciler: error transicionando a QUEUED",
				"document_id", doc.ID, "error", err)
			continue
		}
		if ok {
			reenqueued++
			slog.Info("reconciler: documento reencolado (UPLOADED estancado)",
				"document_id", doc.ID, "object_key", doc.ObjectKey)
		}
	}

	return reenqueued, nil
}

// sweepOrphans purga objetos en MinIO que no tengan documento asociado
// y que sean más antiguos que el umbral (S4-P2-02).
// Nunca borra documentos de MongoDB (SPEC).
func (r *Reconciler) sweepOrphans(ctx context.Context, now time.Time) (int, error) {
	// Umbral conservador: 7 días. Los objetos de documentos activos
	// nunca alcanzan esta antigüedad porque los documentos tienen su propio TTL.
	threshold := now.Add(-7 * 24 * time.Hour)

	// Listar todos los documentos (cualquier estado) para saber qué claves son válidas.
	docs, _, err := r.repo.List(ctx, ports.ListFilter{Limit: 10000})
	if err != nil {
		return 0, fmt.Errorf("list all docs: %w", err)
	}

	// Construir set de object_keys válidos.
	validKeys := make(map[string]bool, len(docs))
	for _, doc := range docs {
		if doc.ObjectKey != "" {
			validKeys[doc.ObjectKey] = true
		}
	}

	// Listar objetos en MinIO y purgar huérfanos.
	// Nota: esto requiere un método ListObjects en ObjectStorage.
	// Por ahora, la implementación del adapter MinIO debe proveerlo.
	// Se retorna 0 si el storage no soporta listado.
	purged, err := r.purgeOrphanObjects(ctx, validKeys, threshold)
	if err != nil {
		return purged, err
	}

	return purged, nil
}

// purgeOrphanObjects implementa la purga efectiva. Se delega al adapter.
func (r *Reconciler) purgeOrphanObjects(ctx context.Context, validKeys map[string]bool, threshold time.Time) (int, error) {
	// Usar el adapter de MinIO si implementa ListObjects.
	// Como el puerto ObjectStorage no lo tiene todavía, usamos una aserción.
	type lister interface {
		ListObjects(ctx context.Context) ([]ports.ObjectInfo, error)
	}

	l, ok := r.storage.(lister)
	if !ok {
		return 0, nil // storage no soporta listado
	}

	objects, err := l.ListObjects(ctx)
	if err != nil {
		return 0, fmt.Errorf("list objects: %w", err)
	}

	purged := 0
	for _, obj := range objects {
		// No purgar si el objeto tiene documento asociado.
		if validKeys[obj.Key] {
			continue
		}
		// No purgar si el objeto es más nuevo que el umbral.
		if obj.LastModified.After(threshold) {
			continue
		}

		if err := r.storage.Delete(ctx, obj.Key); err != nil {
			slog.Error("reconciler: error purgando objeto huérfano",
				"object_key", obj.Key, "error", err)
			continue
		}
		purged++
		slog.Info("reconciler: objeto huérfano purgado",
			"object_key", obj.Key,
			"last_modified", obj.LastModified,
		)
	}

	return purged, nil
}

// validateAndRecover verifica el PDF y transiciona PENDING_UPLOAD → UPLOADED.
// Devuelve (true, nil) si se recuperó; (false, nil) si el PDF era inválido (REJECTED).
func (r *Reconciler) validateAndRecover(ctx context.Context, doc *domain.Document) (bool, error) {
	data, err := r.storage.GetRange(ctx, doc.ObjectKey, 0, 5)
	if err != nil {
		return false, fmt.Errorf("get range: %w", err)
	}

	if len(data) < 5 || string(data[:5]) != "%PDF-" {
		_, err := r.repo.UpdateStatus(ctx, doc.ID,
			domain.StatusPendingUpload, domain.StatusRejected,
			map[string]any{"failure_reason": "NOT_A_PDF"},
		)
		return false, err
	}

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
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// sweepStuckIntermediate detecta documentos en estados intermedios con
// updated_at antiguo. Solo detecta y loguea.
func (r *Reconciler) sweepStuckIntermediate(ctx context.Context, now time.Time) (int, error) {
	stuckCount := 0
	cutoff := now.Add(-r.stuckAfter)

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

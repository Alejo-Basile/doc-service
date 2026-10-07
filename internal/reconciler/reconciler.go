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
	repo         ports.DocumentRepository
	storage      ports.ObjectStorage
	queue        ports.WorkQueue
	lease        ports.DistributedLease
	clock        ports.Clock
	interval     time.Duration
	stuckAfter   time.Duration // umbral para UPLOADED estancados / reencolado
	minSafetyAge time.Duration // edad mínima: documentos más jóvenes nunca se tocan (S6-P2-02)
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
	minSafetyAge time.Duration,
) *Reconciler {
	if stuckAfter <= 0 {
		stuckAfter = 30 * time.Minute
	}
	if minSafetyAge <= 0 {
		minSafetyAge = interval // default: al menos 1x intervalo
	}
	return &Reconciler{
		repo:         repo,
		storage:      storage,
		queue:        queue,
		lease:        lease,
		clock:        clock,
		interval:     interval,
		stuckAfter:   stuckAfter,
		minSafetyAge: minSafetyAge,
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
	ExpiredPending    int               `json:"expired_pending"`
	RecoveredUploaded int               `json:"recovered_uploaded"`
	Reenqueued        int               `json:"reenqueued"`
	StuckDetected     int               `json:"stuck_detected"`
	OrphansPurged     int               `json:"orphans_purged"`
	CompletedNoTxt    int               `json:"completed_no_txt"` // COMPLETED sin .txt en MinIO (S5-P2-03)
	Errors            int               `json:"errors"`
	LockAcquired      bool              `json:"lock_acquired"`
	CrossCheckReport  *CrossCheckReport `json:"cross_check,omitempty"` // S6-P2-01
}

// CrossCheckReport resume la verificación cruzada MongoDB ↔ MinIO ↔ Redis (S6-P2-01).
type CrossCheckReport struct {
	// MongoDB
	TotalDocuments int64            `json:"total_documents"`
	ByStatus       map[string]int64 `json:"by_status"`
	// MinIO
	RawBucketObjects int64 `json:"raw_bucket_objects"`
	TXTBucketObjects int64 `json:"txt_bucket_objects"`
	// Redis
	StreamLength int64 `json:"stream_length"`
	// Discrepancias
	OrphanRawObjects  []string `json:"orphan_raw_objects"`  // en MinIO sin documento
	MissingRawObjects []string `json:"missing_raw_objects"` // documento sin objeto en MinIO
	CompletedNoTxt    []string `json:"completed_no_txt"`    // COMPLETED sin .txt
	FailedNoReason    []string `json:"failed_no_reason"`    // FAILED sin failure_reason
	// Seguridad
	YoungUntouched int64 `json:"young_untouched"` // documentos < minSafetyAge no tocados
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
	orphans, err := r.sweepOrphans(ctx, now)
	if err != nil {
		slog.Warn("reconciler: sweep orphans falló (no crítico)", "error", err)
	}
	result.OrphansPurged = orphans

	// 5. Documentos COMPLETED sin .txt en MinIO (S5-P2-03).
	completedNoTxt, err := r.sweepCompletedMissingTxt(ctx, now)
	if err != nil {
		slog.Warn("reconciler: sweep completed-no-txt falló (no crítico)", "error", err)
	}
	result.CompletedNoTxt = completedNoTxt

	// 6. Reporte de verificación cruzada (S6-P2-01).
	report, err := r.crossCheck(ctx, now)
	if err != nil {
		slog.Warn("reconciler: cross-check falló (no crítico)", "error", err)
	}
	result.CrossCheckReport = report

	return result, nil
}

// sweepResult acumula contadores de un sweep parcial.
type sweepResult struct {
	expired   int
	recovered int
}

// sweepExpiredPending busca documentos PENDING_UPLOAD vencidos y actúa.
// Respeta el umbral de seguridad: documentos más jóvenes que minSafetyAge no se tocan.
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
		// S6-P2-02: umbral de seguridad — documentos recién creados no se tocan.
		if now.Sub(doc.CreatedAt) < r.minSafetyAge {
			continue
		}
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
				map[string]any{"failure_reason": domain.FailureReasonUploadExpired},
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
		// S6-P2-02: umbral de seguridad.
		if now.Sub(doc.CreatedAt) < r.minSafetyAge {
			continue
		}
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
			// Objeto no existe: marcar FAILED con causa normalizada.
			_, _ = r.repo.UpdateStatus(ctx, doc.ID,
				domain.StatusUploaded, domain.StatusFailed,
				map[string]any{"failure_reason": domain.FailureReasonObjectMissing},
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
			map[string]any{"failure_reason": domain.FailureReasonNotAPDF},
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

// sweepCompletedMissingTxt detecta documentos COMPLETED cuyo .txt no existe en MinIO (S5-P2-03).
// Solo detecta y alerta: no transiciona automáticamente (eso es trabajo del operador).
// Respeta el umbral de seguridad.
func (r *Reconciler) sweepCompletedMissingTxt(ctx context.Context, now time.Time) (int, error) {
	docs, _, err := r.repo.List(ctx, ports.ListFilter{
		Status: domain.StatusCompleted,
		Limit:  200,
	})
	if err != nil {
		return 0, fmt.Errorf("list completed: %w", err)
	}

	count := 0
	for _, doc := range docs {
		// S6-P2-02: umbral de seguridad.
		if now.Sub(doc.CreatedAt) < r.minSafetyAge {
			continue
		}
		if doc.TxtRef == "" {
			count++
			slog.Warn("reconciler: documento COMPLETED sin txt_ref",
				"document_id", doc.ID,
			)
			continue
		}
		// Verificar que el .txt exista en MinIO.
		_, err := r.storage.Stat(ctx, doc.TxtRef)
		if err != nil {
			if isNotFound(err) {
				count++
				slog.Warn("reconciler: documento COMPLETED pero .txt no existe en MinIO",
					"document_id", doc.ID,
					"txt_ref", doc.TxtRef,
				)
			} else {
				slog.Error("reconciler: error verificando .txt",
					"document_id", doc.ID, "txt_ref", doc.TxtRef, "error", err)
			}
		}
	}

	return count, nil
}

// crossCheck genera un reporte de verificación cruzada MongoDB ↔ MinIO ↔ Redis (S6-P2-01).
// Solo lectura: no modifica ningún estado.
func (r *Reconciler) crossCheck(ctx context.Context, now time.Time) (*CrossCheckReport, error) {
	report := &CrossCheckReport{
		ByStatus: make(map[string]int64),
	}

	// 1. Listar todos los documentos de MongoDB.
	docs, _, err := r.repo.List(ctx, ports.ListFilter{Limit: 10000})
	if err != nil {
		return nil, fmt.Errorf("list all docs: %w", err)
	}
	report.TotalDocuments = int64(len(docs))

	// 2. Contar por estado y detectar discrepancias.
	docKeys := make(map[string]*domain.Document)
	validRawKeys := make(map[string]bool)
	validTxtKeys := make(map[string]bool)

	for _, doc := range docs {
		report.ByStatus[string(doc.Status)]++

		// Documentos jóvenes: no tocar (S6-P2-02).
		if now.Sub(doc.CreatedAt) < r.minSafetyAge {
			report.YoungUntouched++
		}

		if doc.ObjectKey != "" {
			docKeys[doc.ObjectKey] = doc
			validRawKeys[doc.ObjectKey] = true
		}
		if doc.TxtRef != "" {
			validTxtKeys[doc.TxtRef] = true
		}

		// FAILED sin failure_reason (S5-P2-02).
		if doc.Status == domain.StatusFailed && doc.FailureReason == "" {
			report.FailedNoReason = append(report.FailedNoReason, doc.ID)
		}

		// COMPLETED sin .txt (S5-P2-03).
		if doc.Status == domain.StatusCompleted {
			if doc.TxtRef == "" {
				report.CompletedNoTxt = append(report.CompletedNoTxt, doc.ID)
			} else if now.Sub(doc.CreatedAt) >= r.minSafetyAge {
				// Solo verificar si pasó el umbral de seguridad.
				if _, err := r.storage.Stat(ctx, doc.TxtRef); err != nil && isNotFound(err) {
					report.CompletedNoTxt = append(report.CompletedNoTxt, doc.ID)
				}
			}
		}

		// Documentos activos sin objeto en MinIO.
		if doc.ObjectKey != "" && isDocActive(doc.Status) {
			if _, err := r.storage.Stat(ctx, doc.ObjectKey); err != nil && isNotFound(err) {
				report.MissingRawObjects = append(report.MissingRawObjects, doc.ID)
			}
		}
	}

	// 3. Listar objetos de MinIO bucket raw y detectar huérfanos.
	type lister interface {
		ListObjects(ctx context.Context) ([]ports.ObjectInfo, error)
	}
	if l, ok := r.storage.(lister); ok {
		objects, err := l.ListObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("list raw objects: %w", err)
		}
		report.RawBucketObjects = int64(len(objects))
		for _, obj := range objects {
			if !validRawKeys[obj.Key] {
				report.OrphanRawObjects = append(report.OrphanRawObjects, obj.Key)
			}
		}
	}

	// 4. Redis stream length.
	if r.queue != nil {
		if q, ok := r.queue.(interface {
			XLen(ctx context.Context) (int64, error)
		}); ok {
			if n, err := q.XLen(ctx); err == nil {
				report.StreamLength = n
			}
		}
	}

	// Log del reporte.
	slog.Info("reconciler: cross-check report",
		"total_documents", report.TotalDocuments,
		"by_status", report.ByStatus,
		"raw_objects", report.RawBucketObjects,
		"stream_length", report.StreamLength,
		"orphan_raw", len(report.OrphanRawObjects),
		"missing_raw", len(report.MissingRawObjects),
		"completed_no_txt", len(report.CompletedNoTxt),
		"failed_no_reason", len(report.FailedNoReason),
		"young_untouched", report.YoungUntouched,
	)

	return report, nil
}

// isDocActive indica si un estado es "activo" (no terminal, en tránsito).
func isDocActive(s domain.Status) bool {
	switch s {
	case domain.StatusPendingUpload, domain.StatusUploaded, domain.StatusQueued,
		domain.StatusProcessing, domain.StatusRetrying:
		return true
	default:
		return false
	}
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

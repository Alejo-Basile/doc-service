// Package reconciler implementa el job de reconciliación (SPEC §5.2, S2-P2-06, S4-P2-01/02/03/07).
//
// v2 (operativo):
//   - Documentos PENDING_UPLOAD con expires_at vencido:
//   - Si el objeto NO existe en MinIO → UPLOAD_EXPIRED.
//   - Si el objeto SÍ existe (webhook perdido) → UPLOADED (con validación %PDF-).
//   - Documentos UPLOADED estancados cuyo objeto exista → reencolan a Redis Streams.
//   - Documentos en estados intermedios trabados → reencolan o completan la SAGA hasta FAILED.
//   - Objetos huérfanos en MinIO sin documento terminal → purga tras umbral.
//   - Bloqueo distribuido (lease Redis con TTL) para evitar ejecución concurrente.
//
// El reconciliador es la red de seguridad: nunca debe borrar documentos de
// MongoDB, solo transicionar estados de forma condicional y auditable.
// La purga de objetos es operación separada con su propio lease.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/Alejo-Basile/doc-service/internal/metrics"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	httpserver "github.com/Alejo-Basile/doc-service/internal/transport/http"
	"github.com/gin-gonic/gin"
	minio "github.com/minio/minio-go/v7"
)

// Reconciler ejecuta el sweep periódico de documentos inconsistentes.
type Reconciler struct {
	repo          ports.DocumentRepository
	storage       ports.ObjectStorage
	queue         ports.WorkQueue
	lease         ports.DistributedLease
	clock         ports.Clock
	interval      time.Duration
	stuckAfter    time.Duration // umbral para UPLOADED estancados / reencolado
	minSafetyAge  time.Duration // edad mínima: documentos más jóvenes nunca se tocan (S6-P2-02)
	internalToken string        // token compartido para proteger el endpoint manual (SPEC §9)
	dryRun        bool          // modo planificado: detecta y loguea, no muta nada (SPEC §6.5)

	// terminalRetention es la retención aplicada a documentos terminales que
	// llegan sin retention_at (el worker en Rust escribe COMPLETED directo).
	terminalRetention time.Duration

	configSource  ConfigSource
	configChanges <-chan struct{}
}

// LiveConfig es la configuración del reconciliador leída al inicio de cada sweep.
type LiveConfig struct {
	Interval   time.Duration
	StuckAfter time.Duration
	DryRun     bool
}

// ConfigSource devuelve la configuración viva del reconciliador.
type ConfigSource func() LiveConfig

// Option configura un Reconciler en construcción (functional options).
type Option func(*Reconciler)

// WithInternalToken protege el endpoint manual POST /internal/reconcile
// con el token compartido de los endpoints internos (SPEC §9).
func WithInternalToken(token string) Option {
	return func(r *Reconciler) { r.internalToken = token }
}

// WithDryRun activa el modo dry-run: todas las acciones destructivas se
// planifican y loguean, pero ninguna se ejecuta (SPEC §6.5).
func WithDryRun(enabled bool) Option {
	return func(r *Reconciler) { r.dryRun = enabled }
}

func WithConfigSource(src ConfigSource) Option {
	return func(r *Reconciler) { r.configSource = src }
}

func WithConfigChanges(ch <-chan struct{}) Option {
	return func(r *Reconciler) { r.configChanges = ch }
}

// WithTerminalRetention configura cuánto se retiene un documento terminal al
// que le falta retention_at (SPEC §4: el TTL recolecta, no decide).
func WithTerminalRetention(d time.Duration) Option {
	return func(r *Reconciler) {
		if d > 0 {
			r.terminalRetention = d
		}
	}
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
	opts ...Option,
) *Reconciler {
	if stuckAfter <= 0 {
		stuckAfter = 30 * time.Minute
	}
	if minSafetyAge <= 0 {
		minSafetyAge = interval // default: al menos 1x intervalo
	}
	// minSafetyAge es boot-time: la coherencia del conjunto se valida por configkv/Validate().
	r := &Reconciler{
		repo:              repo,
		storage:           storage,
		queue:             queue,
		lease:             lease,
		clock:             clock,
		interval:          interval,
		stuckAfter:        stuckAfter,
		minSafetyAge:      minSafetyAge,
		terminalRetention: 168 * time.Hour,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// resolve devuelve el snapshot vigente de la configuración del reconciliador.
func (r *Reconciler) resolve() LiveConfig {
	if r.configSource == nil {
		return LiveConfig{Interval: r.interval, StuckAfter: r.stuckAfter, DryRun: r.dryRun}
	}
	snap := r.configSource()
	if snap.Interval <= 0 {
		snap.Interval = r.interval
	}
	if snap.StuckAfter <= 0 {
		snap.StuckAfter = r.stuckAfter
	}
	return snap
}

// RegisterRoutes expone el endpoint manual de reconciliación (para testing/ops).
// Protegido con el token interno: no debe quedar accesible sin autenticación.
func (r *Reconciler) RegisterRoutes(engine *gin.Engine) {
	internal := engine.Group("/internal")
	internal.Use(httpserver.InternalTokenMiddleware(r.internalToken))
	internal.POST("/reconcile", func(c *gin.Context) {
		var dryRunOverride *bool
		if raw, ok := c.GetQuery("dry_run"); ok {
			v, err := strconv.ParseBool(raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "parámetro dry_run inválido: usar true/false (o 1/0)",
				})
				return
			}
			dryRunOverride = &v
		}
		result, err := r.runOnce(c.Request.Context(), dryRunOverride)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, result)
	})
	internal.GET("/cross-check", func(c *gin.Context) {
		report, err := r.CrossCheckNow(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, report)
	})
}

// CrossCheckNow ejecuta el cross-check de solo lectura y devuelve el reporte.
func (r *Reconciler) CrossCheckNow(ctx context.Context) (*CrossCheckReport, error) {
	return r.crossCheck(ctx, r.clock.Now())
}

// Result resume lo que hizo un sweep.
type Result struct {
	ExpiredPending    int               `json:"expired_pending"`
	RecoveredUploaded int               `json:"recovered_uploaded"`
	Reenqueued        int               `json:"reenqueued"`
	StuckDetected     int               `json:"stuck_detected"`
	OrphansPurged     int               `json:"orphans_purged"`
	CompletedNoMd     int               `json:"completed_no_md"` // COMPLETED sin artefacto usable (md_ref o txt_ref legacy) en MinIO (S5-P2-03, D5)
	// RetentionBackfilled cuenta documentos terminales a los que se les fijó
	// retention_at (el worker en Rust escribe COMPLETED sin ese campo).
	RetentionBackfilled int `json:"retention_backfilled"`
	Errors            int               `json:"errors"`
	LockAcquired      bool              `json:"lock_acquired"`
	CrossCheckReport  *CrossCheckReport `json:"cross_check,omitempty"` // S6-P2-01
	// DryRun indica que la corrida solo planificó. PlannedActions cuenta las
	// acciones que el sweep propuso y ExecutedActions las que se aplicaron
	// (en dry-run queda siempre en 0, SPEC §6.5, T1).
	DryRun          bool `json:"dry_run"`
	PlannedActions  int  `json:"planned_actions"`
	ExecutedActions int  `json:"executed_actions"`
}

// CrossCheckReport resume la verificación cruzada MongoDB ↔ MinIO ↔ Redis (S6-P2-01).
type CrossCheckReport struct {
	// MongoDB
	TotalDocuments int64            `json:"total_documents"`
	ByStatus       map[string]int64 `json:"by_status"`
	// MinIO
	RawBucketObjects int64 `json:"raw_bucket_objects"`
	MDBucketObjects  int64 `json:"md_bucket_objects"`
	// Redis
	StreamLength int64 `json:"stream_length"`
	// Discrepancias
	OrphanRawObjects  []string `json:"orphan_raw_objects"`  // en MinIO sin documento
	MissingRawObjects []string `json:"missing_raw_objects"` // documento sin objeto en MinIO
	CompletedNoMd     []string `json:"completed_no_md"`     // COMPLETED sin artefacto (md_ref o txt_ref legacy)
	FailedNoReason    []string `json:"failed_no_reason"`    // FAILED sin failure_reason
	// Seguridad
	YoungUntouched int64 `json:"young_untouched"` // documentos < minSafetyAge no tocados
}

// Start ejecuta el sweep en loop con el intervalo configurado.
// Bloquea hasta que el contexto se cancele (graceful shutdown).
func (r *Reconciler) Start(ctx context.Context) {
	snap := r.resolve()
	slog.Info("reconciler iniciado",
		"interval", snap.Interval.String(),
		"stuck_after", snap.StuckAfter.String(),
		"dry_run", snap.DryRun,
	)

	interval := snap.Interval
	if interval <= 0 {
		interval = r.interval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("reconciler detenido (context cancelado)")
			return
		case <-r.configChanges:
			newSnap := r.resolve()
			newInterval := newSnap.Interval
			if newInterval > 0 && newInterval != interval {
				ticker.Reset(newInterval)
				interval = newInterval
				slog.Info("reconciler: intervalo actualizado", "interval", interval.String())
			}
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
	return r.runOnce(ctx, nil)
}

// runOnce implementa el sweep con el modo (dry-run o ejecución) resuelto por el
// llamador: el loop periódico usa el configurado y el endpoint manual permite
// override por invocación (SPEC §6.5).
func (r *Reconciler) runOnce(ctx context.Context, dryRunOverride *bool) (*Result, error) {
	snap := r.resolve()
	dryRun := snap.DryRun
	if dryRunOverride != nil {
		dryRun = *dryRunOverride
	}
	result := &Result{DryRun: dryRun}
	actions := &actionReport{}
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
	expiredPending, err := r.sweepExpiredPending(ctx, now, dryRun, actions)
	if err != nil {
		return nil, fmt.Errorf("sweep expired pending: %w", err)
	}
	result.ExpiredPending = expiredPending.expired
	result.RecoveredUploaded = expiredPending.recovered

	// 2. Documentos UPLOADED estancados → transición + reencolar (S4-P2-01/07).
	reenqueued, err := r.sweepStuckUploaded(ctx, now, dryRun, actions, snap.StuckAfter)
	if err != nil {
		return nil, fmt.Errorf("sweep stuck uploaded: %w", err)
	}
	result.Reenqueued = reenqueued

	// 2b. Documentos QUEUED estancados → reencolar sin cambiar estado (cierra ventana de crash).
	reenqueued2, err := r.sweepStuckQueued(ctx, now, dryRun, actions, snap.StuckAfter)
	if err != nil {
		return nil, fmt.Errorf("sweep stuck queued: %w", err)
	}
	result.Reenqueued += reenqueued2

	// 3. Documentos en estado intermedio con updated_at antiguo: reencolar si el
	// objeto existe o completar la vía de fallo hasta FAILED (SPEC §6.5, T3).
	stuck, stuckReenqueued, err := r.sweepStuckIntermediate(ctx, now, dryRun, actions, snap.StuckAfter)
	if err != nil {
		return nil, fmt.Errorf("sweep stuck: %w", err)
	}
	result.StuckDetected = stuck
	result.Reenqueued += stuckReenqueued

	// 4. Purga de objetos huérfanos en MinIO (S4-P2-02).
	orphans, err := r.sweepOrphans(ctx, now, dryRun, actions)
	if err != nil {
		slog.Warn("reconciler: sweep orphans falló (no crítico)", "error", err)
	}
	result.OrphansPurged = orphans

	// 5. Documentos COMPLETED sin .md en MinIO (S5-P2-03).
	completedNoMd, err := r.sweepCompletedMissingMd(ctx, now)
	if err != nil {
		slog.Warn("reconciler: sweep completed-no-txt falló (no crítico)", "error", err)
	}
	result.CompletedNoMd = completedNoMd

	// 5b. Retención terminal faltante (el worker escribe COMPLETED directo).
	retentionFixed, err := r.sweepMissingRetention(ctx, now)
	if err != nil {
		slog.Warn("reconciler: sweep retención terminal falló (no crítico)", "error", err)
	}
	result.RetentionBackfilled = retentionFixed

	// 6. Reporte de verificación cruzada (S6-P2-01).
	report, err := r.crossCheck(ctx, now)
	if err != nil {
		slog.Warn("reconciler: cross-check falló (no crítico)", "error", err)
	}
	result.CrossCheckReport = report

	result.PlannedActions = actions.planned
	result.ExecutedActions = actions.executed
	if dryRun && actions.planned > 0 {
		metrics.ReconcileDryRun.Inc()
		slog.Info("reconciler: dry-run completado (ninguna mutación ejecutada)",
			"planned_actions", actions.planned,
		)
	}

	return result, nil
}

// actionReport acumula las acciones planificadas por el sweep y cuántas se
// ejecutaron de verdad (en dry-run todas quedan planificadas).
type actionReport struct {
	planned  int
	executed int
}

// updateStatusWithHistory ejecuta la transición condicional (con historial) o,
// en dry-run, solo la registra como acción planificada (SPEC §6.5).
// Devuelve true en dry-run para que el plan incluya las acciones en cascada.
func (r *Reconciler) updateStatusWithHistory(
	ctx context.Context, actions *actionReport, dryRun bool,
	id string, from, to domain.Status, entry domain.StatusEntry, extraSet map[string]any,
) (bool, error) {
	actions.planned++
	if dryRun {
		slog.Info("reconciler: acción planificada (dry-run)",
			"action", "update_status",
			"document_id", id,
			"from", from,
			"to", to,
		)
		return true, nil
	}
	ok, err := r.repo.UpdateStatusWithHistory(ctx, id, from, to, entry, extraSet)
	if err == nil && ok {
		actions.executed++
	}
	return ok, err
}

// enqueue encola el mensaje de trabajo o, en dry-run, solo lo planifica.
func (r *Reconciler) enqueue(ctx context.Context, actions *actionReport, dryRun bool, msg ports.WorkMessage) error {
	actions.planned++
	if dryRun {
		slog.Info("reconciler: acción planificada (dry-run)",
			"action", "enqueue",
			"document_id", msg.DocumentID,
			"object_key", msg.ObjectKey,
		)
		return nil
	}
	if err := r.queue.Enqueue(ctx, msg); err != nil {
		return err
	}
	actions.executed++
	return nil
}

// deleteObject purga el objeto o, en dry-run, solo lo planifica.
func (r *Reconciler) deleteObject(ctx context.Context, actions *actionReport, dryRun bool, objectKey string) error {
	actions.planned++
	if dryRun {
		slog.Info("reconciler: acción planificada (dry-run)",
			"action", "delete_object",
			"object_key", objectKey,
		)
		return nil
	}
	if err := r.storage.Delete(ctx, objectKey); err != nil {
		return err
	}
	actions.executed++
	return nil
}

// deleteMDObject purga un objeto del bucket extracted-md o, en dry-run, solo
// lo planifica. Espeja a deleteObject para que el choke point de dry-run cubra
// también las mutaciones sobre el bucket de markdown (SPEC §6.5).
func (r *Reconciler) deleteMDObject(ctx context.Context, actions *actionReport, dryRun bool, objectKey string) error {
	actions.planned++
	if dryRun {
		slog.Info("reconciler: acción planificada (dry-run)",
			"action", "delete_md_object",
			"object_key", objectKey,
		)
		return nil
	}
	if err := r.storage.DeleteMD(ctx, objectKey); err != nil {
		return err
	}
	actions.executed++
	return nil
}

// sweepResult acumula contadores de un sweep parcial.
type sweepResult struct {
	expired   int
	recovered int
}

// Límites del recorrido de vencidos. El TTL ya no compite con este sweep
// (opera sobre retention_at), pero el barrido acota cuánto trabajo hace por
// corrida para no monopolizar MinIO.
const (
	expiredPendingPageSize = 200
	expiredPendingMaxPages = 10
)

// sweepExpiredPending busca documentos PENDING_UPLOAD vencidos y actúa.
// Respeta el umbral de seguridad: documentos más jóvenes que minSafetyAge no se tocan.
//
// S9: pagina por (expires_at ASC, _id ASC). El orden por defecto de List es
// _id DESC y traía sólo los documentos más nuevos, que todavía no vencieron;
// con un lote grande de vencidos el sweep no decidía ninguno y el TTL los
// borraba sin estado terminal (SPEC §4: el TTL recolecta, no decide).
func (r *Reconciler) sweepExpiredPending(ctx context.Context, now time.Time, dryRun bool, actions *actionReport) (*sweepResult, error) {
	result := &sweepResult{}

	var cursorAt time.Time
	var cursorID string
	seen := make(map[string]struct{}, expiredPendingPageSize)
	for page := 0; page < expiredPendingMaxPages; page++ {
		docs, _, err := r.repo.List(ctx, ports.ListFilter{
			Status:          domain.StatusPendingUpload,
			Limit:           expiredPendingPageSize,
			ExpiredBefore:   now,
			SortByExpires:   true,
			ExpiredCursorAt: cursorAt,
			ExpiredCursorID: cursorID,
		})
		if err != nil {
			return nil, fmt.Errorf("list pending upload: %w", err)
		}
		if len(docs) == 0 {
			break
		}
		fresh := make([]*domain.Document, 0, len(docs))
		for _, doc := range docs {
			if _, ok := seen[doc.ID]; ok {
				continue
			}
			seen[doc.ID] = struct{}{}
			fresh = append(fresh, doc)
		}
		// Guarda contra repositorios que ignoren el cursor: si la página no
		// trae nada nuevo, cortar evita reprocesar el mismo lote.
		if len(fresh) == 0 {
			break
		}
		r.processExpiredPage(ctx, now, dryRun, actions, result, fresh)
		last := docs[len(docs)-1]
		cursorAt, cursorID = last.ExpiresAt, last.ID
		if len(docs) < expiredPendingPageSize {
			break
		}
	}
	return result, nil
}

// processExpiredPage decide una página de documentos vencidos.
func (r *Reconciler) processExpiredPage(
	ctx context.Context, now time.Time, dryRun bool,
	actions *actionReport, result *sweepResult, docs []*domain.Document,
) {
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
			recovered, err := r.validateAndRecover(ctx, doc, actions, dryRun)
			if err != nil {
				slog.Error("reconciler: error validando/recuperando",
					"document_id", doc.ID, "error", err)
				continue
			}
			if recovered {
				result.recovered++
				if !dryRun {
					slog.Info("reconciler: documento recuperado (webhook perdido)",
						"document_id", doc.ID, "object_key", doc.ObjectKey)
				}
			}
		} else {
			// Objeto no existe → UPLOAD_EXPIRED.
			ok, err := r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
				domain.StatusPendingUpload, domain.StatusUploadExpired,
				domain.StatusEntry{
					Status:        domain.StatusUploadExpired,
					Actor:         domain.ActorReconciler,
					CorrelationID: doc.CorrelationID,
					Reason:        domain.FailureReasonUploadExpired,
				},
				nil,
			)
			if err != nil {
				slog.Error("reconciler: error marcando UPLOAD_EXPIRED",
					"document_id", doc.ID, "error", err)
				continue
			}
			if ok {
				result.expired++
				if !dryRun {
					slog.Info("reconciler: documento expirado (objeto no encontrado)",
						"document_id", doc.ID)
				}
			}
		}
	}
}

// sweepStuckUploaded busca documentos UPLOADED estancados cuyo objeto exista
// y los reencola a Redis Streams (S4-P2-01/07).
func (r *Reconciler) sweepStuckUploaded(ctx context.Context, now time.Time, dryRun bool, actions *actionReport, stuckAfter time.Duration) (int, error) {
	if r.queue == nil {
		return 0, nil // sin queue configurado
	}

	reenqueued := 0
	cutoff := now.Add(-stuckAfter)

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
			_, _ = r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
				domain.StatusUploaded, domain.StatusFailed,
				domain.StatusEntry{
					Status:        domain.StatusFailed,
					Actor:         domain.ActorReconciler,
					CorrelationID: doc.CorrelationID,
					Reason:        domain.FailureReasonObjectMissing,
				},
				nil,
			)
			continue
		}

		// Invariante de orden: transicionar UPLOADED → QUEUED PRIMERO, luego XADD.
		ok, err := r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
			domain.StatusUploaded, domain.StatusQueued,
			domain.StatusEntry{
				Status:        domain.StatusQueued,
				Actor:         domain.ActorReconciler,
				CorrelationID: doc.CorrelationID,
			},
			nil,
		)
		if err != nil {
			slog.Error("reconciler: error transicionando a QUEUED",
				"document_id", doc.ID, "error", err)
			continue
		}
		if !ok {
			// Ya no estaba UPLOADED; omitir.
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
		if err := r.enqueue(ctx, actions, dryRun, msg); err != nil {
			slog.Error("reconciler: error reencolando documento",
				"document_id", doc.ID, "error", err)
			continue
		}
		reenqueued++
		if !dryRun {
			slog.Info("reconciler: documento reencolado (UPLOADED estancado)",
				"document_id", doc.ID, "object_key", doc.ObjectKey)
		}
	}

	return reenqueued, nil
}

// sweepStuckQueued reencola documentos QUEUED cuyo mensaje pudo haberse perdido
// (crash entre transición y XADD, o ACK espurio). No cambia el estado.
func (r *Reconciler) sweepStuckQueued(ctx context.Context, now time.Time, dryRun bool, actions *actionReport, stuckAfter time.Duration) (int, error) {
	if r.queue == nil {
		return 0, nil
	}
	reenqueued := 0
	cutoff := now.Add(-stuckAfter)

	docs, _, err := r.repo.List(ctx, ports.ListFilter{
		Status: domain.StatusQueued,
		Limit:  100,
	})
	if err != nil {
		return 0, fmt.Errorf("list queued: %w", err)
	}

	for _, doc := range docs {
		if doc.ObjectKey == "" {
			continue
		}
		if now.Sub(doc.CreatedAt) < r.minSafetyAge {
			continue
		}
		if !doc.UpdatedAt.Before(cutoff) {
			continue
		}
		msg := ports.WorkMessage{
			DocumentID:    doc.ID,
			ObjectKey:     doc.ObjectKey,
			CorrelationID: doc.CorrelationID,
			EnqueuedAt:    now.Format(time.RFC3339),
			SchemaVersion: doc.SchemaVersion,
		}
		if err := r.enqueue(ctx, actions, dryRun, msg); err != nil {
			slog.Error("reconciler: error reencolando QUEUED",
				"document_id", doc.ID, "error", err)
			continue
		}
		reenqueued++
	}
	return reenqueued, nil
}

// sweepOrphans purga objetos en MinIO que no tengan documento asociado
// y que sean más antiguos que el umbral (S4-P2-02).
// Nunca borra documentos de MongoDB (SPEC).
func (r *Reconciler) sweepOrphans(ctx context.Context, now time.Time, dryRun bool, actions *actionReport) (int, error) {
	// Umbral conservador: 7 días. Los objetos de documentos activos
	// nunca alcanzan esta antigüedad porque los documentos tienen su propio TTL.
	threshold := now.Add(-7 * 24 * time.Hour)

	// Listar todos los documentos (cualquier estado) para saber qué claves son válidas.
	docs, _, err := r.repo.List(ctx, ports.ListFilter{Limit: 10000})
	if err != nil {
		return 0, fmt.Errorf("list all docs: %w", err)
	}

	// Construir los sets de claves válidas con el mismo predicado del pass raw:
	// cualquier documento (sin importar estado ni edad) con la referencia
	// seteada protege su objeto.
	validKeys := make(map[string]bool, len(docs))
	validMdKeys := make(map[string]bool, len(docs))
	for _, doc := range docs {
		if doc.ObjectKey != "" {
			validKeys[doc.ObjectKey] = true
		}
		if doc.MdRef != "" {
			validMdKeys[doc.MdRef] = true
		}
	}

	// Listar objetos en MinIO y purgar huérfanos del bucket raw.
	// Nota: esto requiere un método ListObjects en ObjectStorage.
	// Por ahora, la implementación del adapter MinIO debe proveerlo.
	// Se retorna 0 si el storage no soporta listado.
	purged, err := r.purgeOrphanObjects(ctx, validKeys, threshold, dryRun, actions)
	if err != nil {
		return purged, err
	}

	// Segundo pass: huérfanos del bucket extracted-md (SPEC §6.5). Mismo
	// umbral de edad y misma regla de referencia (doc.MdRef).
	purgedMD, err := r.purgeOrphanMD(ctx, validMdKeys, threshold, dryRun, actions)
	if err != nil {
		return purged + purgedMD, err
	}

	return purged + purgedMD, nil
}

// purgeOrphanObjects implementa la purga efectiva. Se delega al adapter.
func (r *Reconciler) purgeOrphanObjects(ctx context.Context, validKeys map[string]bool, threshold time.Time, dryRun bool, actions *actionReport) (int, error) {
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
		// Ignorar entradas de prefijo (common prefixes) del listado.
		if strings.HasSuffix(obj.Key, "/") {
			continue
		}
		// No purgar si el objeto tiene documento asociado.
		if validKeys[obj.Key] {
			continue
		}
		// No purgar si el objeto es más nuevo que el umbral.
		if obj.LastModified.After(threshold) {
			continue
		}

		if err := r.deleteObject(ctx, actions, dryRun, obj.Key); err != nil {
			slog.Error("reconciler: error purgando objeto huérfano",
				"object_key", obj.Key, "error", err)
			continue
		}
		purged++
		if !dryRun {
			slog.Info("reconciler: objeto huérfano purgado",
				"object_key", obj.Key,
				"last_modified", obj.LastModified,
			)
		}
	}

	return purged, nil
}

// purgeOrphanMD purga objetos del bucket extracted-md sin documento que los
// referencie (MdRef) y más antiguos que el umbral. Espeja la semántica de
// purgeOrphanObjects (mismo umbral, idempotencia y choke point de dry-run),
// pero sobre el bucket de markdown (SPEC §6.5).
func (r *Reconciler) purgeOrphanMD(ctx context.Context, validKeys map[string]bool, threshold time.Time, dryRun bool, actions *actionReport) (int, error) {
	objects, err := r.storage.ListObjectsMD(ctx)
	if err != nil {
		return 0, fmt.Errorf("list md objects: %w", err)
	}

	purged := 0
	for _, obj := range objects {
		// Ignorar entradas de prefijo (common prefixes) del listado.
		if strings.HasSuffix(obj.Key, "/") {
			continue
		}
		// No purgar si algún documento referencia el .md.
		if validKeys[obj.Key] {
			continue
		}
		// No purgar si el objeto es más nuevo que el umbral.
		if obj.LastModified.After(threshold) {
			continue
		}

		if err := r.deleteMDObject(ctx, actions, dryRun, obj.Key); err != nil {
			slog.Error("reconciler: error purgando objeto huérfano md",
				"object_key", obj.Key, "error", err)
			continue
		}
		purged++
		if !dryRun {
			slog.Info("reconciler: objeto huérfano md purgado",
				"object_key", obj.Key,
				"last_modified", obj.LastModified,
			)
		}
	}

	return purged, nil
}

// validateAndRecover verifica el PDF y transiciona PENDING_UPLOAD → UPLOADED.
// Devuelve (true, nil) si se recuperó; (false, nil) si el PDF era inválido (REJECTED).
func (r *Reconciler) validateAndRecover(ctx context.Context, doc *domain.Document, actions *actionReport, dryRun bool) (bool, error) {
	data, err := r.storage.GetRange(ctx, doc.ObjectKey, 0, 5)
	if err != nil {
		return false, fmt.Errorf("get range: %w", err)
	}

	if len(data) < 5 || string(data[:5]) != "%PDF-" {
		_, err := r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
			domain.StatusPendingUpload, domain.StatusRejected,
			domain.StatusEntry{
				Status:        domain.StatusRejected,
				Actor:         domain.ActorReconciler,
				CorrelationID: doc.CorrelationID,
				Reason:        domain.FailureReasonNotAPDF,
			},
			nil,
		)
		return false, err
	}

	_, err = r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
		domain.StatusPendingUpload, domain.StatusUploaded,
		domain.StatusEntry{
			Status:        domain.StatusUploaded,
			Actor:         domain.ActorReconciler,
			CorrelationID: doc.CorrelationID,
		},
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

// sweepStuckIntermediate recupera documentos en estados intermedios con
// updated_at antiguo (SPEC §6.5, T3), usando solo aristas declaradas en la
// matriz del dominio (SPEC §4):
//   - PROCESSING/RETRYING con el objeto raw presente → reencolar (espeja el
//     retry handler de internal/api/internal.go): PROCESSING→RETRYING + Enqueue;
//     RETRYING solo Enqueue (RETRYING→PROCESSING pertenece al worker).
//   - PROCESSING/RETRYING sin el objeto → completar la vía de fallo de la SAGA:
//     →EXTRACTION_FAILED (OBJECT_MISSING) →COMPENSATING →FAILED.
//   - COMPENSATING → FAILED (COMPENSATION_FAILED).
//
// QUEUED queda fuera de este sweep: sweepStuckQueued ya lo reencola sin
// transicionar y acá solo se detecta (no se expande scope).
//
// Devuelve cuántos documentos trabados detectó y cuántos se reencolaron
// (planificados en dry-run).
func (r *Reconciler) sweepStuckIntermediate(ctx context.Context, now time.Time, dryRun bool, actions *actionReport, stuckAfter time.Duration) (int, int, error) {
	stuckCount := 0
	reenqueued := 0
	cutoff := now.Add(-stuckAfter)

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
			return stuckCount, reenqueued, fmt.Errorf("list %s: %w", status, err)
		}

		for _, doc := range docs {
			if !doc.UpdatedAt.Before(cutoff) {
				continue
			}
			stuckCount++
			slog.Warn("reconciler: documento en estado intermedio colgado",
				"document_id", doc.ID,
				"status", doc.Status,
				"updated_at", doc.UpdatedAt,
				"stuck_since", stuckAfter,
			)

			switch doc.Status {
			case domain.StatusQueued:
				// Fuera de scope (T3): sweepStuckQueued lo reencola sin transición.
				continue
			case domain.StatusCompensating:
				if err := r.failStuckCompensating(ctx, doc, dryRun, actions); err != nil {
					slog.Error("reconciler: error recuperando COMPENSATING",
						"document_id", doc.ID, "error", err)
				}
			case domain.StatusProcessing, domain.StatusRetrying:
				enqueued, err := r.recoverStuckExtraction(ctx, now, doc, dryRun, actions)
				if err != nil {
					slog.Error("reconciler: error recuperando estado intermedio",
						"document_id", doc.ID, "status", doc.Status, "error", err)
					continue
				}
				if enqueued {
					reenqueued++
				}
			}
		}
	}

	return stuckCount, reenqueued, nil
}

// recoverStuckExtraction resuelve un PROCESSING/RETRYING trabado: si el objeto
// raw existe se reencola; si no existe, se completa la vía de fallo de la SAGA.
// Devuelve true solo cuando se reencoló (o se planificó el encolado en dry-run).
func (r *Reconciler) recoverStuckExtraction(
	ctx context.Context, now time.Time, doc *domain.Document, dryRun bool, actions *actionReport,
) (bool, error) {
	exists, err := r.objectExists(ctx, doc.ObjectKey)
	if err != nil {
		return false, fmt.Errorf("verificar objeto: %w", err)
	}

	if !exists {
		return false, r.failMissingObject(ctx, doc, dryRun, actions)
	}

	// Sin queue no hay forma de reencolar: se deja el documento intacto para
	// que un sweep posterior (con queue configurado) lo recupere.
	if r.queue == nil {
		slog.Warn("reconciler: estado intermedio trabado sin queue configurado, no se reencola",
			"document_id", doc.ID, "status", doc.Status)
		return false, nil
	}

	// PROCESSING → RETRYING antes del XADD. RETRYING solo se reencola: la
	// transición a PROCESSING la hace el worker al reclamar el mensaje.
	if doc.Status == domain.StatusProcessing {
		ok, err := r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
			domain.StatusProcessing, domain.StatusRetrying,
			domain.StatusEntry{
				Status:        domain.StatusRetrying,
				Actor:         domain.ActorReconciler,
				CorrelationID: doc.CorrelationID,
			},
			nil,
		)
		if err != nil {
			return false, fmt.Errorf("transicionar a RETRYING: %w", err)
		}
		if !ok {
			return false, nil // el documento cambió de estado: no encolar
		}
	}

	msg := ports.WorkMessage{
		DocumentID:    doc.ID,
		ObjectKey:     doc.ObjectKey,
		CorrelationID: doc.CorrelationID,
		EnqueuedAt:    now.Format(time.RFC3339),
		SchemaVersion: doc.SchemaVersion,
	}
	if err := r.enqueue(ctx, actions, dryRun, msg); err != nil {
		return false, fmt.Errorf("reencolar: %w", err)
	}
	if !dryRun {
		slog.Info("reconciler: documento reencolado (estado intermedio trabado)",
			"document_id", doc.ID, "status", doc.Status, "object_key", doc.ObjectKey)
	}
	return true, nil
}

// failMissingObject completa la vía de fallo de la SAGA para un documento
// intermedio cuyo objeto raw no existe, con aristas declaradas: EXTRACTION_FAILED
// (OBJECT_MISSING) → COMPENSATING → FAILED. Es el mismo camino que tomaría un
// worker vivo ante un error de negocio (SPEC §4).
func (r *Reconciler) failMissingObject(ctx context.Context, doc *domain.Document, dryRun bool, actions *actionReport) error {
	steps := []struct {
		from   domain.Status
		to     domain.Status
		reason string
	}{
		{doc.Status, domain.StatusExtractionFailed, domain.FailureReasonObjectMissing},
		{domain.StatusExtractionFailed, domain.StatusCompensating, ""},
		{domain.StatusCompensating, domain.StatusFailed, domain.FailureReasonObjectMissing},
	}

	for _, step := range steps {
		ok, err := r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
			step.from, step.to,
			domain.StatusEntry{
				Status:        step.to,
				Actor:         domain.ActorReconciler,
				CorrelationID: doc.CorrelationID,
				Reason:        step.reason,
			},
			nil,
		)
		if err != nil {
			return fmt.Errorf("%s→%s: %w", step.from, step.to, err)
		}
		if !ok {
			// El documento cambió de estado: no forzar el resto de la cascada.
			return nil
		}
	}

	if !dryRun {
		slog.Info("reconciler: documento fallido (objeto ausente en estado intermedio)",
			"document_id", doc.ID, "failure_reason", domain.FailureReasonObjectMissing)
	}
	return nil
}

// failStuckCompensating cierra un COMPENSATING trabado: COMPENSATING→FAILED
// con COMPENSATION_FAILED (arista y constante existentes, SPEC §4).
func (r *Reconciler) failStuckCompensating(ctx context.Context, doc *domain.Document, dryRun bool, actions *actionReport) error {
	ok, err := r.updateStatusWithHistory(ctx, actions, dryRun, doc.ID,
		domain.StatusCompensating, domain.StatusFailed,
		domain.StatusEntry{
			Status:        domain.StatusFailed,
			Actor:         domain.ActorReconciler,
			CorrelationID: doc.CorrelationID,
			Reason:        domain.FailureReasonCompensateFail,
		},
		nil,
	)
	if err != nil {
		return err
	}
	if ok && !dryRun {
		slog.Info("reconciler: documento fallido (compensación trabada)",
			"document_id", doc.ID, "failure_reason", domain.FailureReasonCompensateFail)
	}
	return nil
}

// sweepMissingRetention fija retention_at en documentos terminales que no lo
// tienen. El worker en Rust cierra COMPLETED con un update condicional propio y
// no conoce el campo; sin esta red, esos documentos nunca serían recolectados
// por el TTL (crecimiento sin límite). Es idempotente y acotado por corrida.
func (r *Reconciler) sweepMissingRetention(ctx context.Context, now time.Time) (int, error) {
	docs, _, err := r.repo.List(ctx, ports.ListFilter{
		Status: domain.StatusCompleted,
		Limit:  200,
	})
	if err != nil {
		return 0, fmt.Errorf("list completed for retention: %w", err)
	}

	fixed := 0
	for _, doc := range docs {
		if !doc.RetentionAt.IsZero() {
			continue
		}
		ok, err := r.repo.UpdateStatus(ctx, doc.ID,
			domain.StatusCompleted, domain.StatusCompleted,
			map[string]any{"retention_at": now.Add(r.terminalRetention)},
		)
		if err != nil {
			slog.Warn("reconciler: no se pudo fijar retención terminal",
				"document_id", doc.ID, "error", err)
			continue
		}
		if ok {
			fixed++
		}
	}
	return fixed, nil
}

// sweepCompletedMissingMd detecta documentos COMPLETED cuyo .md no existe en MinIO (S5-P2-03).// Solo detecta y alerta: no transiciona automáticamente (eso es trabajo del operador).
// Respeta el umbral de seguridad.
func (r *Reconciler) sweepCompletedMissingMd(ctx context.Context, now time.Time) (int, error) {
	// S9: se limita la página a 50 HEADs por corrida. Con 200 por sweep el
	// barrido saturaba MinIO y consumía el presupuesto del breaker compartido,
	// devolviendo 500 en POST /api/v2/documents durante ~30 s.
	docs, _, err := r.repo.List(ctx, ports.ListFilter{
		Status: domain.StatusCompleted,
		Limit:  50,
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
		// Fallback legacy (D5): md_ref primero; si no existe, txt_ref (bucket legacy).
		ref, legacy := doc.ExtractedRef()
		if ref == "" {
			count++
			slog.Warn("reconciler: documento COMPLETED sin artefacto (ni md_ref ni txt_ref)",
				"document_id", doc.ID,
			)
			continue
		}
		// Verificar que el artefacto exista en MinIO (bucket según el tipo de ref).
		var err error
		if legacy {
			_, err = r.storage.StatLegacyTXT(ctx, ref)
		} else {
			_, err = r.storage.StatMD(ctx, ref)
		}
		if err != nil {
			if isNotFound(err) {
				count++
				slog.Warn("reconciler: documento COMPLETED pero el artefacto no existe en MinIO",
					"document_id", doc.ID,
					"ref", ref, "legacy", legacy,
				)
			} else {
				slog.Error("reconciler: error verificando artefacto",
					"document_id", doc.ID, "ref", ref, "legacy", legacy, "error", err)
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
	validMdKeys := make(map[string]bool)

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
		if doc.MdRef != "" {
			validMdKeys[doc.MdRef] = true
		}

		// FAILED sin failure_reason (S5-P2-02).
		if doc.Status == domain.StatusFailed && doc.FailureReason == "" {
			report.FailedNoReason = append(report.FailedNoReason, doc.ID)
		}

		// COMPLETED sin artefacto verificable (S5-P2-03, fallback legacy D5).
		if doc.Status == domain.StatusCompleted {
			ref, legacy := doc.ExtractedRef()
			if ref == "" {
				report.CompletedNoMd = append(report.CompletedNoMd, doc.ID)
			} else if now.Sub(doc.CreatedAt) >= r.minSafetyAge {
				// Solo verificar si pasó el umbral de seguridad.
				var err error
				if legacy {
					_, err = r.storage.StatLegacyTXT(ctx, ref)
				} else {
					_, err = r.storage.StatMD(ctx, ref)
				}
				if err != nil && isNotFound(err) {
					report.CompletedNoMd = append(report.CompletedNoMd, doc.ID)
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
		rawObjects := 0
		for _, obj := range objects {
			// Ignorar entradas de prefijo (common prefixes) del listado.
			if strings.HasSuffix(obj.Key, "/") {
				continue
			}
			rawObjects++
			if !validRawKeys[obj.Key] {
				report.OrphanRawObjects = append(report.OrphanRawObjects, obj.Key)
			}
		}
		report.RawBucketObjects = int64(rawObjects)
	}

	// 3b. Listar objetos del bucket extracted-md.
	type mdLister interface {
		ListObjectsMD(ctx context.Context) ([]ports.ObjectInfo, error)
	}
	if l, ok := r.storage.(mdLister); ok {
		objects, err := l.ListObjectsMD(ctx)
		if err != nil {
			return nil, fmt.Errorf("list md objects: %w", err)
		}
		mdObjects := 0
		for _, obj := range objects {
			if strings.HasSuffix(obj.Key, "/") {
				continue
			}
			mdObjects++
		}
		report.MDBucketObjects = int64(mdObjects)
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
		"completed_no_md", len(report.CompletedNoMd),
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
	var sdkErr minio.ErrorResponse
	if errors.As(err, &sdkErr) {
		// Sólo la ausencia del objeto. Un 404 con otro código (p. ej.
		// NoSuchBucket) es un error de infraestructura: tratarlo como
		// "objeto ausente" marcaría documentos válidos como UPLOAD_EXPIRED
		// o FAILED(object_missing) durante una mala configuración.
		if sdkErr.Code == "NoSuchKey" || sdkErr.Code == "NoSuchObject" {
			return true
		}
		// Fallback: algunos errores del SDK llegan con 404 y sin código.
		return sdkErr.Code == "" && sdkErr.StatusCode == http.StatusNotFound
	}
	msg := err.Error()
	return contains(msg, "not found") ||
		contains(msg, "NoSuchKey") ||
		contains(msg, "NoSuchObject") ||
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

// Package domain contiene las entidades puras del agregado Documento.
// Esta capa no importa ningún framework, driver ni paquete de infraestructura.
package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// Status representa el estado del ciclo de vida de un documento.
type Status string

// Estados del documento (SPEC §4).
const (
	StatusPendingUpload    Status = "PENDING_UPLOAD"
	StatusUploaded         Status = "UPLOADED"
	StatusQueued           Status = "QUEUED"
	StatusProcessing       Status = "PROCESSING"
	StatusRetrying         Status = "RETRYING"
	StatusExtractionFailed Status = "EXTRACTION_FAILED"
	StatusCompensating     Status = "COMPENSATING"
	StatusCompleted        Status = "COMPLETED"
	StatusFailed           Status = "FAILED"
	StatusRejected         Status = "REJECTED"
	StatusUploadExpired    Status = "UPLOAD_EXPIRED"
)

// Actor identifica quién ejecuta una transición de estado.
type Actor string

// Actores que pueden transicionar un documento.
const (
	ActorClient       Actor = "client"
	ActorMinIOWebhook Actor = "minio-webhook"
	ActorWorker       Actor = "worker"
	ActorReconciler   Actor = "reconciler"
	ActorDocService   Actor = "doc-service"
)

// ErrInvalidTransition se devuelve cuando la transición solicitada no está permitida.
var ErrInvalidTransition = errors.New("transición de estado no permitida")

// ErrTerminalState se devuelve cuando se intenta transicionar un estado terminal.
var ErrTerminalState = errors.New("el documento está en un estado terminal")

// IsTerminal indica si el estado es terminal (nada sale de él).
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusRejected, StatusUploadExpired:
		return true
	default:
		return false
	}
}

// IsValid valida que el estado pertenezca al dominio.
func (s Status) IsValid() bool {
	switch s {
	case StatusPendingUpload, StatusUploaded, StatusQueued, StatusProcessing,
		StatusRetrying, StatusExtractionFailed, StatusCompensating,
		StatusCompleted, StatusFailed, StatusRejected, StatusUploadExpired:
		return true
	default:
		return false
	}
}

// allowedTransitions define la matriz estricta de transiciones permitidas (SPEC §4, §11.1).
// Cualquier par (origen, destino) no declarado aquí se rechaza.
var allowedTransitions = map[Status][]Status{
	StatusPendingUpload:    {StatusUploaded, StatusUploadExpired},
	StatusUploaded:         {StatusQueued},
	StatusQueued:           {StatusProcessing},
	StatusProcessing:       {StatusCompleted, StatusRejected, StatusRetrying, StatusExtractionFailed},
	StatusRetrying:         {StatusProcessing, StatusExtractionFailed},
	StatusExtractionFailed: {StatusCompensating},
	StatusCompensating:     {StatusFailed},
}

// CanTransitionFrom indica si se permite pasar de `from` a `to`.
func CanTransitionFrom(from, to Status) bool {
	if from.IsTerminal() {
		return false
	}
	destinos, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	for _, d := range destino(destinos) {
		if d == to {
			return true
		}
	}
	return false
}

// destino es un helper mínimo para mantener la lectura de CanTransitionFrom clara.
func destino(lista []Status) []Status { return lista }

// TransitionsFor retorna los destinos permitidos desde un estado dado.
func TransitionsFor(from Status) []Status {
	if from.IsTerminal() {
		return nil
	}
	destinos := allowedTransitions[from]
	if destinos == nil {
		return nil
	}
	salida := make([]Status, len(destinos))
	copy(salida, destinos)
	return salida
}

// StatusEntry representa una entrada inmutable en el historial de estados.
type StatusEntry struct {
	Status        Status    `bson:"status" json:"status"`
	Actor         Actor     `bson:"actor" json:"actor"`
	CorrelationID string    `bson:"correlation_id" json:"correlation_id"`
	At            time.Time `bson:"at" json:"at"`
	Reason        string    `bson:"reason,omitempty" json:"reason,omitempty"`
}

// Document es el agregado raíz del dominio. Es puro: sin dependencias de infraestructura.
type Document struct {
	ID            string
	Status        Status
	ObjectKey     string
	TxtRef        string
	FailureReason string
	CorrelationID string
	SchemaVersion int
	ExpiresAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	History       []StatusEntry
}

// NewDocument crea un documento en estado inicial PENDING_UPLOAD.
func NewDocument(correlationID string, expiresAt time.Time) *Document {
	now := time.Now().UTC()
	return &Document{
		ID:            ulid.Make().String(),
		Status:        StatusPendingUpload,
		CorrelationID: correlationID,
		SchemaVersion: 1,
		ExpiresAt:     expiresAt,
		CreatedAt:     now,
		UpdatedAt:     now,
		History: []StatusEntry{
			{Status: StatusPendingUpload, Actor: ActorDocService, CorrelationID: correlationID, At: now},
		},
	}
}

// Transition aplica una transición de estado con filtro de origen (SPEC §4, §11.1).
// Devuelve ErrInvalidTransition si la transición no está declarada en la matriz.
func (d *Document) Transition(to Status, actor Actor, correlationID, reason string) error {
	if d.Status.IsTerminal() {
		return fmt.Errorf("%w: %s no puede salir de %s", ErrTerminalState, d.ID, d.Status)
	}
	if !to.IsValid() {
		return fmt.Errorf("%w: estado destino inválido %q", ErrInvalidTransition, to)
	}
	if !CanTransitionFrom(d.Status, to) {
		return fmt.Errorf("%w: %s → %s no está en la matriz", ErrInvalidTransition, d.Status, to)
	}

	now := time.Now().UTC()
	d.Status = to
	d.UpdatedAt = now
	if correlationID != "" {
		d.CorrelationID = correlationID
	}
	if reason != "" && (to == StatusFailed || to == StatusRejected || to == StatusExtractionFailed) {
		d.FailureReason = reason
	}
	d.History = append(d.History, StatusEntry{
		Status:        to,
		Actor:         actor,
		CorrelationID: d.CorrelationID,
		At:            now,
		Reason:        reason,
	})
	return nil
}

// SetObjectKey fija la clave del objeto en MinIO derivada del document_id.
func (d *Document) SetObjectKey(bucketPrefix string) {
	d.ObjectKey = fmt.Sprintf("%s/%s.pdf", bucketPrefix, d.ID)
}

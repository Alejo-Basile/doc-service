package domain

import (
	"errors"
	"testing"
	"time"
)

func TestNewDocument_InicializaEnPendingUpload(t *testing.T) {
	expires := time.Now().Add(30 * time.Minute)
	doc := NewDocument("corr-123", expires)

	if doc.ID == "" {
		t.Fatal("el ID no puede estar vacío")
	}
	if doc.Status != StatusPendingUpload {
		t.Errorf("status esperado PENDING_UPLOAD, got %q", doc.Status)
	}
	if doc.CorrelationID != "corr-123" {
		t.Errorf("correlation_id esperado corr-123, got %q", doc.CorrelationID)
	}
	if doc.SchemaVersion != 1 {
		t.Errorf("schema_version esperado 1, got %d", doc.SchemaVersion)
	}
	if doc.ExpiresAt != expires {
		t.Errorf("expires_at no coincide")
	}
	if len(doc.History) != 1 || doc.History[0].Status != StatusPendingUpload {
		t.Errorf("historial inicial incorrecto: %+v", doc.History)
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	terminales := map[Status]bool{
		StatusCompleted:        true,
		StatusFailed:           true,
		StatusRejected:         true,
		StatusUploadExpired:    true,
		StatusPendingUpload:    false,
		StatusUploaded:         false,
		StatusQueued:           false,
		StatusProcessing:       false,
		StatusRetrying:         false,
		StatusExtractionFailed: false,
		StatusCompensating:     false,
	}
	for status, esperado := range terminales {
		if got := status.IsTerminal(); got != esperado {
			t.Errorf("IsTerminal(%q) = %v, esperado %v", status, got, esperado)
		}
	}
}

func TestCanTransitionFrom_MatrizCompleta(t *testing.T) {
	casos := []struct {
		from, to Status
		esperado bool
	}{
		// Camino feliz
		{StatusPendingUpload, StatusUploaded, true},
		{StatusUploaded, StatusQueued, true},
		{StatusQueued, StatusProcessing, true},
		{StatusProcessing, StatusCompleted, true},
		// Rechazo (no es PDF)
		{StatusProcessing, StatusRejected, true},
		// Reintento
		{StatusProcessing, StatusRetrying, true},
		{StatusRetrying, StatusProcessing, true},
		// Fallo definitivo + compensación
		{StatusProcessing, StatusExtractionFailed, true},
		{StatusRetrying, StatusExtractionFailed, true},
		{StatusExtractionFailed, StatusCompensating, true},
		{StatusCompensating, StatusFailed, true},
		// Expiración de subida
		{StatusPendingUpload, StatusUploadExpired, true},
		// Transiciones prohibidas (saltos, retrocesos, ciclos)
		{StatusPendingUpload, StatusProcessing, false},
		{StatusPendingUpload, StatusCompleted, false},
		{StatusUploaded, StatusProcessing, false},
		{StatusUploaded, StatusCompleted, false},
		{StatusQueued, StatusCompleted, false},
		{StatusQueued, StatusUploaded, false},
		{StatusProcessing, StatusUploaded, false},
		{StatusProcessing, StatusQueued, false},
		{StatusCompleted, StatusProcessing, false},
		{StatusFailed, StatusProcessing, false},
		{StatusRejected, StatusProcessing, false},
		{StatusUploadExpired, StatusUploaded, false},
		{StatusCompensating, StatusProcessing, false},
		{StatusExtractionFailed, StatusProcessing, false},
		// Desde estado terminal nada se permite
		{StatusCompleted, StatusFailed, false},
		{StatusFailed, StatusCompleted, false},
		{StatusRejected, StatusFailed, false},
		{StatusUploadExpired, StatusCompleted, false},
	}

	for _, c := range casos {
		if got := CanTransitionFrom(c.from, c.to); got != c.esperado {
			t.Errorf("CanTransitionFrom(%s, %s) = %v, esperado %v", c.from, c.to, got, c.esperado)
		}
	}
}

func TestDocumentTransition_CaminoFeliz(t *testing.T) {
	doc := NewDocument("corr-1", time.Now().Add(30*time.Minute))
	pasos := []struct {
		to    Status
		actor Actor
	}{
		{StatusUploaded, ActorMinIOWebhook},
		{StatusQueued, ActorDocService},
		{StatusProcessing, ActorWorker},
		{StatusCompleted, ActorWorker},
	}

	for _, p := range pasos {
		if err := doc.Transition(p.to, p.actor, "corr-1", ""); err != nil {
			t.Fatalf("transición a %s falló: %v", p.to, err)
		}
		if doc.Status != p.to {
			t.Fatalf("status esperado %s, got %s", p.to, doc.Status)
		}
	}

	if !doc.Status.IsTerminal() {
		t.Error("COMPLETED debe ser terminal")
	}
	if len(doc.History) != 5 {
		t.Errorf("historial debería tener 5 entradas, got %d", len(doc.History))
	}
	// Verificar que cada entrada del historial tiene actor y timestamp
	for i, entry := range doc.History {
		if entry.At.IsZero() {
			t.Errorf("historial[%d] sin timestamp", i)
		}
		if entry.Actor == "" {
			t.Errorf("historial[%d] sin actor", i)
		}
	}
}

func TestDocumentTransition_RechazoNoEsPDF(t *testing.T) {
	doc := NewDocument("corr-2", time.Now().Add(30*time.Minute))
	_ = doc.Transition(StatusUploaded, ActorMinIOWebhook, "corr-2", "")
	_ = doc.Transition(StatusQueued, ActorDocService, "corr-2", "")
	_ = doc.Transition(StatusProcessing, ActorWorker, "corr-2", "")

	err := doc.Transition(StatusRejected, ActorWorker, "corr-2", "NOT_A_PDF")
	if err != nil {
		t.Fatalf("transición a REJECTED falló: %v", err)
	}
	if doc.Status != StatusRejected {
		t.Errorf("status esperado REJECTED, got %s", doc.Status)
	}
	if doc.FailureReason != "NOT_A_PDF" {
		t.Errorf("failure_reason esperado NOT_A_PDF, got %q", doc.FailureReason)
	}
	if !doc.Status.IsTerminal() {
		t.Error("REJECTED debe ser terminal")
	}
}

func TestDocumentTransition_ReintentoYFalloDefinitivo(t *testing.T) {
	doc := NewDocument("corr-3", time.Now().Add(30*time.Minute))
	_ = doc.Transition(StatusUploaded, ActorMinIOWebhook, "corr-3", "")
	_ = doc.Transition(StatusQueued, ActorDocService, "corr-3", "")
	_ = doc.Transition(StatusProcessing, ActorWorker, "corr-3", "")

	// Fallo transitorio → RETRYING
	if err := doc.Transition(StatusRetrying, ActorWorker, "corr-3", "timeout"); err != nil {
		t.Fatalf("a RETRYING falló: %v", err)
	}
	// Reintento → PROCESSING
	if err := doc.Transition(StatusProcessing, ActorWorker, "corr-3", ""); err != nil {
		t.Fatalf("de RETRYING a PROCESSING falló: %v", err)
	}
	// Reintentos agotados → EXTRACTION_FAILED
	if err := doc.Transition(StatusExtractionFailed, ActorWorker, "corr-3", "max_attempts"); err != nil {
		t.Fatalf("a EXTRACTION_FAILED falló: %v", err)
	}
	// Compensación → FAILED
	if err := doc.Transition(StatusCompensating, ActorDocService, "corr-3", ""); err != nil {
		t.Fatalf("a COMPENSATING falló: %v", err)
	}
	if err := doc.Transition(StatusFailed, ActorDocService, "corr-3", "compensation_done"); err != nil {
		t.Fatalf("a FAILED falló: %v", err)
	}

	if doc.Status != StatusFailed {
		t.Errorf("status esperado FAILED, got %s", doc.Status)
	}
	if doc.FailureReason != "compensation_done" {
		t.Errorf("failure_reason esperado compensation_done, got %q", doc.FailureReason)
	}
}

func TestDocumentTransition_ExpiracionDeSubida(t *testing.T) {
	doc := NewDocument("corr-4", time.Now().Add(-5*time.Minute))

	err := doc.Transition(StatusUploadExpired, ActorReconciler, "corr-4", "object_absent")
	if err != nil {
		t.Fatalf("transición a UPLOAD_EXPIRED falló: %v", err)
	}
	if !doc.Status.IsTerminal() {
		t.Error("UPLOAD_EXPIRED debe ser terminal")
	}
}

func TestDocumentTransition_TransicionInvalidaRechazada(t *testing.T) {
	doc := NewDocument("corr-5", time.Now().Add(30*time.Minute))

	// PENDING_UPLOAD → PROCESSING está prohibido (salto directo)
	err := doc.Transition(StatusProcessing, ActorWorker, "corr-5", "")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("esperado ErrInvalidTransition, got %v", err)
	}
	// El estado no debe haber cambiado
	if doc.Status != StatusPendingUpload {
		t.Errorf("el estado no debe cambiar ante transición inválida: got %s", doc.Status)
	}
}

func TestDocumentTransition_EstadoTerminalInmutable(t *testing.T) {
	doc := NewDocument("corr-6", time.Now().Add(30*time.Minute))
	_ = doc.Transition(StatusUploaded, ActorMinIOWebhook, "corr-6", "")
	_ = doc.Transition(StatusQueued, ActorDocService, "corr-6", "")
	_ = doc.Transition(StatusProcessing, ActorWorker, "corr-6", "")
	_ = doc.Transition(StatusCompleted, ActorWorker, "corr-6", "")

	err := doc.Transition(StatusProcessing, ActorWorker, "corr-6", "")
	if !errors.Is(err, ErrTerminalState) {
		t.Errorf("esperado ErrTerminalState, got %v", err)
	}
}

func TestDocumentTransition_EstadoDestinoInvalido(t *testing.T) {
	doc := NewDocument("corr-7", time.Now().Add(30*time.Minute))

	err := doc.Transition(Status("BOGUS"), ActorWorker, "corr-7", "")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("esperado ErrInvalidTransition para estado inválido, got %v", err)
	}
}

func TestTransitionsFor(t *testing.T) {
	casos := []struct {
		from     Status
		esperado []Status
	}{
		{StatusPendingUpload, []Status{StatusUploaded, StatusUploadExpired}},
		{StatusUploaded, []Status{StatusQueued}},
		{StatusQueued, []Status{StatusProcessing}},
		{StatusProcessing, []Status{StatusCompleted, StatusRejected, StatusRetrying, StatusExtractionFailed}},
		{StatusRetrying, []Status{StatusProcessing, StatusExtractionFailed}},
		{StatusExtractionFailed, []Status{StatusCompensating}},
		{StatusCompensating, []Status{StatusFailed}},
		{StatusCompleted, nil},
		{StatusFailed, nil},
	}

	for _, c := range casos {
		got := TransitionsFor(c.from)
		if len(got) != len(c.esperado) {
			t.Errorf("TransitionsFor(%s) = %v, esperado %v", c.from, got, c.esperado)
			continue
		}
		for i := range got {
			if got[i] != c.esperado[i] {
				t.Errorf("TransitionsFor(%s)[%d] = %s, esperado %s", c.from, i, got[i], c.esperado[i])
			}
		}
	}
}

func TestDocument_SetObjectKey(t *testing.T) {
	doc := NewDocument("corr-8", time.Now().Add(30*time.Minute))
	doc.SetObjectKey("raw-pdfs")

	esperado := "raw-pdfs/" + doc.ID + ".pdf"
	if doc.ObjectKey != esperado {
		t.Errorf("object_key esperado %q, got %q", esperado, doc.ObjectKey)
	}
}

func TestStatus_IsValid(t *testing.T) {
	validos := []Status{
		StatusPendingUpload, StatusUploaded, StatusQueued, StatusProcessing,
		StatusRetrying, StatusExtractionFailed, StatusCompensating,
		StatusCompleted, StatusFailed, StatusRejected, StatusUploadExpired,
	}
	for _, s := range validos {
		if !s.IsValid() {
			t.Errorf("IsValid(%q) = false, esperado true", s)
		}
	}
	if Status("NOPE").IsValid() {
		t.Error("IsValid(NOPE) debe ser false")
	}
}

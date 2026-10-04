package domain

import (
	"testing"
)

// TestTransitions_Exhaustivo verifica la tabla de transiciones para TODOS
// los pares (origen, destino) posibles (S2-P2-04).
//
// La tabla es la fuente de verdad: cualquier par no declarado se rechaza.
func TestTransitions_Exhaustivo(t *testing.T) {
	// Matriz esperada: allowedTransitions del dominio.
	// Para cada par (from, to), CanTransitionFrom debe retornar true solo si
	// el par está declarado en la matriz.
	matrizEsperada := map[Status][]Status{
		StatusPendingUpload:    {StatusUploaded, StatusUploadExpired},
		StatusUploaded:         {StatusQueued},
		StatusQueued:           {StatusProcessing},
		StatusProcessing:       {StatusCompleted, StatusRejected, StatusRetrying, StatusExtractionFailed},
		StatusRetrying:         {StatusProcessing, StatusExtractionFailed},
		StatusExtractionFailed: {StatusCompensating},
		StatusCompensating:     {StatusFailed},
		// Estados terminales: sin transiciones de salida.
		StatusCompleted:     nil,
		StatusFailed:        nil,
		StatusRejected:      nil,
		StatusUploadExpired: nil,
	}

	todosEstados := []Status{
		StatusPendingUpload,
		StatusUploaded,
		StatusQueued,
		StatusProcessing,
		StatusRetrying,
		StatusExtractionFailed,
		StatusCompensating,
		StatusCompleted,
		StatusFailed,
		StatusRejected,
		StatusUploadExpired,
	}

	for _, from := range todosEstados {
		for _, to := range todosEstados {
			esperado := false
			for _, d := range matrizEsperada[from] {
				if d == to {
					esperado = true
					break
				}
			}

			got := CanTransitionFrom(from, to)
			if got != esperado {
				t.Errorf("CanTransitionFrom(%s, %s) esperado %v, got %v",
					from, to, esperado, got)
			}
		}
	}
}

// TestTransitions_EstadosTerminales verifica que los estados terminales no
// tienen transiciones de salida.
func TestTransitions_EstadosTerminales(t *testing.T) {
	terminales := []Status{
		StatusCompleted,
		StatusFailed,
		StatusRejected,
		StatusUploadExpired,
	}

	todosEstados := []Status{
		StatusPendingUpload,
		StatusUploaded,
		StatusQueued,
		StatusProcessing,
		StatusRetrying,
		StatusExtractionFailed,
		StatusCompensating,
		StatusCompleted,
		StatusFailed,
		StatusRejected,
		StatusUploadExpired,
	}

	for _, term := range terminales {
		if !term.IsTerminal() {
			t.Errorf("%s debía ser terminal", term)
		}
		for _, to := range todosEstados {
			if CanTransitionFrom(term, to) {
				t.Errorf("estado terminal %s no debía transicionar a %s", term, to)
			}
		}
	}
}

// TestTransitions_FlujoFeliz verifica el camino feliz completo.
func TestTransitions_FlujoFeliz(t *testing.T) {
	flujo := []Status{
		StatusPendingUpload,
		StatusUploaded,
		StatusQueued,
		StatusProcessing,
		StatusCompleted,
	}

	for i := 0; i < len(flujo)-1; i++ {
		from, to := flujo[i], flujo[i+1]
		if !CanTransitionFrom(from, to) {
			t.Errorf("flujo feliz: %s → %s debía ser permitido", from, to)
		}
	}
}

// TestTransitions_FlujoRechazo verifica que REJECTED no pasa por RETRYING.
func TestTransitions_FlujoRechazo(t *testing.T) {
	// PROCESSING → REJECTED es directo (sin RETRYING).
	if !CanTransitionFrom(StatusProcessing, StatusRejected) {
		t.Error("PROCESSING → REJECTED debía ser permitido")
	}
	// REJECTED es terminal.
	if !StatusRejected.IsTerminal() {
		t.Error("REJECTED debía ser terminal")
	}
}

// TestTransitions_FlujoCompensacion verifica el flujo de compensación.
func TestTransitions_FlujoCompensacion(t *testing.T) {
	flujo := []Status{
		StatusProcessing,
		StatusExtractionFailed,
		StatusCompensating,
		StatusFailed,
	}

	for i := 0; i < len(flujo)-1; i++ {
		from, to := flujo[i], flujo[i+1]
		if !CanTransitionFrom(from, to) {
			t.Errorf("flujo compensación: %s → %s debía ser permitido", from, to)
		}
	}
}

// TestTransitions_Retry verifica el ciclo de reintentos.
func TestTransitions_Retry(t *testing.T) {
	// PROCESSING → RETRYING → PROCESSING.
	if !CanTransitionFrom(StatusProcessing, StatusRetrying) {
		t.Error("PROCESSING → RETRYING debía ser permitido")
	}
	if !CanTransitionFrom(StatusRetrying, StatusProcessing) {
		t.Error("RETRYING → PROCESSING debía ser permitido")
	}
	// RETRYING → EXTRACTION_FAILED (reintentos agotados).
	if !CanTransitionFrom(StatusRetrying, StatusExtractionFailed) {
		t.Error("RETRYING → EXTRACTION_FAILED debía ser permitido")
	}
}

// TestTransitions_UploadExpired verifica la expiración de subida.
func TestTransitions_UploadExpired(t *testing.T) {
	if !CanTransitionFrom(StatusPendingUpload, StatusUploadExpired) {
		t.Error("PENDING_UPLOAD → UPLOAD_EXPIRED debía ser permitido")
	}
	if !StatusUploadExpired.IsTerminal() {
		t.Error("UPLOAD_EXPIRED debía ser terminal")
	}
}

// TestTransitionsFor_ReturnsDestinos verifica que TransitionsFor retorna los destinos correctos.
func TestTransitionsFor_ReturnsDestinos(t *testing.T) {
	casos := []struct {
		from     Status
		esperado []Status
	}{
		{StatusPendingUpload, []Status{StatusUploaded, StatusUploadExpired}},
		{StatusUploaded, []Status{StatusQueued}},
		{StatusProcessing, []Status{StatusCompleted, StatusRejected, StatusRetrying, StatusExtractionFailed}},
		{StatusCompleted, nil},
		{StatusFailed, nil},
	}

	for _, c := range casos {
		got := TransitionsFor(c.from)
		if len(got) != len(c.esperado) {
			t.Errorf("TransitionsFor(%s) longitud esperada %d, got %d",
				c.from, len(c.esperado), len(got))
			continue
		}
		for i, e := range c.esperado {
			if got[i] != e {
				t.Errorf("TransitionsFor(%s)[%d] esperado %s, got %s",
					c.from, i, e, got[i])
			}
		}
	}
}

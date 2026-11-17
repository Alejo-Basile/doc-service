package resilience

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// transientErr simula un error de dependencia (timeout, red, 5xx): sí
// aporta señal de salud y debe contar para el breaker.
var transientErr = errors.New("fallo transitorio de dependencia")

// tripAtTres devuelve Settings de test con umbral de disparo en 3 fallos
// consecutivos y timeout de Open suficientemente largo para no pasar a
// HalfOpen durante el test.
func tripAtTres() gobreaker.Settings {
	return gobreaker.Settings{
		Name: "test",
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= 3
		},
		Timeout: 60 * time.Second,
		// Igual que en producción (defaultSettings): los sentinelas de
		// negocio y context.Canceled no cuentan para el breaker.
		IsExcluded: excludedClassification(),
	}
}

func TestBreaker_TripsAndRejectsWithoutTouchingInner(t *testing.T) {
	var calls int
	b := NewBreakerWithSettings(tripAtTres())

	for i := 1; i <= 5; i++ {
		err := b.Do(func() error {
			calls++
			return transientErr
		})
		if i <= 3 {
			if err == nil {
				t.Fatalf("llamada %d: se esperaba el error transitorio", i)
			}
			if !errors.Is(err, transientErr) {
				t.Fatalf("llamada %d: error inesperado: %v", i, err)
			}
			if IsRejected(err) {
				t.Fatalf("llamada %d: el error transitorio no debe marcarse como rechazo", i)
			}
		} else {
			if !IsRejected(err) {
				t.Fatalf("llamada %d: se esperaba rechazo del breaker (state=%v), got: %v", i, b.cb.State(), err)
			}
			if errors.Is(err, transientErr) {
				t.Fatalf("llamada %d: el rechazo no debe envolver el error transitorio: %v", i, err)
			}
		}
	}

	if calls != 3 {
		t.Errorf("el inner debe ser invocado exactamente 3 veces (sin red tras el trip), got: %d", calls)
	}
	if got := b.cb.State(); got != gobreaker.StateOpen {
		t.Errorf("estado tras el trip debe ser Open, got: %v", got)
	}
}

func TestBreaker_BusinessErrorsAndCtxCancelDoNotTrip(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "ports.ErrNotFound", err: ports.ErrNotFound, wantErr: ports.ErrNotFound},
		{name: "ports.ErrDuplicate", err: ports.ErrDuplicate, wantErr: ports.ErrDuplicate},
		{name: "ports.ErrInvalidCursor", err: ports.ErrInvalidCursor, wantErr: ports.ErrInvalidCursor},
		{name: "context.Canceled", err: context.Canceled, wantErr: context.Canceled},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			// NewBreaker (no WithSettings): debe llevar la clasificación por defecto.
			b := NewBreaker("breaker-sin-trip")

			const attempts = 20
			for i := 0; i < attempts; i++ {
				err := b.Do(func() error {
					calls++
					return tc.err
				})
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("llamada %d: error debe ser %v, got: %v", i+1, tc.wantErr, err)
				}
				if IsRejected(err) {
					t.Fatalf("llamada %d: %v no debe ser rechazado por el breaker", i+1, tc.err)
				}
			}

			if calls != attempts {
				t.Errorf("el inner debe recibir todas las llamadas (sin trip), got: %d want: %d", calls, attempts)
			}
			if got := b.cb.State(); got != gobreaker.StateClosed {
				t.Errorf("estado debe permanecer Closed, got: %v", got)
			}
			if b.cb.Counts().ConsecutiveFailures != 0 {
				t.Errorf("los errores excluidos no deben contar como fallos, got ConsecutiveFailures=%d",
					b.cb.Counts().ConsecutiveFailures)
			}
		})
	}
}

func TestBreaker_DeadlineExceededCountsAsFailure(t *testing.T) {
	var calls int
	b := NewBreakerWithSettings(tripAtTres())

	for i := 1; i <= 3; i++ {
		err := b.Do(func() error {
			calls++
			return context.DeadlineExceeded
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("llamada %d: error debe ser context.DeadlineExceeded, got: %v", i, err)
		}
	}

	if calls != 3 {
		t.Errorf("el inner debe recibir 3 llamadas, got: %d", calls)
	}
	if got := b.cb.State(); got != gobreaker.StateOpen {
		t.Errorf("context.DeadlineExceeded debe trip el breaker, estado: %v", got)
	}
}

func TestBreaker_ExcludedErrorDoesNotResetConsecutiveFailures(t *testing.T) {
	// Un error excluido interleavado entre fallos transitorios no debe
	// resetear el contador de fallos consecutivos.
	var calls int
	b := NewBreakerWithSettings(tripAtTres())

	// 2 fallos transitorios (ConsecutiveFailures=2), 1 excluido (no toca el
	// contador), 1 fallo transitorio → trip en 3.
	sequence := []error{transientErr, transientErr, ports.ErrNotFound, transientErr}
	for i, want := range sequence {
		err := b.Do(func() error {
			calls++
			return want
		})
		if !errors.Is(err, want) {
			t.Fatalf("llamada %d: error debe ser %v, got: %v", i+1, want, err)
		}
	}

	if calls != len(sequence) {
		t.Errorf("el inner debe recibir %d llamadas, got: %d", len(sequence), calls)
	}
	if got := b.cb.State(); got != gobreaker.StateOpen {
		t.Errorf("el excluido no debe impedir el trip por 3 fallos consecutivos, estado: %v", got)
	}
}

func TestBreaker_SuccessResetsConsecutiveFailures(t *testing.T) {
	var calls int
	b := NewBreakerWithSettings(tripAtTres())

	// 2 fallos, 1 éxito (reset), 2 fallos: no debe trip (nunca 3 consecutivos).
	sequence := []error{transientErr, transientErr, nil, transientErr, transientErr}
	for i, want := range sequence {
		err := b.Do(func() error {
			calls++
			return want
		})
		if want != nil && !errors.Is(err, want) {
			t.Fatalf("llamada %d: error debe ser %v, got: %v", i+1, want, err)
		}
		if want == nil && err != nil {
			t.Fatalf("llamada %d: no se esperaba error, got: %v", i+1, err)
		}
	}

	if calls != len(sequence) {
		t.Errorf("el inner debe recibir %d llamadas, got: %d", len(sequence), calls)
	}
	if got := b.cb.State(); got != gobreaker.StateClosed {
		t.Errorf("con éxitos intercalados no debe trip, estado: %v", got)
	}
}

func TestIsRejected(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "ErrOpenState", err: gobreaker.ErrOpenState, want: true},
		{name: "ErrTooManyRequests", err: gobreaker.ErrTooManyRequests, want: true},
		{name: "ErrOpenState envuelto con %w", err: fmt.Errorf("mongo GetByID: %w", gobreaker.ErrOpenState), want: true},
		{name: "ErrTooManyRequests envuelto con %w", err: fmt.Errorf("redis Enqueue: %w", gobreaker.ErrTooManyRequests), want: true},
		{name: "error ordinario", err: errors.New("boom"), want: false},
		{name: "error de negocio", err: ports.ErrNotFound, want: false},
		{name: "timeout de contexto", err: context.DeadlineExceeded, want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRejected(tc.err); got != tc.want {
				t.Errorf("IsRejected(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

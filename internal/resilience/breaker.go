// Package resilience implementa el patrón circuit breaker (SPEC §7.2) sobre
// los puertos de la aplicación: un breaker por dependencia (MinIO, Mongo,
// Redis) y decoradores de puertos que enrutan cada llamada por su breaker,
// sin tocar los adaptadores concretos ni a los consumidores.
package resilience

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// defaultOpenTimeout es la duración del estado Open antes de pasar a
// HalfOpen (SPEC §7.2).
const defaultOpenTimeout = 30 * time.Second

// defaultTripThreshold es la cantidad de fallos consecutivos que dispara
// el estado Open (SPEC §7.2: ReadyToTrip por fallos consecutivos).
const defaultTripThreshold = 5

// Breaker es un envoltorio liviano sobre gobreaker para operaciones que
// solo devuelven error. Un Breaker por dependencia: comparte el estado
// (Closed/Open/HalfOpen) entre todos los consumidores de esa dependencia.
type Breaker struct {
	cb *gobreaker.CircuitBreaker[struct{}]
}

// excludedClassification devuelve IsExcluded para los Settings: los errores
// que no aportan señal de salud de la dependencia (sentinelas de negocio y
// cancelación del cliente) no cuentan ni como éxito ni como fallo
// (gobreaker v2: TotalExclusions, no toca ConsecutiveFailures).
// context.DeadlineExceeded SÍ cuenta como fallo: es señal real de
// dependencia lenta o saturada.
func excludedClassification() func(err error) bool {
	return func(err error) bool {
		return errors.Is(err, context.Canceled) ||
			errors.Is(err, ports.ErrNotFound) ||
			errors.Is(err, ports.ErrDuplicate) ||
			errors.Is(err, ports.ErrInvalidCursor)
	}
}

// defaultSettings devuelve los Settings de SPEC §7.2 con la clasificación
// de errores excluidos y el log de transiciones de estado.
func defaultSettings(name string) gobreaker.Settings {
	return gobreaker.Settings{
		Name: name,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > defaultTripThreshold
		},
		Timeout: defaultOpenTimeout,
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("circuit breaker cambió de estado",
				"breaker", name,
				"from", from.String(),
				"to", to.String(),
			)
		},
		IsExcluded: excludedClassification(),
	}
}

// NewBreaker crea el breaker estándar para una dependencia con los
// parámetros de SPEC §7.2: trip a los >5 fallos consecutivos, 30s de Open
// antes de HalfOpen, errores de negocio/cancelación excluidos del conteo y
// transiciones de estado logueadas en Warn.
func NewBreaker(name string) *Breaker {
	return NewBreakerWithSettings(defaultSettings(name))
}

// NewBreakerWithSettings crea un breaker con Settings custom (tests).
func NewBreakerWithSettings(st gobreaker.Settings) *Breaker {
	return &Breaker{cb: gobreaker.NewCircuitBreaker[struct{}](st)}
}

// Do ejecuta fn a través del breaker. Si el breaker está Open (o HalfOpen
// sin cupo), devuelve el error de rechazo sin invocar fn: los errores de
// rechazo satisfacen IsRejected.
func (b *Breaker) Do(fn func() error) error {
	_, err := b.cb.Execute(func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

// IsRejected informa si err es un rechazo fast-fail del breaker
// (Open o HalfOpen sin cupo) y no un error real de la operación.
func IsRejected(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) ||
		errors.Is(err, gobreaker.ErrTooManyRequests)
}

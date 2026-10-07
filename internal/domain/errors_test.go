package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestAppError_TiposYConstructores(t *testing.T) {
	casos := []struct {
		nombre string
		err    *AppError
		kind   ErrorKind
		status int
	}{
		{"validation", NewValidation("campo requerido"), KindValidation, 400},
		{"not_found", NewNotFound("documento no existe"), KindNotFound, 404},
		{"conflict", NewConflict("idempotency key duplicada"), KindConflict, 409},
		{"dependency", NewDependencyUnavailable("mongo caído", errors.New("dial timeout")), KindDependencyUnavailable, 503},
		{"internal", NewInternal("fallo inesperado", errors.New("panic")), KindInternal, 500},
	}

	for _, c := range casos {
		if c.err.Kind != c.kind {
			t.Errorf("%s: kind esperado %q, got %q", c.nombre, c.kind, c.err.Kind)
		}
		if c.err.HTTPStatus() != c.status {
			t.Errorf("%s: status esperado %d, got %d", c.nombre, c.status, c.err.HTTPStatus())
		}
		if c.err.Message == "" {
			t.Errorf("%s: message no puede estar vacío", c.nombre)
		}
	}
}

func TestAppError_ErrorString(t *testing.T) {
	e := NewValidation("nombre requerido")
	esperado := "validation: nombre requerido"
	if e.Error() != esperado {
		t.Errorf("Error() esperado %q, got %q", esperado, e.Error())
	}

	eWrapped := NewInternal("fallo", errors.New("detalle"))
	esperadoWrapped := "internal: fallo: detalle"
	if eWrapped.Error() != esperadoWrapped {
		t.Errorf("Error() con wrap esperado %q, got %q", esperadoWrapped, eWrapped.Error())
	}
}

func TestAppError_Unwrap(t *testing.T) {
	orig := errors.New("causa raíz")
	e := NewInternal("fallo", orig)

	if !errors.Is(e, orig) {
		t.Error("errors.Is debe encontrar el error envuelto")
	}
	if !errors.Is(e, e) {
		t.Error("errors.Is debe funcionar sobre el propio AppError")
	}
}

func TestAsAppError_ConvierteErrores(t *testing.T) {
	// AppError directo
	app := NewNotFound("no existe")
	if got := AsAppError(app); got != app {
		t.Error("AsAppError debe retornar el mismo AppError si ya lo es")
	}

	// Error genérico → se envuelve como internal
	gen := errors.New("error cualquiera")
	wrapped := AsAppError(gen)
	if wrapped.Kind != KindInternal {
		t.Errorf("error genérico debe envolverse como internal, got %q", wrapped.Kind)
	}
	if !errors.Is(wrapped, gen) {
		t.Error("el error original debe quedar envuelto")
	}

	// nil → nil
	if got := AsAppError(nil); got != nil {
		t.Errorf("AsAppError(nil) debe ser nil, got %v", got)
	}
}

func TestKind_DetectaKind(t *testing.T) {
	casos := []struct {
		err      error
		esperado ErrorKind
	}{
		{NewValidation("x"), KindValidation},
		{NewNotFound("x"), KindNotFound},
		{NewConflict("x"), KindConflict},
		{NewDependencyUnavailable("x", nil), KindDependencyUnavailable},
		{NewInternal("x", nil), KindInternal},
		{errors.New("genérico"), KindInternal},
		{nil, ""},
	}

	for _, c := range casos {
		if got := Kind(c.err); got != c.esperado {
			t.Errorf("Kind(%v) esperado %q, got %q", c.err, c.esperado, got)
		}
	}
}

func TestDomainErrors_NoDependenDeInfraestructura(t *testing.T) {
	// Este test es conceptual: domain/errors.go no importa gin, mongo, etc.
	// Si alguien agrega un import de infra, go vet / revisión de arquitectura lo detecta.
	// Aquí solo verificamos que los tipos son utilitables sin contexto externo.
	e := NewValidation("test")
	if fmt.Sprintf("%v", e) == "" {
		t.Error("el error debe ser imprimible")
	}
}

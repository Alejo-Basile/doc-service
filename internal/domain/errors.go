// Package domain contiene las entidades puras del agregado Documento.
// Esta capa no importa ningún framework, driver ni paquete de infraestructura.
package domain

import (
	"errors"
	"fmt"
)

// ErrorKind clasifica los errores del dominio según su naturaleza (SPEC §11.2).
type ErrorKind string

// Kinds de error del dominio.
const (
	KindValidation            ErrorKind = "validation"
	KindNotFound              ErrorKind = "not_found"
	KindConflict              ErrorKind = "conflict"
	KindDependencyUnavailable ErrorKind = "dependency_unavailable"
	KindInternal              ErrorKind = "internal"
)

// AppError es el tipo de error estándar del dominio.
// Transporta un kind clasificable que la capa HTTP mapea a códigos de respuesta.
type AppError struct {
	Kind    ErrorKind
	Message string
	Err     error // error original envuelto (opcional)
}

// Error implementa la interfaz error.
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %s", e.Kind, e.Message, e.Err.Error())
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

// Unwrap permite errors.Is/errors.As sobre el error envuelto.
func (e *AppError) Unwrap() error { return e.Err }

// HTTPStatus retorna el código HTTP asociado al kind de error.
func (e *AppError) HTTPStatus() int {
	switch e.Kind {
	case KindValidation:
		return 400
	case KindNotFound:
		return 404
	case KindConflict:
		return 409
	case KindDependencyUnavailable:
		return 503
	case KindInternal:
		return 500
	default:
		return 500
	}
}

// --- constructores de errores ---

// NewValidation crea un error de validación (400).
func NewValidation(message string) *AppError {
	return &AppError{Kind: KindValidation, Message: message}
}

// NewNotFound crea un error de recurso no encontrado (404).
func NewNotFound(message string) *AppError {
	return &AppError{Kind: KindNotFound, Message: message}
}

// NewConflict crea un error de conflicto (409).
func NewConflict(message string) *AppError {
	return &AppError{Kind: KindConflict, Message: message}
}

// NewDependencyUnavailable crea un error de dependencia caída (503).
func NewDependencyUnavailable(message string, err error) *AppError {
	return &AppError{Kind: KindDependencyUnavailable, Message: message, Err: err}
}

// NewInternal crea un error interno (500).
func NewInternal(message string, err error) *AppError {
	return &AppError{Kind: KindInternal, Message: message, Err: err}
}

// AsAppError intenta convertir un error a *AppError.
// Si no lo es, lo envuelve como *AppError de KindInternal.
func AsAppError(err error) *AppError {
	if err == nil {
		return nil
	}
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr
	}
	return NewInternal("error interno", err)
}

// Kind retorna el kind del error, o KindInternal si no es un AppError.
func Kind(err error) ErrorKind {
	if err == nil {
		return ""
	}
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Kind
	}
	return KindInternal
}

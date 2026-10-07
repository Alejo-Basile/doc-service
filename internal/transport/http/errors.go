package httpserver

import (
	"net/http"

	"github.com/Alejo-Basile/doc-service/internal/domain"
	"github.com/gin-gonic/gin"
)

// problemResponse es el formato de error RFC 9457 (application/problem+json),
// alineado con SPEC §11.2.
type problemResponse struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail,omitempty"`
	Instance      string `json:"instance,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// errorTitles asigna un título estándar a cada kind de error.
var errorTitles = map[domain.ErrorKind]string{
	domain.KindValidation:            "Error de validación",
	domain.KindNotFound:              "Recurso no encontrado",
	domain.KindConflict:              "Conflicto",
	domain.KindDependencyUnavailable: "Dependencia no disponible",
	domain.KindInternal:              "Error interno del servidor",
}

// respondProblem envía una respuesta de error en formato problem+json.
func respondProblem(c *gin.Context, err error) {
	appErr := domain.AsAppError(err)
	status := appErr.HTTPStatus()

	title, ok := errorTitles[appErr.Kind]
	if !ok {
		title = errorTitles[domain.KindInternal]
	}

	// Correlation ID si ya está en el contexto (S0-P2-03 lo setea).
	corrID, _ := c.Get("correlation_id")
	corrStr, _ := corrID.(string)

	c.AbortWithStatusJSON(status, problemResponse{
		Type:          "https://api.doc-service/errors/" + string(appErr.Kind),
		Title:         title,
		Status:        status,
		Detail:        appErr.Message,
		Instance:      c.Request.URL.Path,
		CorrelationID: corrStr,
	})
}

// ValidationErrorHandler aborta con un error de validación (400).
func ValidationErrorHandler(c *gin.Context, message string) {
	respondProblem(c, domain.NewValidation(message))
}

// NotFoundErrorHandler aborta con un error de no encontrado (404).
func NotFoundErrorHandler(c *gin.Context, message string) {
	respondProblem(c, domain.NewNotFound(message))
}

// ConflictErrorHandler aborta con un error de conflicto (409).
func ConflictErrorHandler(c *gin.Context, message string) {
	respondProblem(c, domain.NewConflict(message))
}

// InternalErrorHandler aborta con un error interno (500).
func InternalErrorHandler(c *gin.Context, message string, err error) {
	respondProblem(c, domain.NewInternal(message, err))
}

// DependencyErrorHandler aborta con dependencia caída (503).
func DependencyErrorHandler(c *gin.Context, message string, err error) {
	respondProblem(c, domain.NewDependencyUnavailable(message, err))
}

// statusForKind expone el mapeo kind→HTTP para tests del paquete.
func statusForKind(kind domain.ErrorKind) int {
	switch kind {
	case domain.KindValidation:
		return http.StatusBadRequest
	case domain.KindNotFound:
		return http.StatusNotFound
	case domain.KindConflict:
		return http.StatusConflict
	case domain.KindDependencyUnavailable:
		return http.StatusServiceUnavailable
	case domain.KindInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

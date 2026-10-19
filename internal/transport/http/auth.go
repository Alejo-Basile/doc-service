// Package httpserver implementa el transporte HTTP con Gin.
package httpserver

import (
	"crypto/subtle"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// InternalTokenHeader es el header con el token compartido de los endpoints
// internos (SPEC §9). Se acepta también "Authorization: Bearer <token>".
const InternalTokenHeader = "X-Internal-Token"

// InternalTokenMiddleware protege endpoints internos con un token compartido.
// Falla cerrado: si el token configurado está vacío, rechaza todo.
// La comparación es en tiempo constante para no filtrar el secreto por timing.
func InternalTokenMiddleware(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		got := c.GetHeader(InternalTokenHeader)
		if got == "" {
			if auth := c.GetHeader("Authorization"); len(auth) > 7 && auth[:7] == "Bearer " {
				got = auth[7:]
			}
		}

		if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			slog.Warn("internal: token inválido o ausente", "path", c.Request.URL.Path)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			return
		}
		c.Next()
	}
}

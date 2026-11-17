// Package httpserver implementa el transporte HTTP con Gin.
package httpserver

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// rateLimitHeadersWhitelist son los únicos headers de rate-limit que se
// propagan desde el request entrante (inyectados por Traefik forwardAuth)
// hacia la respuesta del backend.
//
// Why: Traefik forwardAuth, ante una respuesta 2xx del auth server, copia los
// headers listados en authResponseHeaders sobre el REQUEST reenviado al backend,
// NO sobre la respuesta al cliente. Por eso el backend debe ecoarlos para que
// lleguen al cliente. El spoofing se previene porque Traefik (via
// authResponseHeaders) elimina/reemplaza del request del cliente estos mismos
// nombres antes de reenviarlo.
var rateLimitHeadersWhitelist = []string{
	"X-RateLimit-Limit",
	"X-RateLimit-Remaining",
	"X-RateLimit-Reset",
	"X-RateLimit-Retry-After",
	"X-RateLimit-Degraded",
	"Retry-After",
}

// RateLimitHeadersMiddleware ecoa la whitelist de headers de rate-limit desde
// el request entrante hacia la respuesta. Solo setea el header de respuesta si
// el inbound es no-vacío. No inventa valores.
func RateLimitHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		for _, name := range rateLimitHeadersWhitelist {
			if v := c.GetHeader(name); v != "" {
				c.Header(name, v)
			}
		}
		c.Next()
	}
}

// correlationHeader es el header HTTP estándar para correlation IDs.
// Se acepta inbound (para seguir trazas entre servicios) o se genera uno nuevo.
const correlationHeader = "X-Correlation-ID"

// CorrelationIDMiddleware inyecta un correlation ID en el contexto de cada
// request. Si el caller envía X-Correlation-ID, se respeta; si no, se genera
// uno nuevo (UUID v4). El ID queda disponible en:
//   - c.GetString("correlation_id") para handlers
//   - ctx.Value para middlewares posteriores
func CorrelationIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		corrID := c.GetHeader(correlationHeader)
		if corrID == "" {
			corrID = uuid.NewString()
		}

		c.Set("correlation_id", corrID)
		c.Header(correlationHeader, corrID)
		c.Next()
	}
}

// RequestLoggerMiddleware emite un log estructurado al terminar cada request,
// incluyendo correlation_id, método, ruta, status, latencia y client IP.
// Usa slog del logger global para centralizar salida.
func RequestLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		corrID, _ := c.Get("correlation_id")

		// Procesar la request (sigue la cadena de middlewares + handler).
		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		// Nivel según status: 5xx → Error, 4xx → Warn, 2xx/3xx → Info.
		level := slog.LevelInfo
		switch {
		case status >= 500:
			level = slog.LevelError
		case status >= 400:
			level = slog.LevelWarn
		}

		slog.Log(c.Request.Context(), level, "http_request",
			"correlation_id", corrID,
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", status,
			"latency_ms", latency.Milliseconds(),
			"client_ip", c.ClientIP(),
			"user_agent", c.Request.UserAgent(),
		)
	}
}

// ErrorLogMiddleware captura panics no manejados y los registra con
// correlation_id antes de responder 500. (Gin.Recovery hace el abort;
// este middleware solo agrega contexto al log.)
func ErrorLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if len(c.Errors) > 0 {
			corrID, _ := c.Get("correlation_id")
			for _, e := range c.Errors {
				slog.Error("request_error",
					"correlation_id", corrID,
					"error", e.Error(),
					"type", int(e.Type),
					"method", c.Request.Method,
					"path", c.Request.URL.Path,
				)
			}
		}
	}
}

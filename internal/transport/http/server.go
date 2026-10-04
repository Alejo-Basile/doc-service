// Package httpserver implementa el transporte HTTP con Gin.
// Solo contiene controladores y configuración de rutas: sin lógica de negocio.
package httpserver

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Server encapsula el motor Gin y sus rutas.
type Server struct {
	engine *gin.Engine
}

// NewServer crea un servidor HTTP con el modo de Gin según el entorno.
func NewServer(debug bool) *Server {
	if !debug {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())

	s := &Server{engine: engine}
	s.registerRoutes()
	return s
}

// Engine retorna el motor Gin (útil para tests con httptest).
func (s *Server) Engine() *gin.Engine { return s.engine }

// registerRoutes registra todas las rutas del servicio.
func (s *Server) registerRoutes() {
	// Health checks (SPEC §10: /healthz y /readyz deben verificar dependencias reales,
	// pero en esta fase solo responden proceso vivo).
	s.engine.GET("/healthz", handleHealthz)
	s.engine.GET("/readyz", handleReadyz)

	// Rutas de la API v2 se registran en fases posteriores:
	// s.engine.Group("/api/v2/documents", ...)
	// Rutas internas (webhook MinIO, etc.):
	// s.engine.Group("/internal", ...)
}

// Run inicia el servidor HTTP.
func (s *Server) Run(addr string) error {
	return s.engine.Run(addr)
}

// Handler retorna el http.Handler del servidor (para graceful shutdown).
func (s *Server) Handler() http.Handler { return s.engine }

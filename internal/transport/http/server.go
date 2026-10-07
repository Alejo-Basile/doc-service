// Package httpserver implementa el transporte HTTP con Gin.
// Solo contiene controladores y configuración de rutas: sin lógica de negocio.
package httpserver

import (
	"net/http"
	"time"

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

// RouteRegistrar es cualquier handler capaz de registrar sus rutas
// sobre el motor Gin (DocumentHandler, WebhookHandler, etc.).
type RouteRegistrar interface {
	RegisterRoutes(engine *gin.Engine)
}

// RegisterAPI registra las rutas de la API y los endpoints internos.
// Debe invocarse después de NewServer y antes de NewHTTPServer, con los
// handlers ya construidos por inyección de dependencias en main.
func (s *Server) RegisterAPI(registrars ...RouteRegistrar) {
	for _, r := range registrars {
		r.RegisterRoutes(s.engine)
	}
}

// registerRoutes registra todas las rutas del servicio.
func (s *Server) registerRoutes() {
	// Middlewares globales: correlation_id → request logging → error logging → recovery.
	s.engine.Use(
		CorrelationIDMiddleware(),
		RequestLoggerMiddleware(),
		ErrorLogMiddleware(),
		gin.Recovery(),
	)

	// Health checks (SPEC §10: /healthz y /readyz deben verificar dependencias reales,
	// pero en esta fase solo responden proceso vivo).
	s.engine.GET("/healthz", handleHealthz)
	s.engine.GET("/readyz", handleReadyz)

	// Rutas de la API v2 y endpoints internos se registran vía RegisterAPI
	// (inyección de dependencias desde cmd/api/main.go).
}

// NewHTTPServer crea un *http.Server configurado para graceful shutdown.
// Los timeouts evitan que conexiones colgadas bloqueen el drenaje.
func (s *Server) NewHTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           s.engine,
		ReadHeaderTimeout: 5 * time.Second,
		// No se fija WriteTimeout para no cortar requests largas en vuelo;
		// el Shutdown(ctx) con timeout es el que limita el drenaje.
	}
}

// Handler retorna el http.Handler del servidor (para tests o montaje externo).
func (s *Server) Handler() http.Handler { return s.engine }

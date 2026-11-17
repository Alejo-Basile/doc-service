// Package httpserver implementa el transporte HTTP con Gin.
// Solo contiene controladores y configuración de rutas: sin lógica de negocio.
package httpserver

import (
	"net/http"
	"time"

	_ "github.com/Alejo-Basile/doc-service/internal/metrics"
	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server encapsula el motor Gin y sus rutas.
type Server struct {
	engine *gin.Engine
	probes []ports.HealthProbe
}

// NewServer creates an HTTP server with Gin's mode based on the environment.
// probes feeds /readyz: when none is injected, only process liveness is
// verified (compatibility with starts/tests that do not wire dependencies).
func NewServer(debug bool, probes ...ports.HealthProbe) *Server {
	if !debug {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())

	s := &Server{engine: engine, probes: probes}
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
		RateLimitHeadersMiddleware(),
		CorrelationIDMiddleware(),
		RequestLoggerMiddleware(),
		ErrorLogMiddleware(),
		gin.Recovery(),
	)

	// Health checks (SPEC §10). /healthz is pure liveness (the Dockerfile
	// HEALTHCHECK depends on its semantics); /readyz probes Mongo, Redis and
	// MinIO through the injected health probes.
	s.engine.GET("/healthz", handleHealthz)
	s.engine.GET("/readyz", s.handleReadyz)

	// Métricas Prometheus (SPEC §10): scrapeado por el stack de observabilidad.
	// Se monta con gin.WrapH porque promhttp.Handler expone http.Handler, no gin.HandlerFunc.
	s.engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

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

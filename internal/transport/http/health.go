package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// readinessTimeout bounds every /readyz request. Individual probes get the
// same context; a probe that ignores it cannot hang the endpoint.
const readinessTimeout = 2 * time.Second

// healthResponse es el payload de los health checks.
type healthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Service   string `json:"service"`
	Version   string `json:"version,omitempty"`
}

// readinessResponse is the /readyz payload with one status per dependency.
type readinessResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Service   string            `json:"service"`
	Checks    map[string]string `json:"checks"`
}

// probeResult is what a probe goroutine reports back to the handler.
type probeResult struct {
	name string
	err  error
}

// handleHealthz responde 200 si el proceso está vivo.
func handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, healthResponse{
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Service:   "doc-service",
	})
}

// handleReadyz runs every dependency probe in parallel and answers 200 only
// when all of them pass (SPEC §11.7). It reports a generic per-dependency
// status; raw errors are logged server-side and never sent to the client.
func (s *Server) handleReadyz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), readinessTimeout)
	defer cancel()

	results := make(chan probeResult, len(s.probes))
	for _, probe := range s.probes {
		go func(p ports.HealthProbe) {
			results <- probeResult{name: p.Name(), err: p.Check(ctx)}
		}(probe)
	}

	checks := make(map[string]string, len(s.probes))
	pending := len(s.probes)
	healthy := true

	record := func(r probeResult) {
		pending--
		if r.err != nil {
			healthy = false
			checks[r.name] = "error"
			slog.Warn("readiness probe failed", "dependency", r.name, "error", r.err)
			return
		}
		checks[r.name] = "ok"
	}

collect:
	for pending > 0 {
		select {
		case r := <-results:
			record(r)
		case <-ctx.Done():
			break collect
		}
	}

	// Drain probes that finished at the same instant as the deadline.
	for draining := true; draining; {
		select {
		case r := <-results:
			record(r)
		default:
			draining = false
		}
	}

	// Anything still unresolved hit the deadline. Its goroutine may keep
	// running, but the buffered channel lets it exit without blocking.
	for _, probe := range s.probes {
		name := probe.Name()
		if _, seen := checks[name]; seen {
			continue
		}
		healthy = false
		checks[name] = "timeout"
		slog.Warn("readiness probe timed out", "dependency", name)
	}

	status := "ready"
	code := http.StatusOK
	if !healthy {
		status = "error"
		code = http.StatusServiceUnavailable
	}

	c.JSON(code, readinessResponse{
		Status:    status,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Service:   "doc-service",
		Checks:    checks,
	})
}

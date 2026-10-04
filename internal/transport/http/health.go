package httpserver

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// healthResponse es el payload de los health checks.
type healthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
	Service   string `json:"service"`
	Version   string `json:"version,omitempty"`
}

// handleHealthz responde 200 si el proceso está vivo.
func handleHealthz(c *gin.Context) {
	c.JSON(http.StatusOK, healthResponse{
		Status:    "healthy",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Service:   "doc-service",
	})
}

// handleReadyz responde 200 si el proceso está listo para recibir tráfico.
// En fases posteriores verificará Mongo, MinIO y Redis; por ahora solo proceso vivo.
func handleReadyz(c *gin.Context) {
	c.JSON(http.StatusOK, healthResponse{
		Status:    "ready",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Service:   "doc-service",
	})
}

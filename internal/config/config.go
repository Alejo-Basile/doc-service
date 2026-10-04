// Package config centraliza la configuración del servicio.
// Lee variables de entorno, aplica defaults seguros y valida las
// obligatorias antes de que el proceso arranque (fail-fast, SPEC §11.5).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contiene toda la configuración del doc-service.
// Los campos se cargan desde variables de entorno (12-Factor App).
type Config struct {
	// HTTP
	HTTPPort int
	Debug    bool

	// MongoDB (Replica Set requerido para Change Streams, SPEC §3.6)
	MongoURI      string
	MongoDatabase string

	// MinIO
	MinIOEndpoint       string // endpoint interno para el SDK
	MinIOPublicEndpoint string // host público para firmar URLs prefirmadas (ADR-0016)
	MinIOAccessKey      string
	MinIOSecretKey      string
	MinIOBucketRaw      string
	MinIOBucketTXT      string
	MinIOWebhookSecret  string

	// Redis
	RedisAddr      string
	RedisStreamKey string

	// Reglas de negocio
	MaxPDFBytes       int64
	ReconcileInterval time.Duration
	DocUploadGrace    time.Duration
	// MinSafetyAge es la edad mínima antes de que el reconciliador toque un documento (S6-P2-02).
	// Documentos más jóvenes que esto nunca son alterados bajo ninguna condición.
	MinSafetyAge time.Duration
}

// default values documentados en SPEC §11.5.
const (
	defaultHTTPPort          = 8080
	defaultMongoDatabase     = "documents"
	defaultMinIOBucketRaw    = "raw-pdfs"
	defaultMinIOBucketTXT    = "extracted-txt"
	defaultRedisStreamKey    = "stream:pdf-processing"
	defaultMaxPDFBytes       = 26214400 // 25 MB (SPEC §5.1)
	defaultReconcileMin      = 10
	defaultDocUploadGraceMin = 30 // >= 2x reconcil interval (SPEC §4)
	defaultMinSafetyAgeMin   = 15 // >= 1x reconcil interval (S6-P2-02)
)

// Load lee las variables de entorno, aplica defaults y valida.
// Devuelve un error que lista TODAS las variables obligatorias faltantes.
func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:            envInt("HTTP_PORT", defaultHTTPPort),
		Debug:               envBool("DEBUG", false),
		MongoURI:            strings.TrimSpace(os.Getenv("MONGO_URI")),
		MongoDatabase:       envStr("MONGO_DATABASE", defaultMongoDatabase),
		MinIOEndpoint:       strings.TrimSpace(os.Getenv("MINIO_ENDPOINT")),
		MinIOPublicEndpoint: strings.TrimSpace(os.Getenv("MINIO_PUBLIC_ENDPOINT")),
		MinIOAccessKey:      strings.TrimSpace(os.Getenv("MINIO_ACCESS_KEY")),
		MinIOSecretKey:      strings.TrimSpace(os.Getenv("MINIO_SECRET_KEY")),
		MinIOBucketRaw:      envStr("MINIO_BUCKET_RAW", defaultMinIOBucketRaw),
		MinIOBucketTXT:      envStr("MINIO_BUCKET_TXT", defaultMinIOBucketTXT),
		MinIOWebhookSecret:  strings.TrimSpace(os.Getenv("MINIO_WEBHOOK_SECRET")),
		RedisAddr:           strings.TrimSpace(os.Getenv("REDIS_ADDR")),
		RedisStreamKey:      envStr("REDIS_STREAM_KEY", defaultRedisStreamKey),
		MaxPDFBytes:         envInt64("MAX_PDF_BYTES", defaultMaxPDFBytes),
		ReconcileInterval:   time.Duration(envInt("RECONCILE_INTERVAL_MIN", defaultReconcileMin)) * time.Minute,
		DocUploadGrace:      time.Duration(envInt("DOC_UPLOAD_GRACE_MIN", defaultDocUploadGraceMin)) * time.Minute,
		MinSafetyAge:        time.Duration(envInt("MIN_SAFETY_AGE_MIN", defaultMinSafetyAgeMin)) * time.Minute,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate verifica que las variables obligatorias estén presentes.
// Acumula TODOS los errores en uno solo para que el operador vea el cuadro completo.
func (c *Config) Validate() error {
	var missing []string

	required := []struct {
		name  string
		value string
		hint  string
	}{
		{"MONGO_URI", c.MongoURI, "mongodb://user:pass@host:27017/documents?replicaSet=rs0"},
		{"MINIO_ENDPOINT", c.MinIOEndpoint, "minio:9000 (endpoint interno del SDK)"},
		{"MINIO_PUBLIC_ENDPOINT", c.MinIOPublicEndpoint, "https://s3.dominio (host público, ADR-0016)"},
		{"MINIO_ACCESS_KEY", c.MinIOAccessKey, "access key del servicio (nunca root)"},
		{"MINIO_SECRET_KEY", c.MinIOSecretKey, "secret key del servicio"},
		{"MINIO_WEBHOOK_SECRET", c.MinIOWebhookSecret, "secreto compartido del webhook (SPEC §9)"},
		{"REDIS_ADDR", c.RedisAddr, "host:puerto de Redis Streams"},
	}

	for _, r := range required {
		if r.value == "" {
			missing = append(missing, fmt.Sprintf("  - %s (ejemplo: %s)", r.name, r.hint))
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"configuración incompleta: faltan %d variables obligatorias:\n%s\n"+
				"Completa el archivo .env a partir de .env.example",
			len(missing),
			strings.Join(missing, "\n"),
		)
	}

	// Validaciones de coherencia entre valores.
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("configuración inválida: HTTP_PORT=%d fuera de rango [1-65535]", c.HTTPPort)
	}
	if c.MaxPDFBytes <= 0 {
		return fmt.Errorf("configuración inválida: MAX_PDF_BYTES debe ser positivo, got %d", c.MaxPDFBytes)
	}
	// SPEC §4: la gracia debe ser >= 2x el intervalo del reconciliador,
	// para que el TTL de Mongo nunca decida el expirado por su cuenta.
	minGrace := 2 * c.ReconcileInterval
	if c.DocUploadGrace < minGrace {
		return fmt.Errorf(
			"configuración inválida: DOC_UPLOAD_GRACE_MIN (%s) debe ser >= 2x RECONCILE_INTERVAL_MIN (%s)",
			c.DocUploadGrace, minGrace,
		)
	}
	// S6-P2-02: la edad mínima de seguridad debe ser >= 1x el intervalo,
	// para que el reconciliador nunca toque documentos en tránsito legítimo.
	if c.MinSafetyAge < c.ReconcileInterval {
		return fmt.Errorf(
			"configuración inválida: MIN_SAFETY_AGE_MIN (%s) debe ser >= RECONCILE_INTERVAL_MIN (%s)",
			c.MinSafetyAge, c.ReconcileInterval,
		)
	}

	return nil
}

// Addr retorna la dirección de escucha del servidor HTTP.
func (c *Config) Addr() string {
	return fmt.Sprintf(":%d", c.HTTPPort)
}

// --- helpers de lectura de env ---

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envInt64(key string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

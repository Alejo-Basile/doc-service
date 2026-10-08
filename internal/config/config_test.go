package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// setEnv limpia y establece variables de entorno para un test.
func setEnv(t *testing.T, vars map[string]string) {
	t.Helper()
	// Limpiar todas las variables relevantes primero.
	claves := []string{
		"HTTP_PORT", "DEBUG",
		"MONGO_URI", "MONGO_DATABASE",
		"MINIO_ENDPOINT", "MINIO_PUBLIC_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
		"MINIO_BUCKET_RAW", "MINIO_BUCKET_TXT", "MINIO_WEBHOOK_SECRET",
		"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_STREAM_KEY", "INTERNAL_TOKEN",
		"MAX_PDF_BYTES", "RECONCILE_INTERVAL_MIN", "DOC_UPLOAD_GRACE_MIN",
		"MIN_SAFETY_AGE_MIN",
	}
	for _, k := range claves {
		os.Unsetenv(k)
	}
	for k, v := range vars {
		os.Setenv(k, v)
	}
	t.Cleanup(func() {
		for _, k := range claves {
			os.Unsetenv(k)
		}
	})
}

// completeVars retorna un mapa con todas las variables obligatorias válidas.
func completeVars() map[string]string {
	return map[string]string{
		"MONGO_URI":             "mongodb://user:pass@localhost:27017/documents?replicaSet=rs0",
		"MINIO_ENDPOINT":        "minio:9000",
		"MINIO_PUBLIC_ENDPOINT": "https://s3.example.com",
		"MINIO_ACCESS_KEY":      "minioadmin",
		"MINIO_SECRET_KEY":      "minioadmin",
		"MINIO_WEBHOOK_SECRET":  "shared-secret",
		"REDIS_ADDR":            "localhost:6379",
		"REDIS_PASSWORD":        "redis-test-password",
		"INTERNAL_TOKEN":        "internal-test-token",
	}
}

func TestLoad_ConfigCompleta_Sucede(t *testing.T) {
	setEnv(t, completeVars())

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() no debía fallar con config completa: %v", err)
	}

	// Defaults
	if cfg.MongoDatabase != "documents" {
		t.Errorf("MongoDatabase default esperado 'documents', got %q", cfg.MongoDatabase)
	}
	if cfg.MinIOBucketRaw != "raw-pdfs" {
		t.Errorf("MinIOBucketRaw default esperado 'raw-pdfs', got %q", cfg.MinIOBucketRaw)
	}
	if cfg.MinIOBucketTXT != "extracted-txt" {
		t.Errorf("MinIOBucketTXT default esperado 'extracted-txt', got %q", cfg.MinIOBucketTXT)
	}
	if cfg.RedisStreamKey != "stream:pdf-processing" {
		t.Errorf("RedisStreamKey default esperado 'stream:pdf-processing', got %q", cfg.RedisStreamKey)
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort default esperado 8080, got %d", cfg.HTTPPort)
	}
	if cfg.MaxPDFBytes != 26214400 {
		t.Errorf("MaxPDFBytes default esperado 26214400, got %d", cfg.MaxPDFBytes)
	}
	if cfg.ReconcileInterval != 10*time.Minute {
		t.Errorf("ReconcileInterval default esperado 10m, got %s", cfg.ReconcileInterval)
	}
	if cfg.DocUploadGrace != 30*time.Minute {
		t.Errorf("DocUploadGrace default esperado 30m, got %s", cfg.DocUploadGrace)
	}

	// Valores cargados
	if cfg.MongoURI != "mongodb://user:pass@localhost:27017/documents?replicaSet=rs0" {
		t.Errorf("MongoURI no coincide")
	}
	if cfg.MinIOEndpoint != "minio:9000" {
		t.Errorf("MinIOEndpoint no coincide")
	}

	// Addr()
	if cfg.Addr() != ":8080" {
		t.Errorf("Addr() esperado ':8080', got %q", cfg.Addr())
	}
}

func TestLoad_MinIOEndpointFaltante_Falla(t *testing.T) {
	vars := completeVars()
	delete(vars, "MINIO_ENDPOINT")
	setEnv(t, vars)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() debía fallar sin MINIO_ENDPOINT")
	}
	if !strings.Contains(err.Error(), "MINIO_ENDPOINT") {
		t.Errorf("el error debe mencionar MINIO_ENDPOINT, got: %v", err)
	}
	if !strings.Contains(err.Error(), "minio:9000") {
		t.Errorf("el error debe incluir un ejemplo de valor, got: %v", err)
	}
}

func TestLoad_MultiplesVariablesFaltantes_AcumulaErrores(t *testing.T) {
	setEnv(t, map[string]string{}) // nada definido

	_, err := Load()
	if err == nil {
		t.Fatal("Load() debía fallar sin ninguna variable")
	}

	// Debe listar TODAS las obligatorias faltantes.
	esperadas := []string{
		"MONGO_URI", "MINIO_ENDPOINT", "MINIO_PUBLIC_ENDPOINT",
		"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_WEBHOOK_SECRET", "REDIS_ADDR",
		"REDIS_PASSWORD",
		"INTERNAL_TOKEN",
	}
	for _, e := range esperadas {
		if !strings.Contains(err.Error(), e) {
			t.Errorf("el error debe mencionar %s, got: %v", e, err)
		}
	}
	// Debe indicar cuántas faltan.
	if !strings.Contains(err.Error(), "faltan 9 variables") {
		t.Errorf("el error debe indicar 'faltan 9 variables', got: %v", err)
	}
}

func TestLoad_ValoresCustom_SeAplican(t *testing.T) {
	vars := completeVars()
	vars["HTTP_PORT"] = "9090"
	vars["DEBUG"] = "true"
	vars["MONGO_DATABASE"] = "docs_prod"
	vars["MINIO_BUCKET_RAW"] = "pdfs-raw"
	vars["MAX_PDF_BYTES"] = "10485760" // 10 MB
	vars["RECONCILE_INTERVAL_MIN"] = "5"
	vars["DOC_UPLOAD_GRACE_MIN"] = "15" // >= 2x5=10, OK
	setEnv(t, vars)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() falló: %v", err)
	}

	if cfg.HTTPPort != 9090 {
		t.Errorf("HTTPPort esperado 9090, got %d", cfg.HTTPPort)
	}
	if !cfg.Debug {
		t.Error("Debug esperado true")
	}
	if cfg.MongoDatabase != "docs_prod" {
		t.Errorf("MongoDatabase esperado 'docs_prod', got %q", cfg.MongoDatabase)
	}
	if cfg.MaxPDFBytes != 10485760 {
		t.Errorf("MaxPDFBytes esperado 10485760, got %d", cfg.MaxPDFBytes)
	}
	if cfg.ReconcileInterval != 5*time.Minute {
		t.Errorf("ReconcileInterval esperado 5m, got %s", cfg.ReconcileInterval)
	}
	if cfg.DocUploadGrace != 15*time.Minute {
		t.Errorf("DocUploadGrace esperado 15m, got %s", cfg.DocUploadGrace)
	}
	if cfg.Addr() != ":9090" {
		t.Errorf("Addr() esperado ':9090', got %q", cfg.Addr())
	}
}

func TestValidate_GraciaMenorQue2xIntervalo_Falla(t *testing.T) {
	vars := completeVars()
	vars["RECONCILE_INTERVAL_MIN"] = "10"
	vars["DOC_UPLOAD_GRACE_MIN"] = "15" // 15 < 20 = 2x10
	setEnv(t, vars)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() debía fallar con gracia < 2x intervalo")
	}
	if !strings.Contains(err.Error(), "DOC_UPLOAD_GRACE_MIN") {
		t.Errorf("el error debe mencionar DOC_UPLOAD_GRACE_MIN, got: %v", err)
	}
}

func TestValidate_PuertoInvalido_Falla(t *testing.T) {
	vars := completeVars()
	vars["HTTP_PORT"] = "0"
	setEnv(t, vars)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() debía fallar con HTTP_PORT=0")
	}
	if !strings.Contains(err.Error(), "HTTP_PORT") {
		t.Errorf("el error debe mencionar HTTP_PORT, got: %v", err)
	}
}

func TestValidate_MaxPDFBytesNegativo_Falla(t *testing.T) {
	vars := completeVars()
	vars["MAX_PDF_BYTES"] = "-1"
	setEnv(t, vars)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() debía fallar con MAX_PDF_BYTES negativo")
	}
	if !strings.Contains(err.Error(), "MAX_PDF_BYTES") {
		t.Errorf("el error debe mencionar MAX_PDF_BYTES, got: %v", err)
	}
}

func TestValidate_VacioYSoloEspacios_SeTrataComoFaltante(t *testing.T) {
	vars := completeVars()
	vars["MINIO_ENDPOINT"] = "   " // solo espacios
	setEnv(t, vars)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() debía fallar con MINIO_ENDPOINT vacío/espaceado")
	}
	if !strings.Contains(err.Error(), "MINIO_ENDPOINT") {
		t.Errorf("el error debe mencionar MINIO_ENDPOINT, got: %v", err)
	}
}

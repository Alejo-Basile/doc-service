package configkv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Alejo-Basile/doc-service/internal/config"
	"github.com/Alejo-Basile/doc-service/internal/metrics"
)

// validBase returns a config.Config that passes Validate(), so the manager can
// use it as the environment-defaults baseline in tests.
func validBase(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		HTTPPort:          8080,
		MongoURI:          "mongodb://user:pass@localhost:27017/documents?replicaSet=rs0",
		MongoDatabase:     "documents",
		MinIOEndpoint:     "minio:9000",
		MinIOPublicEndpoint: "https://s3.localhost",
		MinIOAccessKey:    "access",
		MinIOSecretKey:    "secret",
		MinIOWebhookSecret: "webhook-secret",
		RedisAddr:         "redis:6379",
		RedisPassword:     "redis-pass",
		InternalToken:     "internal-token",
		MaxPDFBytes:       26214400,
		ReconcileInterval: 10 * time.Minute,
		DocUploadGrace:    30 * time.Minute,
		MinSafetyAge:      15 * time.Minute,
	}
}

// newManagerForTest builds a manager whose cache path and HTTP client are
// redirected to test-controlled locations.
func newManagerForTest(t *testing.T, srv *httptest.Server) *Manager {
	t.Helper()
	base := validBase(t)
	if err := base.Validate(); err != nil {
		t.Fatalf("test base config must be valid: %v", err)
	}
	t.Setenv("CONFIG_CACHE_PATH", filepath.Join(t.TempDir(), "config-cache.json"))
	t.Setenv("CONSUL_KV_DISABLED", "false")
	m := NewManager(base)
	if srv != nil {
		m.Client.Addr = srv.URL
	}
	m.Client.CachePath = m.CachePath
	return m
}

// kvServer serves a fixed KV payload and records how many requests it saw.
func kvServer(t *testing.T, status int, payload map[string]string) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		pairs := make([]KVPair, 0, len(payload))
		for k, v := range payload {
			pairs = append(pairs, KVPair{
				Key:         "config/doc-service/" + k,
				Value:       base64.StdEncoding.EncodeToString([]byte(v)),
				ModifyIndex: 1,
			})
		}
		w.Header().Set("X-Consul-Index", "1")
		if err := json.NewEncoder(w).Encode(pairs); err != nil {
			t.Errorf("encode pairs: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestStartKV500UsesDefaultsWithin2s(t *testing.T) {
	srv, _ := kvServer(t, http.StatusInternalServerError, nil)
	m := newManagerForTest(t, srv)

	start := time.Now()
	m.Start(context.Background())
	elapsed := time.Since(start)

	if elapsed >= 2*time.Second {
		t.Fatalf("Start took %s, want < 2s with KV returning 500", elapsed)
	}
	if got := m.CurrentConfig().MaxPDFBytes; got != 26214400 {
		t.Fatalf("MaxPDFBytes = %d, want default 26214400", got)
	}
	if m.Applied() {
		t.Fatal("Applied() = true after a failed KV read")
	}
}

func TestStartCorruptCacheDoesNotBreak(t *testing.T) {
	srv, _ := kvServer(t, http.StatusInternalServerError, nil)
	m := newManagerForTest(t, srv)
	if err := os.WriteFile(m.CachePath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write corrupt cache: %v", err)
	}

	m.Start(context.Background())

	if got := m.CurrentConfig().MaxPDFBytes; got != 26214400 {
		t.Fatalf("MaxPDFBytes = %d, want default after corrupt cache", got)
	}
}

func TestStartAppliesCacheWhenConsulDown(t *testing.T) {
	srv, _ := kvServer(t, http.StatusInternalServerError, nil)
	m := newManagerForTest(t, srv)
	if err := SaveAtomic(m.CachePath, map[string]string{
		"max_pdf_bytes":              "1048576",
		"reconcile_interval_minutes": "10",
		"doc_upload_grace_minutes":   "30",
		"min_safety_age_minutes":     "15",
	}); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	m.Start(context.Background())

	if got := m.CurrentConfig().MaxPDFBytes; got != 1048576 {
		t.Fatalf("MaxPDFBytes = %d, want cached 1048576", got)
	}
}

func TestStartAppliesKV(t *testing.T) {
	srv, _ := kvServer(t, http.StatusOK, map[string]string{
		"max_pdf_bytes":              "1048576",
		"reconcile_interval_minutes": "10",
		"reconcile_dry_run":          "true",
		"doc_upload_grace_minutes":   "30",
		"min_safety_age_minutes":     "15",
	})
	m := newManagerForTest(t, srv)

	m.Start(context.Background())

	cfg := m.CurrentConfig()
	if cfg.MaxPDFBytes != 1048576 {
		t.Fatalf("MaxPDFBytes = %d, want 1048576 from KV", cfg.MaxPDFBytes)
	}
	if !cfg.DryRun {
		t.Fatal("DryRun = false, want true from KV")
	}
	if !m.Applied() {
		t.Fatal("Applied() = false after a successful KV read")
	}
}

func TestInvalidValueRejectedKeepsPrevious(t *testing.T) {
	srv, _ := kvServer(t, http.StatusOK, map[string]string{"max_pdf_bytes": "26214400"})
	m := newManagerForTest(t, srv)
	m.Start(context.Background())

	before := testutil.ToFloat64(metrics.ConfigKVRejected.WithLabelValues("max_pdf_bytes"))
	rejections := m.ApplyValues(map[string]string{"max_pdf_bytes": "209715200"})
	after := testutil.ToFloat64(metrics.ConfigKVRejected.WithLabelValues("max_pdf_bytes"))

	if len(rejections) != 1 || rejections[0].Key != "max_pdf_bytes" || rejections[0].Reason != "range" {
		t.Fatalf("rejections = %+v, want one {max_pdf_bytes range}", rejections)
	}
	if after-before != 1 {
		t.Fatalf("rejected counter delta = %v, want 1", after-before)
	}
	if got := m.CurrentConfig().MaxPDFBytes; got != 26214400 {
		t.Fatalf("MaxPDFBytes = %d, want previous 26214400", got)
	}
}

func TestCoherenceRejectedKeepsPrevious(t *testing.T) {
	srv, _ := kvServer(t, http.StatusOK, map[string]string{
		"reconcile_interval_minutes": "10",
		"doc_upload_grace_minutes":   "30",
	})
	m := newManagerForTest(t, srv)
	m.Start(context.Background())

	rejections := m.ApplyValues(map[string]string{"doc_upload_grace_minutes": "15"})
	if len(rejections) != 1 || rejections[0].Key != "doc_upload_grace_minutes" || rejections[0].Reason != "coherence" {
		t.Fatalf("rejections = %+v, want one {doc_upload_grace_minutes coherence}", rejections)
	}
	if got := m.CurrentConfig().DocUploadGrace; got != 30*time.Minute {
		t.Fatalf("DocUploadGrace = %s, want previous 30m", got)
	}
}

func TestDisabledSkipsConsul(t *testing.T) {
	srv, hits := kvServer(t, http.StatusOK, map[string]string{"max_pdf_bytes": "1048576"})
	m := newManagerForTest(t, srv)
	m.Disabled = true

	m.Start(context.Background())

	if *hits != 0 {
		t.Fatalf("Consul hit %d times with CONSUL_KV_DISABLED, want 0", *hits)
	}
	if m.Applied() {
		t.Fatal("Applied() = true while disabled")
	}
}

func TestCachePersistedAfterApply(t *testing.T) {
	srv, _ := kvServer(t, http.StatusOK, map[string]string{
		"max_pdf_bytes":              "1048576",
		"reconcile_interval_minutes": "10",
		"doc_upload_grace_minutes":   "30",
		"min_safety_age_minutes":     "15",
	})
	m := newManagerForTest(t, srv)
	m.Start(context.Background())

	cached, err := LoadCache(m.CachePath)
	if err != nil {
		t.Fatalf("LoadCache: %v", err)
	}
	if cached["max_pdf_bytes"] != "1048576" {
		t.Fatalf("cached max_pdf_bytes = %q, want 1048576", cached["max_pdf_bytes"])
	}
}

func TestValidateDoesNotMutate(t *testing.T) {
	srv, _ := kvServer(t, http.StatusOK, map[string]string{"max_pdf_bytes": "26214400"})
	m := newManagerForTest(t, srv)
	m.Start(context.Background())
	before := m.CurrentConfig().MaxPDFBytes

	rejections := m.Validate("max_pdf_bytes", "209715200", m.snapshotEffective())
	if len(rejections) != 1 {
		t.Fatalf("rejections = %+v, want one", rejections)
	}
	if got := m.CurrentConfig().MaxPDFBytes; got != before {
		t.Fatalf("Validate mutated config: %d -> %d", before, got)
	}
}

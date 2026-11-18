package consulreg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/configkv"
)

// newTestRegistrar devuelve un Registrar apuntando a un servidor de prueba.
func newTestRegistrar(t *testing.T, srv *httptest.Server, mutate func(*Config)) *Registrar {
	t.Helper()
	cfg := Config{
		ServiceName: "doc-service",
		AgentAddr:   srv.URL,
		HTTPPort:    8080,
		AddressProbe: func(string) (string, error) {
			return "172.24.0.4", nil
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	client := &configkv.Client{Addr: srv.URL, HTTPClient: srv.Client()}
	return New(cfg, client)
}

// TestRegisterPayload asserts the full registration contract (task §4.2).
func TestRegisterPayload(t *testing.T) {
	var (
		method string
		path   string
		body   registerPayload
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("payload no decodificable: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, nil)
	if err := r.Register(context.Background()); err != nil {
		t.Fatalf("Register devolvió error: %v", err)
	}

	if method != http.MethodPut {
		t.Fatalf("método = %q, se esperaba PUT", method)
	}
	if path != "/v1/agent/service/register" {
		t.Fatalf("path = %q, se esperaba /v1/agent/service/register", path)
	}
	if got, want := body.ID, "doc-service-"+hostname(); got != want {
		t.Fatalf("ID = %q, se esperaba %q", got, want)
	}
	if body.Name != "svc-doc-service" {
		t.Fatalf("Name = %q, se esperaba svc-doc-service", body.Name)
	}
	if body.Address != "172.24.0.4" {
		t.Fatalf("Address = %q, se esperaba la dirección resuelta", body.Address)
	}
	if body.Port != 8080 {
		t.Fatalf("Port = %d, se esperaba 8080", body.Port)
	}
	wantTags := []string{"http", "s11", "doc-service", "metrics_path=/metrics"}
	if strings.Join(body.Tags, ",") != strings.Join(wantTags, ",") {
		t.Fatalf("Tags = %v, se esperaban %v", body.Tags, wantTags)
	}
	if body.Check.HTTP != "http://172.24.0.4:8080/healthz" {
		t.Fatalf("Check.HTTP = %q", body.Check.HTTP)
	}
	if body.Check.Interval != "10s" || body.Check.Timeout != "3s" {
		t.Fatalf("Check Interval/Timeout = %q/%q", body.Check.Interval, body.Check.Timeout)
	}
	if body.Check.DeregisterCriticalServiceAfter != "1m" {
		t.Fatalf("DeregisterCriticalServiceAfter = %q, se esperaba 1m",
			body.Check.DeregisterCriticalServiceAfter)
	}
}

// TestDeregisterPath asserts the deregistration endpoint carries the replica ID.
func TestDeregisterPath(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, nil)
	if err := r.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister devolvió error: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("método = %q, se esperaba PUT", gotMethod)
	}
	if want := "/v1/agent/service/deregister/" + r.ID(); gotPath != want {
		t.Fatalf("path = %q, se esperaba %q", gotPath, want)
	}
}

// TestDisabledSkipsHTTP asserts CONSUL_REGISTRATION_DISABLED=true performs no calls.
func TestDisabledSkipsHTTP(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, func(c *Config) { c.Disabled = true })
	if r.Enabled() {
		t.Fatal("Enabled() = true con Disabled=true")
	}
	if err := r.Register(context.Background()); err != nil {
		t.Fatalf("Register con registro desactivado: %v", err)
	}
	if err := r.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister con registro desactivado: %v", err)
	}
	if calls != 0 {
		t.Fatalf("se hicieron %d requests con el registro desactivado", calls)
	}
}

// TestRegisterNon2xx asserts a rejected registration surfaces an error.
func TestRegisterNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, nil)
	err := r.Register(context.Background())
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %v, se esperaba un error con status 403", err)
	}
}

// TestRegisterTransportError asserts an unreachable agent surfaces an error
// instead of hanging or panicking.
func TestRegisterTransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	srv.Close() // cliente vivo, servidor cerrado: error de transporte

	client := &configkv.Client{Addr: addr, HTTPClient: http.DefaultClient}
	r := New(Config{
		ServiceName:  "doc-service",
		AgentAddr:    addr,
		HTTPPort:     8080,
		AddressProbe: func(string) (string, error) { return "172.24.0.4", nil },
	}, client)
	if err := r.Register(context.Background()); err == nil {
		t.Fatal("Register con el agente caído devolvió nil")
	}
}

// TestAddressProbeFallback asserts a failing probe degrades to the hostname
// instead of leaving Address empty.
func TestAddressProbeFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, func(c *Config) {
		c.AddressProbe = func(string) (string, error) { return "", errors.New("sin interfaz") }
	})
	if got := r.Address(); got != hostname() {
		t.Fatalf("Address = %q, se esperaba el hostname %q", got, hostname())
	}
}

// TestHostFromAddr covers the accepted CONSUL_REGISTRATION_ADDR shapes.
func TestHostFromAddr(t *testing.T) {
	cases := map[string]string{
		"https://consul-agent:8501": "consul-agent",
		"http://127.0.0.1:8500":     "127.0.0.1",
		"consul-agent:8501":         "consul-agent",
		"":                          "consul-agent",
	}
	for in, want := range cases {
		if got := hostFromAddr(in); got != want {
			t.Errorf("hostFromAddr(%q) = %q, se esperaba %q", in, got, want)
		}
	}
}

// TestServiceIDUsesHostname pins the per-replica uniqueness rule.
func TestServiceIDUsesHostname(t *testing.T) {
	if got, want := serviceID(), "doc-service-"+hostname(); got != want {
		t.Fatalf("serviceID() = %q, se esperaba %q", got, want)
	}
	h, _ := os.Hostname()
	if h == "" {
		t.Fatal("hostname vacío en el entorno de test")
	}
}

// TestRegisterWithRetrySucceedsAfterFailures asserts the retry loop keeps
// trying until the agent accepts the registration (Consul levantando después
// que la app) instead of leaving the replica out of the catalog forever.
func TestRegisterWithRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, func(c *Config) {
		c.RetryInitial = time.Millisecond
		c.RetryMax = 2 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r.RegisterWithRetry(ctx)

	if calls != 3 {
		t.Fatalf("registro intentado %d veces, se esperaban 3", calls)
	}
}

// TestRegisterWithRetryStopsOnCancel asserts the retry loop ends with the
// context, so a shutdown while the agent is down does not leak a goroutine.
func TestRegisterWithRetryStopsOnCancel(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	r := newTestRegistrar(t, srv, func(c *Config) {
		c.RetryInitial = time.Millisecond
		c.RetryMax = 2 * time.Millisecond
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r.RegisterWithRetry(ctx)

	if calls < 2 {
		t.Fatalf("se esperaba más de un intento antes de cancelar, hubo %d", calls)
	}
}

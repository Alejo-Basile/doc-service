package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// fakeProbe is a hermetic ports.HealthProbe used to exercise /readyz
// without Docker or network access.
type fakeProbe struct {
	name  string
	err   error
	delay time.Duration
}

func (p fakeProbe) Name() string { return p.name }

func (p fakeProbe) Check(context.Context) error {
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	return p.err
}

type readyPayload struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func getReadyz(t *testing.T, probes ...ports.HealthProbe) (int, readyPayload, string) {
	t.Helper()

	server := NewServer(false, probes...)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	server.Engine().ServeHTTP(w, req)

	var payload readyPayload
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	return w.Code, payload, w.Body.String()
}

func TestReadyz_AllProbesOK_Returns200(t *testing.T) {
	code, payload, _ := getReadyz(t,
		fakeProbe{name: "mongo"},
		fakeProbe{name: "redis"},
		fakeProbe{name: "minio"},
	)

	if code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", code)
	}
	want := map[string]string{"mongo": "ok", "redis": "ok", "minio": "ok"}
	for name, status := range want {
		if payload.Checks[name] != status {
			t.Errorf("check %q: expected %q, got %q", name, status, payload.Checks[name])
		}
	}
}

func TestReadyz_FailingProbe_Returns503NamingDependency(t *testing.T) {
	const rawErr = "dial redis://user:pass@10.0.0.5:6379: connection refused"
	code, payload, body := getReadyz(t,
		fakeProbe{name: "mongo"},
		fakeProbe{name: "redis", err: errors.New(rawErr)},
		fakeProbe{name: "minio"},
	)

	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", code)
	}
	if payload.Checks["redis"] != "error" {
		t.Errorf("check redis: expected error, got %q", payload.Checks["redis"])
	}
	for _, name := range []string{"mongo", "minio"} {
		if payload.Checks[name] != "ok" {
			t.Errorf("check %q: expected ok, got %q", name, payload.Checks[name])
		}
	}
	// Sanitization: no DSN, credentials or raw driver details in the body.
	for _, leaked := range []string{"user:pass", "10.0.0.5", "connection refused"} {
		if contains(body, leaked) {
			t.Errorf("body leaked internal detail %q: %s", leaked, body)
		}
	}
}

func TestReadyz_SlowProbe_Returns503WithinDeadline(t *testing.T) {
	start := time.Now()
	code, payload, _ := getReadyz(t,
		fakeProbe{name: "mongo"},
		fakeProbe{name: "redis"},
		// Ignores the context on purpose: the endpoint must not hang on it.
		fakeProbe{name: "minio", delay: 5 * time.Second},
	)
	elapsed := time.Since(start)

	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", code)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("readyz took %s, must be bounded by the deadline (~2s)", elapsed)
	}
	if payload.Checks["minio"] == "ok" {
		t.Errorf("check minio must not be ok after the timeout: %q", payload.Checks["minio"])
	}
}

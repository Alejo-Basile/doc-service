package redis

import (
	"context"
	"os"
	"testing"
	"time"
)

func leaseRedisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return addr
}

func leaseRedisPassword(t *testing.T) string {
	t.Helper()
	return os.Getenv("REDIS_TEST_PASSWORD")
}

func setupLease(t *testing.T) (*Lease, func()) {
	t.Helper()
	lease := NewLease(leaseRedisAddr(t), leaseRedisPassword(t))
	if err := lease.Ping(context.Background()); err != nil {
		t.Skipf("Redis no disponible: %v", err)
	}
	return lease, func() { _ = lease.Close() }
}

func TestLease_AcquireYRelease(t *testing.T) {
	lease, cleanup := setupLease(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:lease:1"

	// Primer acquire: debe funcionar.
	ok, err := lease.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if !ok {
		t.Fatal("Acquire = false, want true")
	}

	// Segundo acquire con la misma clave: debe fallar (otro holder).
	ok, err = lease.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire (2do) error: %v", err)
	}
	if ok {
		t.Fatal("Acquire (2do) = true, want false (lease ya tomado)")
	}

	// Release: libera el lease.
	if err := lease.Release(ctx, key); err != nil {
		t.Fatalf("Release error: %v", err)
	}

	// Tercer acquire: debe funcionar después del release.
	ok, err = lease.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire (3ro) error: %v", err)
	}
	if !ok {
		t.Fatal("Acquire (3ro) = false, want true")
	}

	// Cleanup.
	_ = lease.Release(ctx, key)
}

func TestLease_Expiracion(t *testing.T) {
	lease, cleanup := setupLease(t)
	defer cleanup()

	ctx := context.Background()
	key := "test:lease:exp"

	// Acquire con TTL corto.
	ok, err := lease.Acquire(ctx, key, 1*time.Second)
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if !ok {
		t.Fatal("Acquire = false, want true")
	}

	// Esperar a que expire.
	time.Sleep(1500 * time.Millisecond)

	// Acquire de nuevo: debe funcionar porque el lease expiró.
	ok, err = lease.Acquire(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire (post-exp) error: %v", err)
	}
	if !ok {
		t.Fatal("Acquire (post-exp) = false, want true (lease expirado)")
	}

	_ = lease.Release(ctx, key)
}

func TestLease_ClavesDistintas(t *testing.T) {
	lease, cleanup := setupLease(t)
	defer cleanup()

	ctx := context.Background()

	// Dos claves distintas: ambas deben poder adquirirse.
	ok1, err := lease.Acquire(ctx, "test:lease:a", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire a: %v", err)
	}
	ok2, err := lease.Acquire(ctx, "test:lease:b", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire b: %v", err)
	}
	if !ok1 || !ok2 {
		t.Fatalf("Acquire = (%v, %v), want (true, true)", ok1, ok2)
	}

	_ = lease.Release(ctx, "test:lease:a")
	_ = lease.Release(ctx, "test:lease:b")
}

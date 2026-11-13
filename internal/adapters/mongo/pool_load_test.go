package mongo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/domain"
)

// TestClient_PoolChico_Carga20Concurrente_SinTimeouts verifica el DoD de S1-P2-01:
// una carga de 20 concurrencias contra un pool deliberadamente chico (2 conexiones)
// no produce timeouts. Con MaxPoolSize=2 el driver encola las operaciones esperando
// conexión en vez de fallar; el deadline por operación (PoolConfig.Timeout) no debe
// vencerse en ninguna de ellas.
//
// Requiere un MongoDB Replica Set (MONGO_TEST_URI o el default del paquete).
func TestClient_PoolChico_Carga20Concurrente_SinTimeouts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := PoolConfig{
		MaxPoolSize:     2,
		MinPoolSize:     2,
		ConnectTimeout:  5 * time.Second,
		Timeout:         30 * time.Second,
		MaxConnIdleTime: 60 * time.Second,
		RetryWrites:     true,
	}

	dbName := fmt.Sprintf("docservice_pool_load_%d", time.Now().UnixNano())
	client, err := NewClient(ctx, testMongoURI(t), dbName, cfg)
	if err != nil {
		t.Fatalf("NewClient falló (¿replica set corriendo?): %v", err)
	}
	defer func() {
		_ = client.DB().Drop(context.Background())
		_ = client.Close(context.Background())
	}()

	repo := NewDocumentRepository(client, "documents")

	const concurrencia = 20
	const opsPorGoroutine = 5

	inicio := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, concurrencia)

	for i := 0; i < concurrencia; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < opsPorGoroutine; j++ {
				doc := domain.NewDocument(
					fmt.Sprintf("corr-pool-load-%d-%d", i, j),
					time.Now().UTC().Add(24*time.Hour),
				)

				if err := repo.Insert(ctx, doc); err != nil {
					errCh <- fmt.Errorf("goroutine %d op %d insert: %w", i, j, err)
					return
				}

				ok, err := repo.UpdateStatus(ctx, doc.ID,
					domain.StatusPendingUpload, domain.StatusUploaded,
					map[string]any{"object_key": fmt.Sprintf("raw-pdfs/%s.pdf", doc.ID)},
				)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d op %d update: %w", i, j, err)
					return
				}
				if !ok {
					errCh <- fmt.Errorf("goroutine %d op %d update: transición no matcheó", i, j)
					return
				}

				if _, err := repo.GetByID(ctx, doc.ID); err != nil {
					errCh <- fmt.Errorf("goroutine %d op %d get: %w", i, j, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	timeouts := 0
	for err := range errCh {
		if esTimeout(err) {
			timeouts++
			t.Errorf("TIMEOUT bajo carga (pool=2): %v", err)
			continue
		}
		t.Errorf("operación falló bajo carga (pool=2): %v", err)
	}

	t.Logf("carga completada: %d goroutines × %d ops × 3 operaciones en %s (MaxPoolSize=%d)",
		concurrencia, opsPorGoroutine, time.Since(inicio).Round(time.Millisecond), cfg.MaxPoolSize)

	if timeouts > 0 {
		t.Errorf("DoD S1-P2-01 incumplido: %d timeouts con MaxPoolSize=%d y %d concurrencias",
			timeouts, cfg.MaxPoolSize, concurrencia)
	}
}

// esTimeout reporta si err es —o envuelve— un timeout de operación o de red.
func esTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline") ||
		strings.Contains(msg, "exceeded time limit")
}

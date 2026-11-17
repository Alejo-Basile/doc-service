package minio

import (
	"context"
	"fmt"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// healthProbe adapts an ObjectStorage to ports.HealthProbe.
type healthProbe struct {
	storage *ObjectStorage
}

// NewHealthProbe returns a readiness probe for MinIO.
func NewHealthProbe(storage *ObjectStorage) ports.HealthProbe {
	return &healthProbe{storage: storage}
}

// Name identifies the dependency in the readiness payload.
func (p *healthProbe) Name() string { return "minio" }

// Check uses BucketExists on the raw bucket: it is a cheap, non-mutating HEAD
// request that exercises connectivity and credentials without listing objects.
// A missing raw bucket is also a readiness failure because uploads depend on it.
func (p *healthProbe) Check(ctx context.Context) error {
	exists, err := p.storage.client.BucketExists(ctx, p.storage.bucketRaw)
	if err != nil {
		return fmt.Errorf("minio bucket exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("minio bucket %q not found", p.storage.bucketRaw)
	}
	return nil
}

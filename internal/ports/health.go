package ports

import "context"

// HealthProbe checks the connectivity of a single dependency.
// Readiness aggregates probes: any failing probe makes the service not ready
// (SPEC §11.7). Implementations live in the adapters and must not expose
// credentials or driver internals through their errors.
type HealthProbe interface {
	// Name is the dependency identifier shown in the readiness payload
	// (e.g. "mongo", "redis", "minio").
	Name() string
	// Check returns nil when the dependency is reachable and usable.
	Check(ctx context.Context) error
}

package configkv

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/config"
	"github.com/Alejo-Basile/doc-service/internal/metrics"
)

type Manager struct {
	Client    *Client
	CachePath string
	Disabled  bool
	Ptr       *atomic.Pointer[config.Config]
	Base      *config.Config

	mu          sync.Mutex
	Effective   map[string]string
	LastSuccess time.Time
	applied     atomic.Bool

	notifyCh chan struct{}
}

func NewManager(base *config.Config) *Manager {
	cp := os.Getenv("CONFIG_CACHE_PATH")
	if cp == "" {
		cp = "/var/lib/doc-service/config-cache.json"
	}
	m := &Manager{
		Client:    NewClientFromEnv(),
		CachePath: cp,
		Disabled:  os.Getenv("CONSUL_KV_DISABLED") == "true",
		Ptr:       &atomic.Pointer[config.Config]{},
		Base:      base,
		Effective: map[string]string{},
		notifyCh:  make(chan struct{}, 1),
	}
	m.Ptr.Store(base)
	return m
}

func Enabled() bool { return os.Getenv("CONSUL_KV_DISABLED") != "true" }

func (m *Manager) Enabled() bool { return !m.Disabled }

func DefaultCachePath() string {
	if v := os.Getenv("CONFIG_CACHE_PATH"); v != "" {
		return v
	}
	return "/var/lib/doc-service/config-cache.json"
}

func (m *Manager) CurrentConfig() *config.Config { return m.Ptr.Load() }

// Applied reports whether at least one KV apply succeeded, so callers can
// report the effective configuration source truthfully.
func (m *Manager) Applied() bool { return m.applied.Load() }

func (m *Manager) Notify() <-chan struct{} { return m.notifyCh }

func (m *Manager) notify() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

// snapshotEffective returns a copy of the effective raw value set. Callers must
// not read m.Effective directly: the watcher writes it from another goroutine.
func (m *Manager) snapshotEffective() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.Effective))
	for k, v := range m.Effective {
		out[k] = v
	}
	return out
}

func (m *Manager) EffectiveValues() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{}
	for k, v := range m.Effective {
		var val any
		if err := json.Unmarshal([]byte(v), &val); err != nil {
			out[k] = v
			continue
		}
		out[k] = val
	}
	return out
}

func configToMap(c *config.Config) map[string]any {
	return map[string]any{
		"max_pdf_bytes":                 c.MaxPDFBytes,
		"presign_upload_ttl_seconds":    int64(c.PresignUploadTTL / time.Second),
		"presign_download_ttl_seconds":  int64(c.PresignDownloadTTL / time.Second),
		"reconcile_interval_minutes":    int64(c.ReconcileInterval / time.Minute),
		"reconcile_stuck_after_minutes": int64(c.ReconcileStuckAfter / time.Minute),
		"doc_upload_grace_minutes":      int64(c.DocUploadGrace / time.Minute),
		"min_safety_age_minutes":        int64(c.MinSafetyAge / time.Minute),
		"reconcile_dry_run":             c.DryRun,
	}
}

func parseOverlay(raw map[string]string) map[string]any {
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		var parsed any
		if err := json.Unmarshal([]byte(v), &parsed); err != nil {
			out[k] = v
			continue
		}
		out[k] = parsed
	}
	return out
}

func equalOverlay(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func mergeOverlay(base *config.Config, overlay map[string]string) map[string]any {
	cur := configToMap(base)
	for k, v := range parseOverlay(overlay) {
		cur[k] = v
	}
	return cur
}

type applyOpts struct {
	saveCache   bool
	markSuccess bool
}

func (m *Manager) apply(incoming map[string]any, opts applyOpts) []Rejection {
	m.mu.Lock()
	prev := make(map[string]string, len(m.Effective))
	for k, v := range m.Effective {
		prev[k] = v
	}
	accepted, rejections := Apply(mergeOverlay(m.Base, prev), incoming)
	rejected := make(map[string]bool, len(rejections))
	for _, r := range rejections {
		rejected[r.Key] = true
	}
	next := make(map[string]string, len(prev)+len(incoming))
	for k, v := range prev {
		next[k] = v
	}
	for k := range incoming {
		if rejected[k] {
			continue
		}
		if v, ok := accepted[k]; ok {
			if b, err := json.Marshal(v); err == nil {
				next[k] = string(b)
			}
		}
	}
	candidate := *m.Base
	applyToConfig(&candidate, next)
	if err := candidate.ValidateTunables(); err != nil {
		for k := range incoming {
			if !rejected[k] {
				rejected[k] = true
				rejections = append(rejections, Rejection{Key: k, Reason: "coherence"})
			}
		}
		for _, r := range rejections {
			slog.Warn("configkv rejection", "key", r.Key, "reason", r.Reason)
			metrics.ConfigKVRejected.WithLabelValues(r.Key).Inc()
		}
		m.mu.Unlock()
		slog.Warn("configkv apply rolled back", "err", err.Error())
		return rejections
	}
	for _, r := range rejections {
		slog.Warn("configkv rejection", "key", r.Key, "reason", r.Reason)
		metrics.ConfigKVRejected.WithLabelValues(r.Key).Inc()
	}
	m.Effective = next
	m.Ptr.Store(&candidate)
	if opts.markSuccess {
		m.LastSuccess = time.Now()
		m.applied.Store(true)
		metrics.ConfigKVLastSuccess.Set(float64(m.LastSuccess.Unix()))
		metrics.ConfigKVStale.Set(0)
	}
	m.mu.Unlock()
	m.notify()
	// Only rewrite the cache when the overlay actually changed: the watcher
	// re-applies the same payload every poll and a needless rewrite races with
	// callers that remove the cache directory (tests) or read it concurrently.
	if opts.saveCache && !equalOverlay(prev, next) {
		_ = SaveAtomic(m.CachePath, next)
	}
	return rejections
}

func (m *Manager) ApplyValues(incoming map[string]string) []Rejection {
	return m.apply(parseOverlay(incoming), applyOpts{})
}

func (m *Manager) CacheSave(eff map[string]string) error { return SaveAtomic(m.CachePath, eff) }

func (m *Manager) CacheLoad() (map[string]string, error) { return LoadCache(m.CachePath) }

func (m *Manager) Validate(key, value string, current map[string]string) []Rejection {
	_, rej := Apply(mergeOverlay(m.Base, current), parseOverlay(map[string]string{key: value}))
	return rej
}

func (m *Manager) Start(ctx context.Context) {
	m.Ptr.Store(m.Base)
	if m.Disabled {
		slog.Info("configkv disabled")
		return
	}
	// load cache
	if v, err := LoadCache(m.CachePath); err == nil && v != nil {
		m.apply(parseOverlay(v), applyOpts{})
	}

	kvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, _, err := m.Client.LoadInitial(kvCtx)
	if err == nil {
		m.apply(parseOverlay(raw), applyOpts{saveCache: true, markSuccess: true})
	} else {
		metrics.ConfigKVStale.Set(1)
		slog.Warn("configkv load failed", "err", err.Error())
	}

	go m.Client.Watch(ctx, func(mm map[string]string, _ uint64) {
		m.apply(parseOverlay(mm), applyOpts{saveCache: true, markSuccess: true})
	})
}

var globalManager atomic.Pointer[Manager]

func Start(ctx context.Context) {
	cfg, err := config.Load()
	if err != nil {
		slog.Warn("config load failed", "err", err.Error())
		return
	}
	m := NewManager(cfg)
	globalManager.Store(m)
	m.Start(ctx)
}

func GlobalManager() *Manager { return globalManager.Load() }

func applyToConfig(cfg *config.Config, eff map[string]string) {
	if v, ok := eff["max_pdf_bytes"]; ok {
		var n int64
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.MaxPDFBytes = n
		}
	}
	if v, ok := eff["presign_upload_ttl_seconds"]; ok {
		var n int
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.PresignUploadTTL = time.Duration(n) * time.Second
		}
	}
	if v, ok := eff["presign_download_ttl_seconds"]; ok {
		var n int
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.PresignDownloadTTL = time.Duration(n) * time.Second
		}
	}
	if v, ok := eff["reconcile_interval_minutes"]; ok {
		var n int
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.ReconcileInterval = time.Duration(n) * time.Minute
		}
	}
	if v, ok := eff["reconcile_stuck_after_minutes"]; ok {
		var n int
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.ReconcileStuckAfter = time.Duration(n) * time.Minute
		}
	}
	if v, ok := eff["doc_upload_grace_minutes"]; ok {
		var n int
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.DocUploadGrace = time.Duration(n) * time.Minute
		}
	}
	if v, ok := eff["min_safety_age_minutes"]; ok {
		var n int
		json.Unmarshal([]byte(v), &n)
		if n > 0 {
			cfg.MinSafetyAge = time.Duration(n) * time.Minute
		}
	}
	if v, ok := eff["reconcile_dry_run"]; ok {
		var b bool
		json.Unmarshal([]byte(v), &b)
		cfg.DryRun = b
	}
}

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

func (m *Manager) ApplyValues(incoming map[string]string) []Rejection {
	// convert to map[string]any
	values := map[string]any{}
	for k, v := range incoming {
		var val any
		if err := json.Unmarshal([]byte(v), &val); err != nil {
			// keep as string? schema expects typed; try raw
			values[k] = v
			continue
		}
		values[k] = val
	}
	m.mu.Lock()
	current := map[string]any{}
	for k, v := range m.Effective {
		var val any
		if err := json.Unmarshal([]byte(v), &val); err != nil {
			current[k] = v
		} else {
			current[k] = val
		}
	}
	m.mu.Unlock()

	accepted, rejections := Apply(current, values)
	// convert accepted map[string]any back to effective strings
	accStr := map[string]string{}
	for k, v := range accepted {
		b, _ := json.Marshal(v)
		accStr[k] = string(b)
	}
	// rebuild effective: keep previous for rejected keys, apply accepted
	m.mu.Lock()
	if m.Effective == nil {
		m.Effective = map[string]string{}
	}
	for k, v := range accStr {
		m.Effective[k] = v
	}
	m.mu.Unlock()

	for _, r := range rejections {
		slog.Warn("configkv rejection", "key", r.Key, "reason", r.Reason)
		metrics.ConfigKVRejected.WithLabelValues(r.Key).Inc()
	}

	// apply to config
	cfg := *m.Base
	applyToConfig(&cfg, m.snapshotEffective())
	m.Ptr.Store(&cfg)
	return rejections
}

func (m *Manager) CacheSave(eff map[string]string) error { return SaveAtomic(m.CachePath, eff) }

func (m *Manager) CacheLoad() (map[string]string, error) { return LoadCache(m.CachePath) }

func (m *Manager) Validate(key, value string, current map[string]string) []Rejection {
	incoming := map[string]string{key: value}
	// delegate through ApplyValues-like but without mutating? spec says exported testable.
	// We'll compute rejections without mutating by using Apply directly.
	vals := map[string]any{}
	for k, v := range incoming {
		var val any
		if err := json.Unmarshal([]byte(v), &val); err != nil {
			vals[k] = v
		} else {
			vals[k] = val
		}
	}
	cur := map[string]any{}
	for k, v := range current {
		var val any
		if err := json.Unmarshal([]byte(v), &val); err != nil {
			cur[k] = v
		} else {
			cur[k] = val
		}
	}
	_, rejections := Apply(cur, vals)
	return rejections
}

func (m *Manager) Start(ctx context.Context) {
	m.Ptr.Store(m.Base)
	if m.Disabled {
		slog.Info("configkv disabled")
		return
	}
	// load cache
	if v, err := LoadCache(m.CachePath); err == nil && v != nil {
		values := map[string]any{}
		for k, val := range v {
			var parsed any
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				continue
			}
			values[k] = parsed
		}
		accepted, _ := Apply(nil, values)
		accStr := map[string]string{}
		for k, val := range accepted {
			b, _ := json.Marshal(val)
			accStr[k] = string(b)
		}
		m.mu.Lock()
		if m.Effective == nil {
			m.Effective = map[string]string{}
		}
		for k, v := range accStr {
			m.Effective[k] = v
		}
		m.mu.Unlock()
		cfg := *m.Base
		applyToConfig(&cfg, m.snapshotEffective())
		m.Ptr.Store(&cfg)
	}

	kvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, _, err := m.Client.LoadInitial(kvCtx)
	if err == nil {
		values := map[string]any{}
		for k, val := range raw {
			var parsed any
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				continue
			}
			values[k] = parsed
		}
		accepted, rejs := Apply(nil, values)
		for _, r := range rejs {
			slog.Warn("configkv rejection", "key", r.Key, "reason", r.Reason)
			metrics.ConfigKVRejected.WithLabelValues(r.Key).Inc()
		}
		m.mu.Lock()
		if m.Effective == nil {
			m.Effective = map[string]string{}
		}
		for k, val := range accepted {
			b, _ := json.Marshal(val)
			m.Effective[k] = string(b)
		}
		m.mu.Unlock()
		cfg := *m.Base
		applyToConfig(&cfg, m.snapshotEffective())
		m.Ptr.Store(&cfg)
		m.LastSuccess = time.Now()
		m.applied.Store(true)
		metrics.ConfigKVLastSuccess.Set(float64(m.LastSuccess.Unix()))
		metrics.ConfigKVStale.Set(0)
		_ = SaveAtomic(m.CachePath, m.snapshotEffective())
	} else {
		metrics.ConfigKVStale.Set(1)
		slog.Warn("configkv load failed", "err", err.Error())
	}

	go m.Client.Watch(ctx, func(mm map[string]string, i uint64) {
		values := map[string]any{}
		for k, val := range mm {
			var parsed any
			if err := json.Unmarshal([]byte(val), &parsed); err != nil {
				continue
			}
			values[k] = parsed
		}
		m.mu.Lock()
		current := map[string]any{}
		for k, v := range m.Effective {
			var val any
			if err := json.Unmarshal([]byte(v), &val); err != nil {
				current[k] = v
			} else {
				current[k] = val
			}
		}
		m.mu.Unlock()
		accepted, rejs := Apply(current, values)
		for _, r := range rejs {
			slog.Warn("configkv rejection", "key", r.Key, "reason", r.Reason)
			metrics.ConfigKVRejected.WithLabelValues(r.Key).Inc()
		}
		m.mu.Lock()
		if m.Effective == nil {
			m.Effective = map[string]string{}
		}
		for k, val := range accepted {
			b, _ := json.Marshal(val)
			m.Effective[k] = string(b)
		}
		m.mu.Unlock()
		cfg := *m.Base
		applyToConfig(&cfg, m.snapshotEffective())
		m.Ptr.Store(&cfg)
		m.LastSuccess = time.Now()
		m.applied.Store(true)
		metrics.ConfigKVLastSuccess.Set(float64(m.LastSuccess.Unix()))
		metrics.ConfigKVStale.Set(0)
		_ = SaveAtomic(m.CachePath, m.snapshotEffective())
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

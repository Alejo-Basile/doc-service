package configkv

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	start, _ := os.Getwd()
	dir := start
	for {
		if _, err := os.Stat(filepath.Join(dir, "infra", "compose", "consul", "testdata")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// The C10 fixtures are the parity oracle; a missing oracle must
			// fail loudly rather than silently skipping the contract check.
			t.Fatalf("consul testdata dir not found walking up from %s", start)
		}
		dir = parent
	}
}

func TestDecodeDropsMetadata(t *testing.T) {
	raw, _ := os.ReadFile(filepath.Join(repoRoot(t), "infra", "compose", "consul", "testdata", "doc-service.valid.json"))
	m, rej := Decode(raw)
	if len(rej) != 0 {
		t.Fatal(rej)
	}
	if _, ok := m["_expected"]; ok {
		t.Fatal("_expected present")
	}
}

func TestDecodeMalformed(t *testing.T) {
	raw, _ := os.ReadFile(filepath.Join(repoRoot(t), "infra", "compose", "consul", "testdata", "doc-service.malformed.json"))
	m, rej := Decode(raw)
	if len(m) != 0 {
		t.Fatalf("values: %v", m)
	}
	if len(rej) != 1 || rej[0].Key != "" || rej[0].Reason != "malformed_json" {
		t.Fatalf("rej: %v", rej)
	}
}

func TestApplyKeepsPreviousValue(t *testing.T) {
	raw, _ := os.ReadFile(filepath.Join(repoRoot(t), "infra", "compose", "consul", "testdata", "doc-service.valid.json"))
	incoming, _ := Decode(raw)
	cur := Defaults()
	eff, _ := Apply(cur, incoming)
	bad := map[string]any{"max_pdf_bytes": float64(209715200)}
	eff2, rej := Apply(eff, bad)
	if len(rej) != 1 || rej[0].Key != "max_pdf_bytes" || rej[0].Reason != "range" {
		t.Fatalf("rej: %v", rej)
	}
	if eff2["max_pdf_bytes"] != int64(26214400) {
		t.Fatalf("value: %v", eff2["max_pdf_bytes"])
	}
}

func TestApplyFixtures(t *testing.T) {
	root := repoRoot(t)
	manifestBytes, _ := os.ReadFile(filepath.Join(root, "infra", "compose", "consul", "testdata", "manifest.json"))
	var man struct {
		Fixtures []struct {
			File    string  `json:"file"`
			Verdict string  `json:"verdict"`
			Key     *string `json:"key"`
			Reason  *string `json:"reason"`
		} `json:"fixtures"`
	}
	json.Unmarshal(manifestBytes, &man)
	for _, f := range man.Fixtures {
		if len(f.File) < len("doc-service.") || f.File[:len("doc-service.")] != "doc-service." {
			continue
		}
		t.Run(f.File, func(t *testing.T) {
			raw, _ := os.ReadFile(filepath.Join(root, "infra", "compose", "consul", "testdata", f.File))
			incoming, decRej := Decode(raw)
			var rejections []Rejection
			if len(decRej) > 0 {
				rejections = decRej
			} else {
				_, r := Apply(Defaults(), incoming)
				rejections = r
			}
			if f.Verdict == "accepted" {
				if len(rejections) != 0 {
					t.Fatalf("expected accepted, got %v", rejections)
				}
				return
			}
			if len(rejections) == 0 {
				t.Fatal("expected rejection")
			}
			rr := rejections[0]
			expKey := ""
			if f.Key != nil {
				expKey = *f.Key
			}
			expReason := ""
			if f.Reason != nil {
				expReason = *f.Reason
			}
			if rr.Key != expKey || rr.Reason != expReason {
				t.Fatalf("got %v want %s/%s", rr, expKey, expReason)
			}
		})
	}
}

func TestDefaults(t *testing.T) {
	root := repoRoot(t)
	raw, _ := os.ReadFile(filepath.Join(root, "infra", "compose", "consul", "testdata", "doc-service.valid.json"))
	incoming, _ := Decode(raw)
	d := Defaults()
	if len(d) != len(incoming) {
		t.Fatalf("len %d vs %d", len(d), len(incoming))
	}
	for k, v := range incoming {
		if v2, ok := d[k].(int64); ok {
			if v2 != int64(v.(float64)) {
				t.Fatalf("mismatch %s", k)
			}
		} else if d[k] != v {
			t.Fatalf("mismatch %s: %v vs %v", k, d[k], v)
		}
	}
}

func TestSchemaParity(t *testing.T) {
	root := repoRoot(t)
	raw, _ := os.ReadFile(filepath.Join(root, "infra", "compose", "consul", "config", "schema.json"))
	var s map[string]map[string]map[string]any
	json.Unmarshal(raw, &s)
	sec := s["doc-service"]
	if len(sec) != len(DocServiceSchema) {
		t.Fatalf("count %d vs %d", len(sec), len(DocServiceSchema))
	}
	for k, v := range sec {
		f, ok := DocServiceSchema[k]
		if !ok {
			t.Fatalf("missing %s", k)
		}
		if f.Type != v["type"].(string) {
			t.Fatalf("type %s", k)
		}
		if f.Reload != v["reload"].(string) {
			t.Fatalf("reload %s", k)
		}
		if _, ok := v["min"]; ok {
			if !f.HasRange {
				t.Fatalf("hasrange %s", k)
			}
			if f.Min != v["min"].(float64) {
				t.Fatalf("min %s", k)
			}
			if f.Max != v["max"].(float64) {
				t.Fatalf("max %s", k)
			}
		} else {
			if f.HasRange {
				t.Fatalf("hasrange2 %s", k)
			}
		}
		def := v["default"]
		switch def.(type) {
		case bool:
			if f.Default != def {
				t.Fatalf("default bool %s", k)
			}
		case float64:
			if f.Default.(int64) != int64(def.(float64)) {
				t.Fatalf("default int %s: %v vs %v", k, f.Default, def)
			}
		}
	}
	for k := range DocServiceSchema {
		if _, ok := sec[k]; !ok {
			t.Fatalf("extra %s", k)
		}
	}
}

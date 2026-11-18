package configkv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCacheMissing(t *testing.T) {
	m, err := LoadCache(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil || m != nil {
		t.Fatalf("m=%v err=%v", m, err)
	}
}

func TestCacheCorrupt(t *testing.T) {
	p := filepath.Join(t.TempDir(), "c.json")
	os.WriteFile(p, []byte("{not json"), 0600)
	m, err := LoadCache(p)
	if err != nil || m != nil {
		t.Fatalf("m=%v err=%v", m, err)
	}
}

func TestSaveAtomicRoundTrip(t *testing.T) {
	// Assert the directory SaveAtomic creates, not t.TempDir() itself: the
	// contract is that the cache directory is created 0700 when absent.
	dir := filepath.Join(t.TempDir(), "cfg")
	p := filepath.Join(dir, "cache.json")
	vals := map[string]string{"a": "1", "b": "2"}
	if err := SaveAtomic(p, vals); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("file mode %v", info.Mode())
	}
	dinfo, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if dinfo.Mode().Perm() != 0700 {
		t.Fatalf("dir mode %v", dinfo.Mode())
	}
	got, err := LoadCache(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["a"] != "1" || got["b"] != "2" {
		t.Fatalf("got %v", got)
	}
	matches, _ := filepath.Glob(dir + "/*.tmp")
	if len(matches) != 0 {
		t.Fatalf("tmp files %v", matches)
	}
}

func TestSaveAtomicFailureLeavesNoTmp(t *testing.T) {
	dir := t.TempDir()
	targetDir := filepath.Join(dir, "ro")
	os.MkdirAll(targetDir, 0700)
	os.Chmod(targetDir, 0500)
	t.Cleanup(func() { os.Chmod(targetDir, 0700) })
	p := filepath.Join(targetDir, "cache.json")
	err := SaveAtomic(p, map[string]string{"a": "1"})
	if err == nil {
		t.Fatal("expected error")
	}
	matches, _ := filepath.Glob(targetDir + "/*.tmp")
	if len(matches) != 0 {
		t.Fatalf("tmp files %v", matches)
	}
}

package configkv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGetBlockingQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if _, ok := q["recurse"]; !ok {
			t.Error("recurse missing")
		}
		if q.Get("index") != "0" {
			t.Errorf("index=%s", q.Get("index"))
		}
		wait, err := time.ParseDuration(q.Get("wait"))
		if err != nil || wait != 5*time.Minute {
			t.Errorf("wait=%s err=%v", q.Get("wait"), err)
		}
		w.Header().Set("X-Consul-Index", "42")
		w.WriteHeader(200)
		b64 := base64.StdEncoding.EncodeToString([]byte("26214400"))
		_ = json.NewEncoder(w).Encode([]KVPair{{Key: "config/doc-service/max_pdf_bytes", Value: b64, ModifyIndex: 42}})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", "", 0)
	c.HTTPClient = srv.Client()
	pairs, idx, status, err := c.Get(context.Background(), "config/doc-service", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if status != 200 {
		t.Fatalf("status %d", status)
	}
	if idx != 42 {
		t.Fatalf("idx %d", idx)
	}
	if len(pairs) != 1 || pairs[0].Key != "config/doc-service/max_pdf_bytes" {
		t.Fatalf("pairs %+v", pairs)
	}
	m, err := PairsToMap(pairs)
	if err != nil {
		t.Fatal(err)
	}
	if m["max_pdf_bytes"] != "26214400" {
		t.Fatalf("map %v", m)
	}
}

func TestGetNotModified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(304)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", "", 0)
	c.HTTPClient = srv.Client()
	_, idx, status, err := c.Get(context.Background(), "config/doc-service", 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if status != 304 {
		t.Fatalf("status %d", status)
	}
	if idx != 7 {
		t.Fatalf("idx %d", idx)
	}
}

func TestPairsToMapBadBase64(t *testing.T) {
	_, err := PairsToMap([]KVPair{{Key: "config/doc-service/x", Value: "!!!notbase64!!!"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPairsToMapStripsPrefix(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString([]byte("v"))
	m, err := PairsToMap([]KVPair{
		{Key: "config/doc-service/x", Value: b64},
		{Key: "config/doc-service", Value: b64},
	})
	if err != nil {
		t.Fatal(err)
	}
	if m["x"] != "v" {
		t.Fatalf("map %v", m)
	}
	if _, ok := m["config/doc-service"]; ok {
		t.Fatalf("bare entry not skipped: %v", m)
	}
}

func TestLoadInitialHonorsContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", "", 0)
	c.HTTPClient = srv.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := c.LoadInitial(ctx)
	if err == nil {
		t.Fatal("expected error")
	}
	if time.Since(start) >= time.Second {
		t.Fatalf("took too long: %v", time.Since(start))
	}
}

func TestTokenHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Consul-Token") != "s3cr3t" {
			t.Errorf("token header %q", r.Header.Get("X-Consul-Token"))
		}
		w.Header().Set("X-Consul-Index", "1")
		w.WriteHeader(200)
		b64 := base64.StdEncoding.EncodeToString([]byte("val"))
		_ = json.NewEncoder(w).Encode([]KVPair{{Key: "config/doc-service/x", Value: b64, ModifyIndex: 1}})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "s3cr3t", "", 0)
	c.HTTPClient = srv.Client()
	m, _, _, err := c.Get(context.Background(), "config/doc-service", 0, 0)
	_ = m
	if err != nil {
		t.Fatal(err)
	}
	// verify token not in values via LoadInitial path
	mm, _, err := c.LoadInitial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range mm {
		if strings.Contains(v, "s3cr3t") {
			t.Fatal("token leaked into values")
		}
	}
}

func TestWatchAppliesIncreasingIndex(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		n := calls
		if n == 1 {
			w.Header().Set("X-Consul-Index", "1")
			w.WriteHeader(200)
			b64 := base64.StdEncoding.EncodeToString([]byte("A"))
			_ = json.NewEncoder(w).Encode([]KVPair{{Key: "config/doc-service/v", Value: b64, ModifyIndex: 1}})
			return
		}
		if n == 2 {
			w.Header().Set("X-Consul-Index", "2")
			w.WriteHeader(200)
			b64 := base64.StdEncoding.EncodeToString([]byte("B"))
			_ = json.NewEncoder(w).Encode([]KVPair{{Key: "config/doc-service/v", Value: b64, ModifyIndex: 2}})
			return
		}
		// block until request context done
		<-r.Context().Done()
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", "", 0)
	c.HTTPClient = srv.Client()
	c.Wait = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan map[string]string, 10)
	go c.Watch(ctx, func(m map[string]string, idx uint64) {
		ch <- m
	})
	timeout := time.After(2 * time.Second)
	var first, second string
	select {
	case m := <-ch:
		first = m["v"]
	case <-timeout:
		t.Fatal("timeout waiting first")
	}
	select {
	case m := <-ch:
		second = m["v"]
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting second")
	}
	if first != "A" || second != "B" {
		t.Fatalf("got %s %s", first, second)
	}
	cancel()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not return")
	default:
	}
}

func TestWatchContextCancelReturnsPromptly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", "", 0)
	c.HTTPClient = srv.Client()
	c.Wait = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Watch(ctx, func(m map[string]string, idx uint64) {})
		close(done)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not return")
	}
}

func TestWatchResyncsOnLowerIndex(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), queries...)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.RawQuery)
		n := len(queries)
		mu.Unlock()
		if n == 1 {
			w.Header().Set("X-Consul-Index", "10")
			w.WriteHeader(200)
			b64 := base64.StdEncoding.EncodeToString([]byte("A"))
			_ = json.NewEncoder(w).Encode([]KVPair{{Key: "config/doc-service/v", Value: b64, ModifyIndex: 10}})
			return
		}
		if n == 2 {
			w.Header().Set("X-Consul-Index", "3")
			w.WriteHeader(200)
			b64 := base64.StdEncoding.EncodeToString([]byte("B"))
			_ = json.NewEncoder(w).Encode([]KVPair{{Key: "config/doc-service/v", Value: b64, ModifyIndex: 3}})
			return
		}
		<-r.Context().Done()
		w.WriteHeader(200)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "", "", 0)
	c.HTTPClient = srv.Client()
	c.Wait = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Watch(ctx, func(m map[string]string, idx uint64) {})
	// wait for at least 3 requests
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(snapshot()) < 3 {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	// after index-3 response, next request should carry index=0
	got := snapshot()
	if len(got) < 3 {
		t.Fatalf("only %d queries", len(got))
	}
	if !strings.Contains(got[2], "index=0") {
		t.Fatalf("third query %s", got[2])
	}
}



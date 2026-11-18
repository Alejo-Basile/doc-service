package configkv

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Rejection struct {
	Key    string `json:"key"`
	Reason string `json:"reason"`
}

type KVPair struct {
	Key         string `json:"Key"`
	Value       string `json:"Value"`
	ModifyIndex uint64 `json:"ModifyIndex"`
}

type Client struct {
	Addr       string
	Token      string
	CachePath  string
	Timeout    time.Duration
	Wait       time.Duration
	HTTPClient *http.Client
}

func NewClient(addr, token, cachePath string, timeout time.Duration) *Client {
	if addr == "" {
		addr = os.Getenv("CONSUL_HTTP_ADDR")
	}
	if addr == "" {
		addr = "https://consul-agent:8501"
	}
	if token == "" {
		f := os.Getenv("CONSUL_HTTP_TOKEN_FILE")
		if f == "" {
			f = "/run/secrets/consul_doc_service_token"
		}
		if b, err := os.ReadFile(f); err == nil {
			token = strings.TrimSpace(string(b))
		}
	}
	if cachePath == "" {
		cachePath = os.Getenv("CONFIG_CACHE_PATH")
		if cachePath == "" {
			cachePath = "/var/lib/doc-service/config-cache.json"
		}
	}
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	wait := 5 * time.Minute
	cacert := os.Getenv("CONSUL_CACERT")
	if cacert == "" {
		cacert = "/etc/ssl/certs/consul-ca.pem"
	}
	tlsCfg := &tls.Config{InsecureSkipVerify: false}
	if b, err := os.ReadFile(cacert); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(b) {
			tlsCfg.RootCAs = pool
		}
	} else {
		slog.Warn("consul CA file missing, degrading", "path", cacert, "err", err.Error())
	}
	tr := &http.Transport{TLSClientConfig: tlsCfg}
	_ = filepath.Dir
	return &Client{Addr: strings.TrimRight(addr, "/"), Token: token, CachePath: cachePath, Timeout: timeout, Wait: wait, HTTPClient: &http.Client{Transport: tr, Timeout: timeout}}
}

func NewClientFromEnv() *Client { return NewClient("", "", "", 0) }

func (c *Client) Put(ctx context.Context, path string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, "PUT", c.Addr+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.Token != "" {
		req.Header.Set("X-Consul-Token", c.Token)
	}
	return c.HTTPClient.Do(req)
}

func (c *Client) Get(ctx context.Context, key string, index uint64, wait time.Duration) ([]KVPair, uint64, int, error) {
	if wait == 0 {
		wait = c.Wait
		if wait == 0 {
			wait = 5 * time.Minute
		}
	}
	url := fmt.Sprintf("%s/v1/kv/%s?recurse&index=%d&wait=%s", c.Addr, strings.TrimPrefix(key, "/"), index, wait.String())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, index, 0, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, index, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 304 {
		return nil, index, 304, nil
	}
	if resp.StatusCode != 200 {
		return nil, index, resp.StatusCode, fmt.Errorf("consul kv get %s: %d", key, resp.StatusCode)
	}
	var pairs []KVPair
	if err := json.Unmarshal(body, &pairs); err != nil {
		return nil, index, resp.StatusCode, err
	}
	newIdx := index
	if v := resp.Header.Get("X-Consul-Index"); v != "" {
		var n uint64
		fmt.Sscan(v, &n)
		if n > 0 {
			newIdx = n
		}
	}
	return pairs, newIdx, resp.StatusCode, nil
}

func (c *Client) LoadInitial(ctx context.Context) (map[string]string, uint64, error) {
	pairs, idx, _, err := c.Get(ctx, "config/doc-service", 0, 0)
	if err != nil {
		return nil, 0, err
	}
	m, err := PairsToMap(pairs)
	if err != nil {
		return nil, 0, err
	}
	return m, idx, nil
}

func PairsToMap(pairs []KVPair) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range pairs {
		k := strings.TrimPrefix(p.Key, "config/doc-service/")
		if k == "config/doc-service" || k == "" {
			continue
		}
		dec, err := base64.StdEncoding.DecodeString(p.Value)
		if err != nil {
			return nil, err
		}
		out[k] = string(dec)
	}
	return out, nil
}

func (c *Client) Watch(ctx context.Context, onChange func(map[string]string, uint64)) {
	idx := uint64(0)
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		pairs, newIdx, status, err := c.Get(ctx, "config/doc-service", idx, c.Wait)
		if err != nil || status >= 500 {
			jitter := time.Duration(rand.Int63n(int64(backoff/4) + 1))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff + jitter):
			}
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			continue
		}
		backoff = time.Second
		if status == 304 {
			continue
		}
		if idx != 0 && newIdx < idx {
			idx = 0
			continue
		}
		if newIdx > idx || idx == 0 {
			idx = newIdx
			m, _ := PairsToMap(pairs)
			onChange(m, idx)
		}
	}
}

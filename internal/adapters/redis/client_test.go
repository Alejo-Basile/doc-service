package redis

import "testing"

func TestNewClient_DireccionFija(t *testing.T) {
	c := NewClient(ClientConfig{Addr: "localhost:6379", Password: "x"})
	if c == nil {
		t.Fatal("NewClient devolvió nil en modo dirección fija")
	}
	_ = c.Close()
}

func TestNewClient_Sentinel(t *testing.T) {
	c := NewClient(ClientConfig{
		SentinelAddrs: []string{"s1:26379", "s2:26379"},
		MasterName:    "mymaster",
		Password:      "x",
	})
	if c == nil {
		t.Fatal("NewClient devolvió nil en modo Sentinel")
	}
	_ = c.Close()
}

func TestNewWorkQueueFromClient_UsaElClienteDado(t *testing.T) {
	client := NewClient(ClientConfig{Addr: "localhost:6379"})
	defer client.Close()

	cfg := DefaultWorkQueueConfig()
	cfg.StreamKey = "stream:test"
	q := NewWorkQueueFromClient(client, cfg)
	if q == nil {
		t.Fatal("NewWorkQueueFromClient devolvió nil")
	}
	if q.client != client {
		t.Error("el WorkQueue no reutilizó el cliente pasado")
	}
	if q.streamKey != "stream:test" {
		t.Errorf("streamKey esperado 'stream:test', got %q", q.streamKey)
	}
}

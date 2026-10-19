// Package redis implementa el adaptador de infraestructura para Redis.
package redis

import "github.com/redis/go-redis/v9"

// ClientConfig parametriza la creación del cliente Redis.
//
// Dos modos posibles (SPEC §3.5), mutuamente excluyentes:
//   - Dirección fija: Addr (+ Password).
//   - Sentinel: SentinelAddrs + MasterName. El cliente resuelve el primario
//     y sigue automáticamente un failover.
type ClientConfig struct {
	Addr          string
	Password      string
	SentinelAddrs []string
	MasterName    string
}

// NewClient construye un cliente Redis. Si SentinelAddrs está definido usa un
// cliente de failover (Sentinel); en caso contrario, un cliente a dirección fija.
func NewClient(cfg ClientConfig) *redis.Client {
	if len(cfg.SentinelAddrs) > 0 {
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.SentinelAddrs,
			Password:      cfg.Password,
		})
	}
	return redis.NewClient(&redis.Options{Addr: cfg.Addr, Password: cfg.Password})
}

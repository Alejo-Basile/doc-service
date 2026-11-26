// Package consulreg registra y desregistra cada réplica de doc-service en el
// agente local de Consul (S11-P1-C14).
//
// El registro es no bloqueante: un fallo de Consul nunca impide que el
// servicio arranque ni que atienda tráfico (plan S11 §1, garantía 5). El
// desregistro en el apagado tiene un presupuesto propio de 2 s, independiente
// del drenaje HTTP, y su fallo no cambia el exit code: la red de seguridad es
// el DeregisterCriticalServiceAfter del propio health check.
//
// El plano de tráfico no depende de Consul: Traefik sigue con provider `file`
// y `routing.yml` (plan S11 §1, garantía 6).
package consulreg

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/configkv"
	"github.com/Alejo-Basile/doc-service/internal/metrics"
)

const (
	// defaultAgentAddr es el agente cliente por host, no el server: el agente
	// es el punto de registro de las apps (plan S11 §4.1).
	defaultAgentAddr = "https://consul-agent:8501"
	// defaultServiceName es el nombre lógico del servicio; el nombre en el
	// catálogo de Consul es "svc-" + ServiceName.
	defaultServiceName = "doc-service"
	// healthPath es el mismo endpoint que usa el healthcheck de compose, de
	// modo que Docker y Consul coinciden en el veredicto.
	healthPath = "/healthz"

	checkInterval = "10s"
	checkTimeout  = "3s"
	// checkDeregister limpia el catálogo cuando el desregistro ordenado no
	// ocurre (SIGKILL, OOM). Es la única defensa contra un registro fantasma.
	checkDeregister = "1m"

	// agentProbePort solo se usa para resolver la IP local con un dial UDP:
	// no abre conexión ni envía paquetes.
	agentProbePort = "8501"

	// retryInitialDelay y retryMaxDelay acotan el reintento del registro
	// cuando el agente no está disponible al arrancar (por ejemplo, Consul
	// levantándose después que las apps). Sin este reintento la réplica queda
	// fuera del catálogo hasta que alguien la reinicie.
	retryInitialDelay = 5 * time.Second
	retryMaxDelay     = 60 * time.Second
	registerAttempt   = 5 * time.Second

	reassertInterval = 30 * time.Second
)

// Config reúne los parámetros de registro. AddressProbe es inyectable para
// que los tests no dependan de la red del entorno.
type Config struct {
	ServiceName  string
	AgentAddr    string
	ServiceAddr  string
	HTTPPort     int
	Disabled     bool
	AddressProbe func(target string) (string, error)
	// RetryInitial y RetryMax acotan el backoff del reintento de registro; en
	// cero usan los defaults (5 s y 60 s). Los tests los reducen.
	RetryInitial time.Duration
	RetryMax     time.Duration
	// ReassertEvery es el intervalo entre re-registros periódicos. En cero
	// usa reassertInterval (30 s).
	ReassertEvery time.Duration
}

// Registrar publica esta réplica en el catálogo de Consul.
type Registrar struct {
	cfg    Config
	client *configkv.Client
	id     string

	mu      sync.Mutex
	address string
}

// New construye un Registrar con un cliente HTTP ya resuelto. Se usa en tests
// y permite reutilizar el transporte TLS del cliente KV (C11).
func New(cfg Config, client *configkv.Client) *Registrar {
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultServiceName
	}
	if cfg.AgentAddr == "" {
		cfg.AgentAddr = defaultAgentAddr
	}
	if cfg.AddressProbe == nil {
		cfg.AddressProbe = deriveLocalAddress
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = retryInitialDelay
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = retryMaxDelay
	}
	if cfg.ReassertEvery <= 0 {
		cfg.ReassertEvery = reassertInterval
	}
	return &Registrar{cfg: cfg, client: client, id: serviceID()}
}

// NewFromEnv arma el Registrar desde variables de entorno, reutilizando el
// cliente de configkv (misma CA y mismo token por archivo que el watcher KV).
func NewFromEnv(httpPort int) *Registrar {
	agentAddr := os.Getenv("CONSUL_REGISTRATION_ADDR")
	if agentAddr == "" {
		agentAddr = defaultAgentAddr
	}
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	// El token sale del mismo secreto que el KV (CONSUL_HTTP_TOKEN_FILE o el
	// default /run/secrets/consul_doc_service_token): una sola credencial por
	// servicio, nunca inline.
	client := configkv.NewClient(agentAddr, "", "", 0)

	reassertEvery := time.Duration(0)
	if raw := os.Getenv("CONSUL_REASSERT_SECS"); raw != "" {
		// parse positive integer
		var secs int
		if _, err := fmt.Sscanf(raw, "%d", &secs); err != nil || secs <= 0 {
			slog.Warn("valor inválido de CONSUL_REASSERT_SECS, usando default",
				"value", raw, "default", reassertInterval.String())
		} else {
			reassertEvery = time.Duration(secs) * time.Second
		}
	}

	return New(Config{
		ServiceName:   serviceName,
		AgentAddr:     agentAddr,
		ServiceAddr:   strings.TrimSpace(os.Getenv("CONSUL_SERVICE_ADDR")),
		HTTPPort:      httpPort,
		Disabled:      os.Getenv("CONSUL_REGISTRATION_DISABLED") == "true",
		ReassertEvery: reassertEvery,
	}, client)
}

// ID es el identificador único de la réplica en el catálogo. Se deriva del
// hostname del contenedor, único incluso con `--scale` (compose no fija
// container_name para doc-service).
func (r *Registrar) ID() string { return r.id }

// Enabled informa si el registro está activo. Con
// CONSUL_REGISTRATION_DISABLED=true el servicio arranca sin tocar Consul
// (rollback de la tarea, §9).
func (r *Registrar) Enabled() bool { return !r.cfg.Disabled }

// Address devuelve la dirección publicada. Si Config.ServiceAddr está definido
// se usa verbatim y nunca se llama al probe. De lo contrario se resuelve con el
// probe y se cachea solo cuando tiene éxito.
func (r *Registrar) Address() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cfg.ServiceAddr != "" {
		return r.cfg.ServiceAddr, nil
	}
	if r.address != "" {
		return r.address, nil
	}
	addr, err := r.cfg.AddressProbe(r.cfg.AgentAddr)
	if err != nil {
		return "", fmt.Errorf("consul register: no se pudo resolver la dirección: %w", err)
	}
	if strings.TrimSpace(addr) == "" {
		return "", fmt.Errorf("consul register: el probe no devolvió una dirección")
	}
	r.address = strings.TrimSpace(addr)
	return r.address, nil
}

// registerPayload es el body de PUT /v1/agent/service/register. Los nombres
// de campo son los de la API de Consul y no se traducen.
type registerPayload struct {
	ID      string   `json:"ID"`
	Name    string   `json:"Name"`
	Address string   `json:"Address"`
	Port    int      `json:"Port"`
	Tags    []string `json:"Tags"`
	Check   check    `json:"Check"`
}

type check struct {
	HTTP                           string `json:"HTTP"`
	Interval                       string `json:"Interval"`
	Timeout                        string `json:"Timeout"`
	DeregisterCriticalServiceAfter string `json:"DeregisterCriticalServiceAfter"`
}

// payload arma el objeto de registro de esta réplica.
func (r *Registrar) payload() (registerPayload, error) {
	addr, err := r.Address()
	if err != nil {
		return registerPayload{}, err
	}
	return registerPayload{
		ID:      r.id,
		Name:    "svc-" + r.cfg.ServiceName,
		Address: addr,
		Port:    r.cfg.HTTPPort,
		Tags:    []string{"http", "s11", r.cfg.ServiceName, "metrics_path=/metrics"},
		Check: check{
			HTTP:                           fmt.Sprintf("http://%s:%d%s", addr, r.cfg.HTTPPort, healthPath),
			Interval:                       checkInterval,
			Timeout:                        checkTimeout,
			DeregisterCriticalServiceAfter: checkDeregister,
		},
	}, nil
}

// Register publica la réplica. Un fallo no aborta el arranque: el llamador
// loguea y sigue sirviendo (plan S11 §1, garantía 5).
func (r *Registrar) Register(ctx context.Context) error {
	if !r.Enabled() {
		return nil
	}
	p, err := r.payload()
	if err != nil {
		metrics.ConsulRegistrationFailed.Inc()
		return err
	}
	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("consul register: marshal payload: %w", err)
	}
	status, err := r.client.Put(ctx, "/v1/agent/service/register", body)
	if err != nil {
		metrics.ConsulRegistrationFailed.Inc()
		return fmt.Errorf("consul register: %w", err)
	}
	if status < 200 || status >= 300 {
		metrics.ConsulRegistrationFailed.Inc()
		return fmt.Errorf("consul register: status %d", status)
	}
	return nil
}

// RegisterWithRetry publica la réplica reintentando con backoff hasta lograrlo
// o hasta que el contexto se cancele. Un Consul que arranca después que las
// apps, o un agente que se reinicia, terminan con las réplicas registradas sin
// intervención manual: el reintento no bloquea el arranque ni el servicio.
func (r *Registrar) RegisterWithRetry(ctx context.Context) {
	r.registerWithRetry(ctx, false)
}

// registerWithRetry es el bucle de reintento. Con quiet=true el éxito se
// registra a nivel debug: la re-afirmación periódica no debe inundar los logs.
func (r *Registrar) registerWithRetry(ctx context.Context, quiet bool) {
	if !r.Enabled() {
		return
	}
	backoff := r.cfg.RetryInitial
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, registerAttempt)
		err := r.Register(attemptCtx)
		cancel()
		if err == nil {
			addr, _ := r.Address()
			if quiet {
				slog.Debug("re-registrado en Consul",
					"id", r.ID(), "address", addr, "port", r.cfg.HTTPPort)
			} else {
				slog.Info("registrado en Consul",
					"id", r.ID(), "address", addr, "port", r.cfg.HTTPPort)
			}
			return
		}
		addr, _ := r.Address()
		slog.Warn("registro en Consul fallido, el servicio sigue operando y se reintentará",
			"id", r.ID(), "address", addr, "error", err.Error(), "retry_in", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < r.cfg.RetryMax {
			backoff *= 2
			if backoff > r.cfg.RetryMax {
				backoff = r.cfg.RetryMax
			}
		}
	}
}

// KeepRegistered re-afirma el registro periódicamente para que, si el agente
// de Consul se reinicia y pierde los servicios registrados por API, la réplica
// vuelva a aparecer sin reinicio manual.
func (r *Registrar) KeepRegistered(ctx context.Context) {
	if !r.Enabled() {
		return
	}
	first := true
	for {
		r.registerWithRetry(ctx, !first)
		first = false
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.cfg.ReassertEvery):
		}
	}
}

// Deregister retira la réplica del catálogo. Si falla o se agota el
// presupuesto, el DeregisterCriticalServiceAfter limpia la entrada.
func (r *Registrar) Deregister(ctx context.Context) error {
	if !r.Enabled() {
		return nil
	}
	status, err := r.client.Put(ctx, "/v1/agent/service/deregister/"+url.PathEscape(r.id), nil)
	if err != nil {
		metrics.ConsulRegistrationFailed.Inc()
		return fmt.Errorf("consul deregister: %w", err)
	}
	if status < 200 || status >= 300 {
		metrics.ConsulRegistrationFailed.Inc()
		return fmt.Errorf("consul deregister: status %d", status)
	}
	return nil
}

// serviceID deriva el ID único de la réplica del hostname del contenedor.
func serviceID() string {
	return defaultServiceName + "-" + hostname()
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}

// deriveLocalAddress resuelve la IPv4 local que el agente de Consul puede
// alcanzar. No se publica el hostname porque el DNS embebido de Docker no lo
// resuelve para otros contenedores (verificado: el agente resuelve
// "infra-doc-service-1" pero no el hostname corto del contenedor), y un check
// contra un nombre irresoluble quedaría crítico para siempre.
func deriveLocalAddress(target string) (string, error) {
	host := hostFromAddr(target)
	port := agentProbePort
	if u, err := url.Parse(target); err == nil && u.Port() != "" {
		port = u.Port()
	}
	if port == "" {
		port = agentProbePort
	}
	conn, err := net.Dial("udp", net.JoinHostPort(host, port))
	if err != nil {
		return "", fmt.Errorf("consul register: no se pudo resolver la IP local: %w", err)
	}
	defer conn.Close()
	udpAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || udpAddr.IP == nil || udpAddr.IP.IsLoopback() || udpAddr.IP.To4() == nil {
		return "", fmt.Errorf("consul register: ninguna IPv4 utilizable alcanza al agente")
	}
	return udpAddr.IP.String(), nil
}

// hostFromAddr extrae el host de una dirección de Consul ("https://h:8501").
func hostFromAddr(raw string) string {
	if raw == "" {
		raw = defaultAgentAddr
	}
	if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
		return u.Hostname()
	}
	// Tolerancia a valores sin esquema ("consul-agent:8501").
	if i := strings.Index(raw, ":"); i >= 0 {
		return raw[:i]
	}
	return raw
}

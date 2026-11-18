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
)

// Config reúne los parámetros de registro. AddressProbe es inyectable para
// que los tests no dependan de la red del entorno.
type Config struct {
	ServiceName  string
	AgentAddr    string
	HTTPPort     int
	Disabled     bool
	AddressProbe func(target string) (string, error)
	// RetryInitial y RetryMax acotan el backoff del reintento de registro; en
	// cero usan los defaults (5 s y 60 s). Los tests los reducen.
	RetryInitial time.Duration
	RetryMax     time.Duration
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
	return New(Config{
		ServiceName: serviceName,
		AgentAddr:   agentAddr,
		HTTPPort:    httpPort,
		Disabled:    os.Getenv("CONSUL_REGISTRATION_DISABLED") == "true",
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

// Address devuelve la dirección publicada, resolviéndola una sola vez.
func (r *Registrar) Address() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.address == "" {
		addr, err := r.cfg.AddressProbe(r.cfg.AgentAddr)
		if err != nil || addr == "" {
			// Último recurso: el hostname. Puede no resolver desde el agente,
			// pero es preferible a no publicar nada.
			addr = hostname()
		}
		r.address = addr
	}
	return r.address
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
func (r *Registrar) payload() registerPayload {
	addr := r.Address()
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
	}
}

// Register publica la réplica. Un fallo no aborta el arranque: el llamador
// loguea y sigue sirviendo (plan S11 §1, garantía 5).
func (r *Registrar) Register(ctx context.Context) error {
	if !r.Enabled() {
		return nil
	}
	body, err := json.Marshal(r.payload())
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
	if !r.Enabled() {
		return
	}
	backoff := r.cfg.RetryInitial
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, registerAttempt)
		err := r.Register(attemptCtx)
		cancel()
		if err == nil {
			slog.Info("registrado en Consul",
				"id", r.ID(), "address", r.Address(), "port", r.cfg.HTTPPort)
			return
		}
		slog.Warn("registro en Consul fallido, el servicio sigue operando y se reintentará",
			"id", r.ID(), "address", r.Address(), "error", err.Error(), "retry_in", backoff.String())
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
	// Dial UDP: fija la ruta de salida sin enviar paquetes ni esperar
	// handshake; LocalAddr es la IP de la interfaz que alcanza al agente.
	conn, err := net.Dial("udp", net.JoinHostPort(host, agentProbePort))
	if err == nil {
		defer conn.Close()
		if udpAddr, ok := conn.LocalAddr().(*net.UDPAddr); ok &&
			udpAddr.IP != nil && !udpAddr.IP.IsLoopback() {
			return udpAddr.IP.String(), nil
		}
	}
	return firstGlobalIPv4()
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

// firstGlobalIPv4 es el fallback cuando el dial no puede resolver el destino.
func firstGlobalIPv4() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP
			if ip.To4() != nil && !ip.IsLoopback() && ip.IsGlobalUnicast() {
				return ip.String(), nil
			}
		}
	}
	return "", fmt.Errorf("consul register: ninguna IPv4 utilizable")
}

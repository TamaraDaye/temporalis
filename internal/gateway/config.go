package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultShutdownTimeout = 30 * time.Second

	defaultReadHeaderTimeout      = 10 * time.Second
	defaultIdleTimeout            = 120 * time.Second
	defaultMaxHeaderBytes    uint = 1 << 20 // 1 MiB

	defaultDialTimeout              = 5 * time.Second
	defaultResponseHeaderTimeout    = 30 * time.Second
	defaultIdleConnTimeout          = 90 * time.Second
	defaultMaxIdleConns        uint = 100
	defaultMaxIdleConnsPerHost uint = 32

	defaultStrategy                = "round_robin"
	defaultHealthInterval          = 10 * time.Second
	defaultHealthTimeout           = 2 * time.Second
	defaultHealthyThreshold   uint = 2
	defaultUnhealthyThreshold uint = 3
)

var validStrategies = map[string]bool{
	"round_robin": true,
	"random":      true,
	"least_conn":  true,
}

type Config struct {
	Gateway GatewayConfig      `yaml:"gateway"`
	Routes  []Route            `yaml:"routes"`
	Service map[string]Service `yaml:"services"`
}

type GatewayConfig struct {
	ListenAddr      string          `yaml:"listen_addr"`
	ShutdownTimeout time.Duration   `yaml:"shutdown_timeout"`
	ServerConfig    ServerConfig    `yaml:"server"`
	TransportConfig TransportConfig `yaml:"transport"`
}

type ServerConfig struct {
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxHeaderBytes    uint          `yaml:"max_header_bytes"`
}

type TransportConfig struct {
	DialTimeout           time.Duration `yaml:"dial_timeout"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout"`
	MaxIdleConns          uint          `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   uint          `yaml:"max_idle_conns_per_host"`
}

type Route struct {
	Name        string     `yaml:"name"`
	Match       RouteMatch `yaml:"match"`
	Service     string     `yaml:"service"`
	StripPrefix bool       `yaml:"strip_prefix"`
}

type RouteMatch struct {
	Host       string `yaml:"host"`
	PathPrefix string `yaml:"path_prefix"`
}

type Service struct {
	LoadBalancing LoadBalancing `yaml:"load_balancing"`
	HealthCheck   HealthCheck   `yaml:"health_check"`
	Instances     []Instance    `yaml:"instances"`
}

type LoadBalancing struct {
	Strategy string `yaml:"strategy"`
	Fallback string `yaml:"fallback"`
}

type HealthCheck struct {
	Enabled            bool          `yaml:"enabled"`
	Path               string        `yaml:"path"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	UnhealthyThreshold uint          `yaml:"unhealthy_threshold"`
	HealthyThreshold   uint          `yaml:"healthy_threshold"`
}

type Instance struct {
	ID  string `yaml:"id"`
	URL string `yaml:"url"`
}

type Options struct {
	Logger *slog.Logger
	Strict bool

	// DynamicDiscovery relaxes static instance requirements once ViewServer
	// supplies upstream endpoints at runtime. Leave false for static bring-up.
	DynamicDiscovery bool
}

func Load(path string, opts Options) (*Config, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config %q: %w", path, err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)

	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("config %q is empty", path)
		}
		return nil, fmt.Errorf("failed to parse config %q: %w", path, err)
	}

	applyDefaults(&cfg, log)

	warnings, err := cfg.Validate(opts)
	if opts.Strict {
		for _, w := range warnings {
			err = errors.Join(err, errors.New("warning (strict mode): "+w))
		}
	} else {
		for _, w := range warnings {
			log.Warn("config warning", "detail", w)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("invalid config %q:\n%w", path, err)
	}

	return &cfg, nil
}

func setDefault[T comparable](log *slog.Logger, field string, v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
		log.Debug("config: field not set, using default", "field", field, "value", def)
	}
}

func applyDefaults(c *Config, log *slog.Logger) {
	g := &c.Gateway
	setDefault(log, "gateway.shutdown_timeout", &g.ShutdownTimeout, defaultShutdownTimeout)

	s := &g.ServerConfig
	setDefault(log, "gateway.server.read_header_timeout", &s.ReadHeaderTimeout, defaultReadHeaderTimeout)
	setDefault(log, "gateway.server.idle_timeout", &s.IdleTimeout, defaultIdleTimeout)
	setDefault(log, "gateway.server.max_header_bytes", &s.MaxHeaderBytes, defaultMaxHeaderBytes)

	t := &g.TransportConfig
	setDefault(log, "gateway.transport.dial_timeout", &t.DialTimeout, defaultDialTimeout)
	setDefault(log, "gateway.transport.response_header_timeout", &t.ResponseHeaderTimeout, defaultResponseHeaderTimeout)
	setDefault(log, "gateway.transport.idle_conn_timeout", &t.IdleConnTimeout, defaultIdleConnTimeout)
	setDefault(log, "gateway.transport.max_idle_conns", &t.MaxIdleConns, defaultMaxIdleConns)
	setDefault(log, "gateway.transport.max_idle_conns_per_host", &t.MaxIdleConnsPerHost, min(defaultMaxIdleConnsPerHost, t.MaxIdleConns))

	for name, svc := range c.Service {
		p := "services." + name

		setDefault(log, p+".load_balancing.strategy", &svc.LoadBalancing.Strategy, defaultStrategy)

		if svc.HealthCheck.Enabled {
			hc := &svc.HealthCheck
			setDefault(log, p+".health_check.interval", &hc.Interval, defaultHealthInterval)
			setDefault(log, p+".health_check.timeout", &hc.Timeout, defaultHealthTimeout)
			setDefault(log, p+".health_check.healthy_threshold", &hc.HealthyThreshold, defaultHealthyThreshold)
			setDefault(log, p+".health_check.unhealthy_threshold", &hc.UnhealthyThreshold, defaultUnhealthyThreshold)
		}

		c.Service[name] = svc
	}
}

func (c *Config) Validate(opts Options) (warnings []string, err error) {
	if c == nil {
		return nil, errors.New("configuration is nil")
	}

	var errs []error
	addErr := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}
	addWarn := func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}

	checkDuration := func(name string, d time.Duration) {
		if d <= 0 {
			addErr("%s: must be > 0", name)
		} else if d < time.Millisecond {
			addWarn("%s: %s is unusually small (missing unit suffix like 's' or 'ms'?)", name, d)
		}
	}

	// 1. Gateway, Server & Transport
	if err := validateHostPort(c.Gateway.ListenAddr); err != nil {
		addErr("gateway.listen_addr: %v", err)
	}
	checkDuration("gateway.shutdown_timeout", c.Gateway.ShutdownTimeout)
	checkDuration("gateway.server.read_header_timeout", c.Gateway.ServerConfig.ReadHeaderTimeout)
	checkDuration("gateway.server.idle_timeout", c.Gateway.ServerConfig.IdleTimeout)
	if c.Gateway.ServerConfig.MaxHeaderBytes < 4096 {
		addWarn("gateway.server.max_header_bytes: %d bytes is smaller than 4 KiB and may reject normal requests", c.Gateway.ServerConfig.MaxHeaderBytes)
	}

	checkDuration("gateway.transport.dial_timeout", c.Gateway.TransportConfig.DialTimeout)
	checkDuration("gateway.transport.response_header_timeout", c.Gateway.TransportConfig.ResponseHeaderTimeout)
	checkDuration("gateway.transport.idle_conn_timeout", c.Gateway.TransportConfig.IdleConnTimeout)
	if c.Gateway.TransportConfig.MaxIdleConnsPerHost > c.Gateway.TransportConfig.MaxIdleConns {
		addWarn(
			"gateway.transport.max_idle_conns_per_host (%d) exceeds max_idle_conns (%d); net/http will clamp per-host idle connections",
			c.Gateway.TransportConfig.MaxIdleConnsPerHost,
			c.Gateway.TransportConfig.MaxIdleConns,
		)
	}

	// 2. Routes
	if len(c.Routes) == 0 {
		addErr("routes: at least one route is required")
	}
	routeNames := make(map[string]bool, len(c.Routes))
	routeMatches := make(map[string]bool, len(c.Routes))
	referencedServices := make(map[string]bool, len(c.Routes))

	for i, rt := range c.Routes {
		if rt.Name == "" {
			addErr("routes[%d].name: required", i)
		} else if routeNames[rt.Name] {
			addErr("routes[%d].name: duplicate %q", i, rt.Name)
		} else {
			routeNames[rt.Name] = true
		}

		host, prefix := strings.ToLower(rt.Match.Host), rt.Match.PathPrefix
		if host == "" && prefix == "" {
			addErr("routes[%d].match: host or path_prefix required", i)
		} else {
			key := host + "|" + prefix
			if routeMatches[key] {
				addErr("routes[%d].match: duplicate match (%s, %s)", i, host, prefix)
			}
			routeMatches[key] = true

			if host == "" && prefix == "/" {
				addWarn("routes[%d] (%q): global catch-all '/' without a host matches all unmatched traffic", i, rt.Name)
			}
		}

		if prefix != "" && !strings.HasPrefix(prefix, "/") {
			addErr("routes[%d].match.path_prefix: %q must start with '/'", i, prefix)
		}
		if rt.Service == "" {
			addErr("routes[%d].service: required", i)
		} else if _, ok := c.Service[rt.Service]; !ok {
			addErr("routes[%d].service: unknown service %q", i, rt.Service)
		} else {
			referencedServices[rt.Service] = true
		}
	}

	// 3. Services
	if len(c.Service) == 0 {
		addErr("services: at least one service is required")
	}
	for name, svc := range c.Service {
		p := "services." + name

		if !referencedServices[name] {
			addWarn("%s: service is defined but not referenced by any route", p)
		}

		if !validStrategies[svc.LoadBalancing.Strategy] {
			addErr("%s.load_balancing.strategy: unknown %q", p, svc.LoadBalancing.Strategy)
		}
		if fb := svc.LoadBalancing.Fallback; fb != "" {
			if !validStrategies[fb] {
				addErr("%s.load_balancing.fallback: unknown %q", p, fb)
			} else if fb == svc.LoadBalancing.Strategy {
				addWarn("%s.load_balancing.fallback: matches primary strategy %q", p, fb)
			}
		}

		hc := svc.HealthCheck
		if hc.Enabled {
			if !strings.HasPrefix(hc.Path, "/") {
				addErr("%s.health_check.path: must start with '/'", p)
			}
			checkDuration(p+".health_check.interval", hc.Interval)
			checkDuration(p+".health_check.timeout", hc.Timeout)
			if hc.Timeout > hc.Interval {
				addErr("%s.health_check: timeout (%s) exceeds interval (%s)", p, hc.Timeout, hc.Interval)
			} else if hc.Timeout == hc.Interval {
				addWarn("%s.health_check: timeout (%s) equals interval (%s); health probes may overlap", p, hc.Timeout, hc.Interval)
			}
		} else {
			addWarn("%s.health_check: disabled; unhealthy instances will not be automatically ejected", p)
			if hc.Path != "" || hc.Interval != 0 || hc.Timeout != 0 {
				addWarn("%s.health_check: path/interval/timeout configured while enabled is false", p)
			}
		}

		if !opts.DynamicDiscovery {
			if len(svc.Instances) == 0 {
				addErr("%s.instances: at least one static instance is required when dynamic discovery is disabled", p)
			} else if len(svc.Instances) == 1 {
				addWarn("%s.instances: only 1 static instance configured (no failover redundancy)", p)
			}
		} else if len(svc.Instances) > 0 {
			addWarn("%s.instances: static instances configured alongside dynamic discovery; verify whether they are intended as bootstrap seeds", p)
		}

		if err := ValidateInstances(p+".instances", svc.Instances); err != nil {
			errs = append(errs, err)
		}
	}

	return warnings, errors.Join(errs...)
}

// ValidateInstances validates a slice of upstream instances. Exported so both
// static config validation and future ViewServer updates share identical checks.
func ValidateInstances(prefix string, instances []Instance) error {
	var errs []error
	seenIDs := make(map[string]bool, len(instances))

	for i, inst := range instances {
		if inst.ID == "" || seenIDs[inst.ID] {
			errs = append(errs, fmt.Errorf("%s[%d].id: missing or duplicate %q", prefix, i, inst.ID))
		} else {
			seenIDs[inst.ID] = true
		}

		if err := validateUpstreamURL(inst.URL); err != nil {
			errs = append(errs, fmt.Errorf("%s[%d].url: %w", prefix, i, err))
		}
	}

	return errors.Join(errs...)
}

func validateHostPort(addr string) error {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	p, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || p == 0 {
		return fmt.Errorf("invalid port %q", portStr)
	}
	return nil
}

func validateUpstreamURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("must be a valid http/https URL with host, got %q", raw)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("must not contain path, query, or fragment")
	}
	return nil
}

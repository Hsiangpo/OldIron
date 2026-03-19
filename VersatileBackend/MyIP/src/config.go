package myipsvc

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr           string
	Strategy       string
	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
	Cooldown       time.Duration
	MaxCooldown    time.Duration
	UpstreamURLs   []string
}

func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		Addr:           getenvWithFallback("MYIP_SERVICE_ADDR", "PROXY_POOL_SERVICE_ADDR", ":17897"),
		Strategy:       getenvWithFallback("MYIP_STRATEGY", "PROXY_POOL_STRATEGY", "host-hash"),
		ConnectTimeout: time.Duration(getenvIntWithFallback("MYIP_CONNECT_TIMEOUT_SECONDS", "PROXY_POOL_CONNECT_TIMEOUT_SECONDS", 15)) * time.Second,
		IdleTimeout:    time.Duration(getenvIntWithFallback("MYIP_IDLE_TIMEOUT_SECONDS", "PROXY_POOL_IDLE_TIMEOUT_SECONDS", 120)) * time.Second,
		Cooldown:       time.Duration(getenvIntWithFallback("MYIP_COOLDOWN_SECONDS", "PROXY_POOL_COOLDOWN_SECONDS", 30)) * time.Second,
		MaxCooldown:    time.Duration(getenvIntWithFallback("MYIP_MAX_COOLDOWN_SECONDS", "PROXY_POOL_MAX_COOLDOWN_SECONDS", 300)) * time.Second,
		UpstreamURLs:   loadUpstreamURLs(),
	}
	if len(cfg.UpstreamURLs) == 0 {
		return Config{}, errors.New("proxy pool upstreams are empty")
	}
	if cfg.Strategy != "host-hash" && cfg.Strategy != "round-robin" {
		cfg.Strategy = "host-hash"
	}
	if cfg.MaxCooldown < cfg.Cooldown {
		cfg.MaxCooldown = cfg.Cooldown
	}
	return cfg, nil
}

func loadUpstreamURLs() []string {
	values := splitProxyValues(getenvWithFallback("MYIP_UPSTREAMS", "PROXY_POOL_UPSTREAMS", ""))
	filePath := strings.TrimSpace(getenvWithFallback("MYIP_UPSTREAMS_FILE", "PROXY_POOL_UPSTREAMS_FILE", ""))
	if filePath == "" {
		return values
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return values
	}
	return append(values, splitProxyValues(string(data))...)
}

func splitProxyValues(raw string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '\n' || r == '\r' || r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func getenv(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value != "" {
		return value
	}
	return fallback
}

func getenvWithFallback(primary string, secondary string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(primary))
	if value != "" {
		return value
	}
	value = strings.TrimSpace(os.Getenv(secondary))
	if value != "" {
		return value
	}
	return fallback
}

func getenvInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func getenvIntWithFallback(primary string, secondary string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(primary))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv(secondary))
	}
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

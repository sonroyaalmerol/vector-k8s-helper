// Package config handles application configuration loading and validation.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration.
type Config struct {
	Namespace        string
	ConfigMapName    string
	ConfigMapKey     string
	ScrapeInterval   time.Duration
	ScrapeTimeout    time.Duration
	HonorLabels      bool
	ResyncInterval   time.Duration
	MetricsAddr      string
	ClusterLabel     string
	AdditionalLabels map[string]string
}

// TargetLabel is the standard Prometheus annotation for enabling scrape.
const TargetLabel = "prometheus.io/scrape"

// Common Prometheus annotation keys.
const (
	AnnotationScrape = "prometheus.io/scrape"
	AnnotationPort   = "prometheus.io/port"
	AnnotationPath   = "prometheus.io/path"
	AnnotationScheme = "prometheus.io/scheme"
)

// Load reads configuration from environment variables with sensible defaults.
func Load() (Config, error) {
	cfg := Config{
		Namespace:        envOr("NAMESPACE", ""),
		ConfigMapName:    envOr("CONFIGMAP_NAME", "vector-scrape-config"),
		ConfigMapKey:     envOr("CONFIGMAP_KEY", "scrape_sources.yaml"),
		ScrapeInterval:   durEnvOr("SCRAPE_INTERVAL", 30*time.Second),
		ScrapeTimeout:    durEnvOr("SCRAPE_TIMEOUT", 10*time.Second),
		HonorLabels:      boolEnvOr("HONOR_LABELS", false),
		ResyncInterval:   durEnvOr("RESYNC_INTERVAL", 5*time.Minute),
		MetricsAddr:      envOr("METRICS_LISTEN_ADDR", ":9090"),
		ClusterLabel:     envOr("CLUSTER_LABEL", ""),
		AdditionalLabels: labelsEnvOr("ADDITIONAL_LABELS"),
	}
	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.ConfigMapName == "" {
		return fmt.Errorf("CONFIGMAP_NAME must be set")
	}
	if c.ScrapeInterval < 5*time.Second {
		return fmt.Errorf("SCRAPE_INTERVAL must be >= 5s, got %s", c.ScrapeInterval)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durEnvOr(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func labelsEnvOr(key string) map[string]string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	m := make(map[string]string)
	start := 0
	for start < len(v) {
		comma := indexByte(v[start:], ',')
		end := len(v)
		if comma >= 0 {
			end = start + comma
		}
		pair := v[start:end]
		if eq := indexByte(pair, '='); eq >= 0 {
			m[pair[:eq]] = pair[eq+1:]
		}
		if comma < 0 {
			break
		}
		start = end + 1
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func boolEnvOr(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// ParseBool parses a bool string, returning fallback on failure.
func ParseBool(s string, fallback bool) bool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return fallback
	}
	return b
}

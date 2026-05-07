// Package renderer generates Vector configuration from discovered targets.
package renderer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/discovery"
	"gopkg.in/yaml.v3"
)

// Config holds the rendering parameters.
type Config struct {
	ScrapeIntervalSecs float64
	ScrapeTimeoutSecs  float64
	HonorLabels        bool
	ClusterLabel       string
	AdditionalLabels   map[string]string
}

// VectorSources is the top-level structure for the generated YAML.
// Only sources and transforms are generated; sinks belong in the main config.
type VectorSources struct {
	Sources    map[string]SourceConfig    `yaml:"sources"`
	Transforms map[string]TransformConfig `yaml:"transforms,omitempty"`
}

// SourceConfig represents a Vector prometheus_scrape source.
type SourceConfig struct {
	Type               string            `yaml:"type"`
	Endpoints          []string          `yaml:"endpoints"`
	ScrapeIntervalSecs float64           `yaml:"scrape_interval_secs"`
	ScrapeTimeoutSecs  float64           `yaml:"scrape_timeout_secs"`
	HonorLabels        bool              `yaml:"honor_labels,omitempty"`
	Query              map[string]string `yaml:"query,omitempty"`
	Auth               *AuthConfig       `yaml:"auth,omitempty"`
	TLS                *TLSConfig        `yaml:"tls,omitempty"`
	Proxy              *ProxyConfig      `yaml:"proxy,omitempty"`
}

// AuthConfig represents Vector auth configuration.
type AuthConfig struct {
	Strategy string      `yaml:"strategy"`
	Basic    *BasicAuth  `yaml:"basic,omitempty"`
	Bearer   *BearerAuth `yaml:"bearer,omitempty"`
}

// BasicAuth holds basic auth credentials.
// Env var values use Vector's ${VAR} interpolation syntax.
type BasicAuth struct {
	User         string `yaml:"user,omitempty"`
	Password     string `yaml:"password,omitempty"`
	PasswordFile string `yaml:"password_file,omitempty"`
}

// BearerAuth holds bearer token config.
type BearerAuth struct {
	Token     string `yaml:"token,omitempty"`
	TokenFile string `yaml:"token_file,omitempty"`
}

// TLSConfig represents Vector TLS configuration.
type TLSConfig struct {
	VerifyCertificate *bool  `yaml:"verify_certificate,omitempty"`
	VerifyHostname    *bool  `yaml:"verify_hostname,omitempty"`
	CAFile            string `yaml:"ca_file,omitempty"`
	CrtFile           string `yaml:"crt_file,omitempty"`
	KeyFile           string `yaml:"key_file,omitempty"`
	ServerName        string `yaml:"server_name,omitempty"`
}

// ProxyConfig represents Vector proxy configuration.
type ProxyConfig struct {
	HTTP  string `yaml:"http,omitempty"`
	HTTPS string `yaml:"https,omitempty"`
}

// TransformConfig represents a Vector remap transform.
type TransformConfig struct {
	Type   string   `yaml:"type"`
	Inputs []string `yaml:"inputs"`
	Source string   `yaml:"source"`
}

// Render generates a Vector config fragment (YAML) from the discovered targets.
func Render(targets []discovery.Target, cfg Config) ([]byte, error) {
	if len(targets) == 0 {
		return RenderEmpty(cfg)
	}

	grouped := groupBySourceKey(targets)
	sources := make(map[string]SourceConfig, len(grouped))
	var sourceNames []string

	for name, group := range grouped {
		endpoints := make([]string, 0, len(group.targets))
		for _, t := range group.targets {
			endpoints = append(endpoints, t.URL)
		}
		sort.Strings(endpoints)

		intervalSecs := cfg.ScrapeIntervalSecs
		if group.key.scrapeInterval > 0 {
			intervalSecs = group.key.scrapeInterval
		}
		timeoutSecs := cfg.ScrapeTimeoutSecs
		if group.key.scrapeTimeout > 0 {
			timeoutSecs = group.key.scrapeTimeout
		}

		src := SourceConfig{
			Type:               "prometheus_scrape",
			Endpoints:          endpoints,
			ScrapeIntervalSecs: intervalSecs,
			ScrapeTimeoutSecs:  timeoutSecs,
			HonorLabels:        cfg.HonorLabels,
		}

		// Auth: basic or bearer.
		auth := buildAuth(group.key)
		if auth != nil {
			src.Auth = auth
		}

		// TLS: only emit when any TLS field is set.
		tls := buildTLS(group.key)
		if tls != nil {
			src.TLS = tls
		}

		// Proxy.
		proxy := buildProxy(group.key)
		if proxy != nil {
			src.Proxy = proxy
		}

		sources[name] = src
		sourceNames = append(sourceNames, name)
	}
	sort.Strings(sourceNames)

	// Build a remap transform that enriches metrics with k8s labels.
	transformName := "enrich_metrics"
	remapSource := buildRemapSource(targets, cfg)

	transforms := map[string]TransformConfig{
		transformName: {
			Type:   "remap",
			Inputs: sourceNames,
			Source: remapSource,
		},
	}

	result := VectorSources{
		Sources:    sources,
		Transforms: transforms,
	}

	out, err := yaml.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	return out, nil
}

// RenderEmpty produces a minimal config when no targets are discovered.
func RenderEmpty(cfg Config) ([]byte, error) {
	result := VectorSources{
		Sources: map[string]SourceConfig{
			"no_targets": {
				Type:               "prometheus_scrape",
				Endpoints:          []string{},
				ScrapeIntervalSecs: cfg.ScrapeIntervalSecs,
				ScrapeTimeoutSecs:  cfg.ScrapeTimeoutSecs,
			},
		},
	}
	out, err := yaml.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal empty config: %w", err)
	}
	return out, nil
}

// sourceGroupKey uniquely identifies a set of source-level config that must
// share the same Vector source. Endpoints with different keys cannot be
// grouped together because auth, TLS, proxy, and interval settings are
// source-level — they apply to all endpoints in the source.
type sourceGroupKey struct {
	scheme string
	path   string
	params string

	// Per-target overrides (zero value = use global default).
	scrapeInterval float64 // seconds; 0 means global
	scrapeTimeout  float64 // seconds; 0 means global

	// Auth fingerprint.
	basicUserEnv    string
	basicPassEnv    string
	basicPassFile   string
	bearerTokenEnv  string
	bearerTokenFile string
	svcAcctBearer   string

	// TLS fingerprint.
	tlsServerName string
	tlsInsecure   bool
	tlsCAFile     string
	tlsCertFile   string
	tlsKeyFile    string

	// Proxy fingerprint.
	httpProxyURL string
}

type sourceGroup struct {
	key     sourceGroupKey
	targets []discovery.Target
}

func groupBySourceKey(targets []discovery.Target) map[string]sourceGroup {
	groups := make(map[sourceGroupKey][]discovery.Target)

	for _, t := range targets {
		key := sourceGroupKey{
			scheme: extractScheme(t.URL),
			path:   extractPath(t.URL),
			params: t.Params,

			basicUserEnv:    t.BasicAuthUserEnvVar,
			basicPassEnv:    t.BasicAuthPasswordEnvVar,
			basicPassFile:   t.BasicAuthPasswordFile,
			bearerTokenEnv:  t.BearerTokenEnvVar,
			bearerTokenFile: t.BearerTokenFile,
			svcAcctBearer:   t.ServiceAccountBearer,

			tlsServerName: t.TLSServerName,
			tlsInsecure:   t.TLSInsecureSkipVerify,
			tlsCAFile:     t.TLSCAFile,
			tlsCertFile:   t.TLSCertFile,
			tlsKeyFile:    t.TLSKeyFile,

			httpProxyURL: t.HTTPProxyURL,
		}
		if t.ScrapeInterval > 0 {
			key.scrapeInterval = t.ScrapeInterval.Seconds()
		}
		if t.ScrapeTimeout > 0 {
			key.scrapeTimeout = t.ScrapeTimeout.Seconds()
		}

		groups[key] = append(groups[key], t)
	}

	result := make(map[string]sourceGroup, len(groups))
	idx := 0
	for key, tgts := range groups {
		baseName := fmt.Sprintf("k8s_metrics_%s_%s",
			sanitize(key.scheme),
			sanitize(strings.Trim(key.path, "/")),
		)
		name := baseName
		if len(groups) > 1 {
			// Disambiguate when auth/TLS/proxy/interval create multiple sources
			// for the same scheme+path combo.
			name = fmt.Sprintf("%s_%02d", baseName, idx)
		}
		result[name] = sourceGroup{key: key, targets: tgts}
		idx++
	}
	return result
}

func buildAuth(key sourceGroupKey) *AuthConfig {
	hasBasic := key.basicUserEnv != "" || key.basicPassEnv != "" || key.basicPassFile != ""
	hasBearer := key.bearerTokenEnv != "" || key.bearerTokenFile != "" || key.svcAcctBearer != ""

	if !hasBasic && !hasBearer {
		return nil
	}

	if hasBasic {
		basic := &BasicAuth{}
		if key.basicUserEnv != "" {
			basic.User = "${" + key.basicUserEnv + "}"
		}
		if key.basicPassEnv != "" {
			basic.Password = "${" + key.basicPassEnv + "}"
		}
		if key.basicPassFile != "" {
			basic.PasswordFile = key.basicPassFile
		}
		return &AuthConfig{
			Strategy: "basic",
			Basic:    basic,
		}
	}

	// Bearer auth.
	bearer := &BearerAuth{}
	if key.svcAcctBearer != "" {
		bearer.Token = key.svcAcctBearer
	}
	if key.bearerTokenFile != "" {
		bearer.TokenFile = key.bearerTokenFile
	}
	if key.bearerTokenEnv != "" {
		bearer.Token = "${" + key.bearerTokenEnv + "}"
	}
	return &AuthConfig{
		Strategy: "bearer",
		Bearer:   bearer,
	}
}

func buildTLS(key sourceGroupKey) *TLSConfig {
	if key.tlsServerName == "" && !key.tlsInsecure && key.tlsCAFile == "" &&
		key.tlsCertFile == "" && key.tlsKeyFile == "" {
		return nil
	}

	tls := &TLSConfig{}
	if key.tlsInsecure {
		verifyFalse := false
		tls.VerifyCertificate = &verifyFalse
		tls.VerifyHostname = &verifyFalse
	}
	if key.tlsCAFile != "" {
		tls.CAFile = key.tlsCAFile
	}
	if key.tlsCertFile != "" {
		tls.CrtFile = key.tlsCertFile
	}
	if key.tlsKeyFile != "" {
		tls.KeyFile = key.tlsKeyFile
	}
	if key.tlsServerName != "" {
		tls.ServerName = key.tlsServerName
	}
	return tls
}

func buildProxy(key sourceGroupKey) *ProxyConfig {
	if key.httpProxyURL == "" {
		return nil
	}
	return &ProxyConfig{
		HTTP: key.httpProxyURL,
	}
}

func extractScheme(url string) string {
	before, _, ok := strings.Cut(url, "://")
	if !ok {
		return "http"
	}
	return before
}

func extractPath(url string) string {
	_, after, ok := strings.Cut(url, "://")
	if !ok {
		return "/metrics"
	}
	// Strip query string — params are handled separately.
	if qIdx := strings.Index(after, "?"); qIdx >= 0 {
		after = after[:qIdx]
	}
	slashIdx := strings.Index(after, "/")
	if slashIdx < 0 {
		return "/"
	}
	return after[slashIdx:]
}

var sanitizeReplacer = strings.NewReplacer(
	".", "_",
	"-", "_",
	"/", "_",
	":", "_",
)

func sanitize(s string) string {
	return sanitizeReplacer.Replace(s)
}

// buildRemapSource generates VRL that enriches metrics with k8s labels
// using an instance-based lookup table.
func buildRemapSource(targets []discovery.Target, cfg Config) string {
	var b strings.Builder

	// Build metadata lookup table keyed by instance (host:port).
	b.WriteString("# Auto-generated Kubernetes metadata enrichment.\n")
	b.WriteString("# Enriches prometheus_scrape metrics with k8s labels.\n\n")
	b.WriteString("metadata = {\n")

	seen := make(map[string]bool)
	for _, t := range targets {
		if t.Instance == "" || seen[t.Instance] {
			continue
		}
		seen[t.Instance] = true

		fmt.Fprintf(&b, "  \"%s\": {", t.Instance)
		parts := make([]string, 0, 6)
		parts = append(parts, fmt.Sprintf("\"namespace\": \"%s\"", t.Namespace))
		if t.Pod != "" {
			parts = append(parts, fmt.Sprintf("\"pod\": \"%s\"", t.Pod))
		}
		if t.Service != "" {
			parts = append(parts, fmt.Sprintf("\"service\": \"%s\"", t.Service))
		}
		if t.Node != "" {
			parts = append(parts, fmt.Sprintf("\"node\": \"%s\"", t.Node))
		}
		if t.Container != "" {
			parts = append(parts, fmt.Sprintf("\"container\": \"%s\"", t.Container))
		}
		if t.Job != "" {
			parts = append(parts, fmt.Sprintf("\"job\": \"%s\"", t.Job))
		}
		b.WriteString(strings.Join(parts, ", "))
		b.WriteString("},\n")
	}
	b.WriteString("}\n\n")

	// Lookup metadata by instance tag.
	b.WriteString("inst = .tags.instance\n")
	b.WriteString("if inst != null && metadata[inst] != null {\n")
	b.WriteString("  .tags.namespace = metadata[inst].namespace\n")
	b.WriteString("  .tags.pod = metadata[inst].pod\n")
	b.WriteString("  .tags.node = metadata[inst].node\n")
	b.WriteString("  .tags.service = metadata[inst].service\n")
	b.WriteString("  .tags.container = metadata[inst].container\n")
	b.WriteString("  .tags.job = metadata[inst].job\n")
	b.WriteString("}\n")

	// Add cluster label.
	if cfg.ClusterLabel != "" {
		fmt.Fprintf(&b, ".tags.cluster = \"%s\"\n", cfg.ClusterLabel)
	}

	// Add any additional static labels.
	for k, v := range cfg.AdditionalLabels {
		fmt.Fprintf(&b, ".tags.%s = \"%s\"\n", k, v)
	}

	return b.String()
}

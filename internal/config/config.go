package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Namespace        string
	ConfigMapName    string
	ConfigMapKey     string
	ScrapeInterval   time.Duration
	ScrapeTimeout    time.Duration
	HonorLabels      bool
	ResyncInterval   time.Duration
	DebounceInterval time.Duration
	MetricsAddr      string
	ClusterLabel     string
	AdditionalLabels map[string]string
	AnnotationPrefix string

	Roles              Roles
	Selectors          Selectors
	Namespaces         NamespaceFilter
	AttachNodeMetadata bool
	AttachNsMetadata   bool
	IncludeAnnotations bool
	IncludeLabels      bool
	NodeScrapePort     int32
	ServiceDNSSuffix   string
}

type Roles struct {
	Pod            bool
	Endpoints      bool
	EndpointSlice  bool
	Node           bool
	Ingress        bool
	ServiceAddress bool
}

type Selectors struct {
	PodLabel           string
	PodField           string
	ServiceLabel       string
	ServiceField       string
	NodeLabel          string
	NodeField          string
	IngressLabel       string
	IngressField       string
	EndpointSliceLabel string
	EndpointSliceField string
	EndpointsLabel     string
	EndpointsField     string
}

type NamespaceFilter struct {
	Include []string
	Exclude []string
}

const (
	AnnotationScrape                    = "scrape"
	AnnotationPort                      = "port"
	AnnotationPath                      = "path"
	AnnotationScheme                    = "scheme"
	AnnotationParams                    = "params"
	AnnotationJob                       = "job"
	AnnotationCollectionInterval        = "collectionInterval"
	AnnotationCollectionTimeout         = "collectionTimeout"
	AnnotationServiceAccountBearerToken = "serviceAccountBearerToken"
	AnnotationTLSServerName             = "tlsServerName"
	AnnotationTLSInsecureSkipVerify     = "tlsInsecureSkipVerify"
	AnnotationHTTPBasicAuthUsernameEnv  = "httpBasicAuthUsernameEnvVar"
	AnnotationHTTPBasicAuthPasswordEnv  = "httpBasicAuthPasswordEnvVar"
	AnnotationHTTPBearerTokenEnv        = "httpBearerTokenEnvVar"
	AnnotationTLSCAFile                 = "tlsCAFile"
	AnnotationTLSCertFile               = "tlsCertFile"
	AnnotationTLSKeyFile                = "tlsKeyFile"

	ExclusionAnnotation = "vector.dev/exclude"
)

const defaultAnnotationPrefix = "prometheus.io"

func (c Config) Annot(key string) string {
	return c.AnnotationPrefix + "/" + key
}

func Load() (Config, error) {
	cfg := Config{
		Namespace:        envOr("NAMESPACE", ""),
		ConfigMapName:    envOr("CONFIGMAP_NAME", "vector-scrape-config"),
		ConfigMapKey:     envOr("CONFIGMAP_KEY", "scrape_sources.yaml"),
		ScrapeInterval:   durEnvOr("SCRAPE_INTERVAL", 30*time.Second),
		ScrapeTimeout:    durEnvOr("SCRAPE_TIMEOUT", 10*time.Second),
		HonorLabels:      boolEnvOr("HONOR_LABELS", false),
		ResyncInterval:   durEnvOr("RESYNC_INTERVAL", 5*time.Minute),
		DebounceInterval: durEnvOr("DEBOUNCE_INTERVAL", 250*time.Millisecond),
		MetricsAddr:      envOr("METRICS_LISTEN_ADDR", ":9090"),
		ClusterLabel:     envOr("CLUSTER_LABEL", ""),
		AdditionalLabels: labelsEnvOr("ADDITIONAL_LABELS"),
		AnnotationPrefix: envOr("ANNOTATION_PREFIX", defaultAnnotationPrefix),

		Roles:              parseRoles(envOr("ROLES", "pod,endpointslice")),
		Selectors:          parseSelectors(),
		Namespaces:         parseNamespaces(envOr("NAMESPACE_INCLUDE", ""), envOr("NAMESPACE_EXCLUDE", "")),
		AttachNodeMetadata: boolEnvOr("ATTACH_NODE_METADATA", false),
		AttachNsMetadata:   boolEnvOr("ATTACH_NAMESPACE_METADATA", false),
		IncludeAnnotations: boolEnvOr("INCLUDE_ANNOTATIONS", false),
		IncludeLabels:      boolEnvOr("INCLUDE_LABELS", true),
		NodeScrapePort:     int32EnvOr("NODE_SCRAPE_PORT", 10250),
		ServiceDNSSuffix:   envOr("SERVICE_DNS_SUFFIX", "svc.cluster.local"),
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
	if c.DebounceInterval < 0 {
		return fmt.Errorf("DEBOUNCE_INTERVAL must be >= 0, got %s", c.DebounceInterval)
	}
	return nil
}

func parseRoles(v string) Roles {
	var r Roles
	if v == "" {
		r.Pod = true
		r.EndpointSlice = true
		return r
	}
	parts := strings.SplitSeq(v, ",")
	for p := range parts {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case "pod":
			r.Pod = true
		case "endpoints":
			r.Endpoints = true
		case "endpointslice":
			r.EndpointSlice = true
		case "node":
			r.Node = true
		case "ingress":
			r.Ingress = true
		case "service":
			r.ServiceAddress = true
		}
	}
	if !r.Pod && !r.Endpoints && !r.EndpointSlice && !r.Node && !r.Ingress && !r.ServiceAddress {
		r.Pod = true
		r.EndpointSlice = true
	}
	return r
}

func parseSelectors() Selectors {
	return Selectors{
		PodLabel:           envOr("POD_LABEL_SELECTOR", ""),
		PodField:           envOr("POD_FIELD_SELECTOR", ""),
		ServiceLabel:       envOr("SERVICE_LABEL_SELECTOR", ""),
		ServiceField:       envOr("SERVICE_FIELD_SELECTOR", ""),
		NodeLabel:          envOr("NODE_LABEL_SELECTOR", ""),
		NodeField:          envOr("NODE_FIELD_SELECTOR", ""),
		IngressLabel:       envOr("INGRESS_LABEL_SELECTOR", ""),
		IngressField:       envOr("INGRESS_FIELD_SELECTOR", ""),
		EndpointSliceLabel: envOr("ENDPOINTSLICE_LABEL_SELECTOR", ""),
		EndpointSliceField: envOr("ENDPOINTSLICE_FIELD_SELECTOR", ""),
		EndpointsLabel:     envOr("ENDPOINTS_LABEL_SELECTOR", ""),
		EndpointsField:     envOr("ENDPOINTS_FIELD_SELECTOR", ""),
	}
}

func (r Roles) Any() bool {
	return r.Pod || r.Endpoints || r.EndpointSlice || r.Node || r.Ingress || r.ServiceAddress
}

func (r Roles) Slice() []string {
	var out []string
	if r.Pod {
		out = append(out, "pod")
	}
	if r.Endpoints {
		out = append(out, "endpoints")
	}
	if r.EndpointSlice {
		out = append(out, "endpointslice")
	}
	if r.Node {
		out = append(out, "node")
	}
	if r.Ingress {
		out = append(out, "ingress")
	}
	if r.ServiceAddress {
		out = append(out, "service")
	}
	return out
}

func (r Roles) String() string {
	return strings.Join(r.Slice(), ",")
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

func int32EnvOr(key string, fallback int32) int32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(n)
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

func indexByte(s string, b byte) int {
	return strings.IndexByte(s, b)
}

func ParseBool(s string, fallback bool) bool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return fallback
	}
	return b
}

func parseNamespaces(include, exclude string) NamespaceFilter {
	return NamespaceFilter{
		Include: splitCSV(include),
		Exclude: splitCSV(exclude),
	}
}

func (nf NamespaceFilter) Allowed(namespace string) bool {
	if slices.Contains(nf.Exclude, namespace) {
		return false
	}
	if len(nf.Include) == 0 {
		return true
	}
	return slices.Contains(nf.Include, namespace)
}

func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.SplitSeq(v, ",")
	var out []string
	for p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

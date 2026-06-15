package renderer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/discovery"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ScrapeIntervalSecs float64
	ScrapeTimeoutSecs  float64
	HonorLabels        bool
	ClusterLabel       string
	AdditionalLabels   map[string]string
}

type VectorSources struct {
	Sources    map[string]SourceConfig    `yaml:"sources"`
	Transforms map[string]TransformConfig `yaml:"transforms,omitempty"`
}

type SourceConfig struct {
	Type               string      `yaml:"type"`
	Endpoints          []string    `yaml:"endpoints"`
	ScrapeIntervalSecs float64     `yaml:"scrape_interval_secs"`
	ScrapeTimeoutSecs  float64     `yaml:"scrape_timeout_secs"`
	HonorLabels        bool        `yaml:"honor_labels,omitempty"`
	Auth               *AuthConfig `yaml:"auth,omitempty"`
	TLS                *TLSConfig  `yaml:"tls,omitempty"`
}

type AuthConfig struct {
	Strategy string `yaml:"strategy"`
	User     string `yaml:"user,omitempty"`
	Password string `yaml:"password,omitempty"`
	Token    string `yaml:"token,omitempty"`
}

type TLSConfig struct {
	VerifyCertificate *bool  `yaml:"verify_certificate,omitempty"`
	VerifyHostname    *bool  `yaml:"verify_hostname,omitempty"`
	CAFile            string `yaml:"ca_file,omitempty"`
	CrtFile           string `yaml:"crt_file,omitempty"`
	KeyFile           string `yaml:"key_file,omitempty"`
	ServerName        string `yaml:"server_name,omitempty"`
}

type TransformConfig struct {
	Type   string   `yaml:"type"`
	Inputs []string `yaml:"inputs"`
	Source string   `yaml:"source"`
}

func Render(targets []discovery.Target, cfg Config) ([]byte, error) {
	if len(targets) == 0 {
		return RenderEmpty(cfg)
	}

	grouped := groupBySourceKey(targets)
	sources := make(map[string]SourceConfig, len(grouped))
	sourceNames := make([]string, 0, len(grouped))

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

		src.Auth = buildAuth(group.key)
		src.TLS = buildTLS(group.key)

		sources[name] = src
		sourceNames = append(sourceNames, name)
	}
	sort.Strings(sourceNames)

	remapSource := buildRemapSource(targets, cfg)

	transforms := map[string]TransformConfig{
		"enrich_metrics": {
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

func RenderEmpty(cfg Config) ([]byte, error) {
	sourceNames := []string{"no_targets"}
	result := VectorSources{
		Sources: map[string]SourceConfig{
			"no_targets": {
				Type:               "prometheus_scrape",
				Endpoints:          []string{},
				ScrapeIntervalSecs: cfg.ScrapeIntervalSecs,
				ScrapeTimeoutSecs:  cfg.ScrapeTimeoutSecs,
			},
		},
		Transforms: map[string]TransformConfig{
			"enrich_metrics": {
				Type:   "remap",
				Inputs: sourceNames,
				Source: buildRemapSource(nil, cfg),
			},
		},
	}
	out, err := yaml.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal empty config: %w", err)
	}
	return out, nil
}

type sourceGroupKey struct {
	scheme         string
	path           string
	scrapeInterval float64
	scrapeTimeout  float64
	basicUserEnv   string
	basicPassword  string
	bearerTokenEnv string
	bearerToken    string
	tlsServerName  string
	tlsInsecure    bool
	tlsCAFile      string
	tlsCertFile    string
	tlsKeyFile     string
}

type sourceGroup struct {
	key     sourceGroupKey
	targets []discovery.Target
}

func groupBySourceKey(targets []discovery.Target) map[string]sourceGroup {
	groups := make(map[sourceGroupKey][]discovery.Target)

	for _, t := range targets {
		key := sourceGroupKey{
			scheme:         extractScheme(t.URL),
			path:           extractPath(t.URL),
			basicUserEnv:   t.BasicAuthUserEnv,
			basicPassword:  t.BasicAuthPassword,
			bearerTokenEnv: t.BearerTokenEnv,
			bearerToken:    t.BearerToken,
			tlsServerName:  t.TLSServerName,
			tlsInsecure:    t.TLSInsecureSkipVerify,
			tlsCAFile:      t.TLSCAFile,
			tlsCertFile:    t.TLSCertFile,
			tlsKeyFile:     t.TLSKeyFile,
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
			sanitize(strings.Trim(key.path, "/")))
		name := baseName
		if len(groups) > 1 {
			name = fmt.Sprintf("%s_%02d", baseName, idx)
		}
		result[name] = sourceGroup{key: key, targets: tgts}
		idx++
	}
	return result
}

func buildAuth(key sourceGroupKey) *AuthConfig {
	hasBasic := key.basicUserEnv != "" || key.basicPassword != ""
	hasBearer := key.bearerTokenEnv != "" || key.bearerToken != ""

	if !hasBasic && !hasBearer {
		return nil
	}

	if hasBasic {
		a := &AuthConfig{Strategy: "basic"}
		if key.basicUserEnv != "" {
			a.User = "${" + key.basicUserEnv + "}"
		}
		if key.basicPassword != "" {
			a.Password = "${" + key.basicPassword + "}"
		}
		return a
	}

	a := &AuthConfig{Strategy: "bearer"}
	if key.bearerToken != "" {
		a.Token = key.bearerToken
	}
	if key.bearerTokenEnv != "" {
		a.Token = "${" + key.bearerTokenEnv + "}"
	}
	return a
}

func buildTLS(key sourceGroupKey) *TLSConfig {
	if key.tlsServerName == "" && !key.tlsInsecure && key.tlsCAFile == "" &&
		key.tlsCertFile == "" && key.tlsKeyFile == "" {
		return nil
	}

	tls := &TLSConfig{}
	if key.tlsInsecure {
		v := false
		tls.VerifyCertificate = &v
		tls.VerifyHostname = &v
	}
	tls.CAFile = key.tlsCAFile
	tls.CrtFile = key.tlsCertFile
	tls.KeyFile = key.tlsKeyFile
	tls.ServerName = key.tlsServerName
	return tls
}

func extractScheme(rawURL string) string {
	before, _, ok := strings.Cut(rawURL, "://")
	if !ok {
		return "http"
	}
	return before
}

func extractPath(rawURL string) string {
	_, after, ok := strings.Cut(rawURL, "://")
	if !ok {
		return "/metrics"
	}
	if qIdx := strings.Index(after, "?"); qIdx >= 0 {
		after = after[:qIdx]
	}
	slashIdx := strings.Index(after, "/")
	if slashIdx < 0 {
		return "/"
	}
	return after[slashIdx:]
}

var sanitizeReplacer = strings.NewReplacer(".", "_", "-", "_", "/", "_", ":", "_")

func sanitize(s string) string {
	return sanitizeReplacer.Replace(s)
}

func buildRemapSource(targets []discovery.Target, cfg Config) string {
	type entry struct {
		instance string
		t        discovery.Target
	}
	entries := make([]entry, 0, len(targets))
	seen := make(map[string]bool, len(targets))
	for _, t := range targets {
		if t.Instance == "" || seen[t.Instance] {
			continue
		}
		seen[t.Instance] = true
		entries = append(entries, entry{instance: t.Instance, t: t})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].instance < entries[j].instance
	})

	var b strings.Builder
	b.Grow(len(entries)*256 + 256)

	b.WriteString("inst = .tags.instance\n")
	b.WriteString("m = get!(.metadata, [inst])\n")
	b.WriteString("if m != null {\n")
	for _, line := range enrichLines() {
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString("  .tags = merge!(.tags, m.labels)\n")
	b.WriteString("}\n")

	if cfg.ClusterLabel != "" {
		fmt.Fprintf(&b, ".tags.cluster = \"%s\"\n", cfg.ClusterLabel)
	}
	for _, k := range sortedKeys(cfg.AdditionalLabels) {
		fmt.Fprintf(&b, ".tags.%s = \"%s\"\n", k, cfg.AdditionalLabels[k])
	}

	var pre strings.Builder
	pre.Grow(len(entries)*256 + 64)
	pre.WriteString("metadata = {\n")
	for _, e := range entries {
		writeEntry(&pre, e.instance, e.t)
	}
	pre.WriteString("}\n\n")

	return pre.String() + b.String()
}

func enrichLines() []string {
	return []string{
		".tags.namespace = m.namespace",
		".tags.pod = m.pod",
		".tags.service = m.service",
		".tags.node = m.node",
		".tags.container = m.container",
		".tags.job = m.job",
		".tags.role = m.role",
		".tags.pod_uid = m.pod_uid",
		".tags.pod_phase = m.pod_phase",
		".tags.pod_ready = m.pod_ready",
		".tags.controller_kind = m.controller_kind",
		".tags.controller_name = m.controller_name",
		".tags.container_image = m.container_image",
		".tags.container_id = m.container_id",
		".tags.container_init = m.container_init",
		".tags.host_ip = m.host_ip",
		".tags.pod_ip = m.pod_ip",
		".tags.node_ip = m.node_ip",
		".tags.port_name = m.port_name",
		".tags.port_number = m.port_number",
		".tags.port_protocol = m.port_protocol",
		".tags.service_port_name = m.service_port_name",
		".tags.service_type = m.service_type",
		".tags.service_cluster_ip = m.service_cluster_ip",
		".tags.service_external_name = m.service_external_name",
		".tags.node_provider_id = m.node_provider_id",
		".tags.endpoint_ready = m.endpoint_ready",
		".tags.endpoint_hostname = m.endpoint_hostname",
		".tags.endpoint_node_name = m.endpoint_node_name",
		".tags.endpoint_zone = m.endpoint_zone",
		".tags.endpoint_address_type = m.endpoint_address_type",
		".tags.endpointslice_name = m.endpointslice_name",
		".tags.ingress_class_name = m.ingress_class_name",
		".tags.ingress_path = m.ingress_path",
		".tags.ingress_scheme = m.ingress_scheme",
		".tags = merge!(.tags, m.node_addresses)",
	}
}

func writeEntry(b *strings.Builder, instance string, t discovery.Target) {
	fmt.Fprintf(b, "  %q: {", instance)
	var parts []string
	parts = appendStr(parts, "namespace", t.Namespace)
	parts = appendStr(parts, "pod", t.Pod)
	parts = appendStr(parts, "service", t.Service)
	parts = appendStr(parts, "node", t.Node)
	parts = appendStr(parts, "container", t.Container)
	parts = appendStr(parts, "job", t.Job)
	parts = appendStr(parts, "role", t.Role)
	parts = appendStr(parts, "pod_uid", t.PodUID)
	parts = appendStr(parts, "pod_phase", t.PodPhase)
	parts = appendStr(parts, "pod_ready", t.PodReady)
	parts = appendStr(parts, "controller_kind", t.ControllerKind)
	parts = appendStr(parts, "controller_name", t.ControllerName)
	parts = appendStr(parts, "container_image", t.ContainerImage)
	parts = appendStr(parts, "container_id", t.ContainerID)
	parts = appendStr(parts, "container_init", t.ContainerInit)
	parts = appendStr(parts, "host_ip", t.HostIP)
	parts = appendStr(parts, "pod_ip", t.PodIP)
	parts = appendStr(parts, "node_ip", t.NodeIP)
	parts = appendStr(parts, "port_name", t.PortName)
	parts = appendStr(parts, "port_number", t.PortNumber)
	parts = appendStr(parts, "port_protocol", t.PortProtocol)
	parts = appendStr(parts, "service_port_name", t.ServicePortName)
	parts = appendStr(parts, "service_type", t.ServiceType)
	parts = appendStr(parts, "service_cluster_ip", t.ServiceClusterIP)
	parts = appendStr(parts, "service_external_name", t.ServiceExternalName)
	parts = appendStr(parts, "node_provider_id", t.NodeProviderID)
	parts = appendStr(parts, "endpoint_ready", t.EndpointReady)
	parts = appendStr(parts, "endpoint_hostname", t.EndpointHostname)
	parts = appendStr(parts, "endpoint_node_name", t.EndpointNodeName)
	parts = appendStr(parts, "endpoint_zone", t.EndpointZone)
	parts = appendStr(parts, "endpoint_address_type", t.EndpointAddressType)
	parts = appendStr(parts, "endpointslice_name", t.EndpointSliceName)
	parts = appendStr(parts, "ingress_class_name", t.IngressClassName)
	parts = appendStr(parts, "ingress_path", t.IngressPath)
	parts = appendStr(parts, "ingress_scheme", t.IngressScheme)

	for i, p := range parts {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(p)
	}
	if len(t.NodeAddresses) > 0 {
		b.WriteString(", \"node_addresses\": {")
		first := true
		for _, k := range sortedKeys(t.NodeAddresses) {
			if !first {
				b.WriteString(", ")
			}
			first = false
			fmt.Fprintf(b, "%q: %q", "node_address_"+k, t.NodeAddresses[k])
		}
		b.WriteString("}")
	} else {
		b.WriteString(", \"node_addresses\": {}")
	}
	if len(t.Labels) > 0 {
		b.WriteString(", \"labels\": {")
		first := true
		for _, k := range sortedKeys(t.Labels) {
			if !first {
				b.WriteString(", ")
			}
			first = false
			fmt.Fprintf(b, "%q: %q", k, t.Labels[k])
		}
		b.WriteString("}")
	} else {
		b.WriteString(", \"labels\": {}")
	}
	b.WriteString("},\n")
}

func appendStr(parts []string, key, val string) []string {
	if val == "" {
		return parts
	}
	return append(parts, fmt.Sprintf("%q: %q", key, val))
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

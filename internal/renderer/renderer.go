package renderer

import (
	"fmt"
	"sort"
	"strconv"
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
	InstanceTag        string      `yaml:"instance_tag"`
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
			InstanceTag:        "instance",
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
				InstanceTag:        "instance",
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
	seen := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		if t.Instance == "" {
			continue
		}
		if _, ok := seen[t.Instance]; ok {
			continue
		}
		seen[t.Instance] = struct{}{}
		entries = append(entries, entry{instance: t.Instance, t: t})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].instance < entries[j].instance
	})

	var b strings.Builder
	b.Grow(len(entries)*256 + 1024)

	// metadata table must precede the remap logic.
	b.WriteString("metadata = {\n")
	for _, e := range entries {
		writeEntry(&b, e.instance, e.t)
	}
	b.WriteString("}\n\n")

	b.WriteString("if .tags.node != get_env_var!(\"VECTOR_SELF_NODE_NAME\") { abort }\n")
	b.WriteString("inst = .tags.instance\n")
	b.WriteString("m = get!(metadata, [inst])\n")
	b.WriteString("if m != null {\n")
	for _, line := range enrichLinesList {
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteString("  .tags = merge!(.tags, m.labels)\n")
	b.WriteString("}\n")

	if cfg.ClusterLabel != "" {
		b.WriteString(".tags.cluster = \"")
		b.WriteString(cfg.ClusterLabel)
		b.WriteString("\"\n")
	}
	for _, k := range sortedKeys(cfg.AdditionalLabels) {
		b.WriteString(".tags.")
		b.WriteString(k)
		b.WriteString(" = \"")
		b.WriteString(cfg.AdditionalLabels[k])
		b.WriteString("\"\n")
	}

	return b.String()
}

var enrichLinesList = []string{
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

func writeEntry(b *strings.Builder, instance string, t discovery.Target) {
	b.WriteString("  ")
	writeQuoted(b, instance)
	b.WriteString(": {")

	sep := ""
	field := func(key, val string) {
		if val == "" {
			return
		}
		b.WriteString(sep)
		sep = ", "
		writeQuoted(b, key)
		b.WriteString(": ")
		writeQuoted(b, val)
	}
	field("namespace", t.Namespace)
	field("pod", t.Pod)
	field("service", t.Service)
	field("node", t.Node)
	field("container", t.Container)
	field("job", t.Job)
	field("role", t.Role)
	field("pod_uid", t.PodUID)
	field("pod_phase", t.PodPhase)
	field("pod_ready", t.PodReady)
	field("controller_kind", t.ControllerKind)
	field("controller_name", t.ControllerName)
	field("container_image", t.ContainerImage)
	field("container_id", t.ContainerID)
	field("container_init", t.ContainerInit)
	field("host_ip", t.HostIP)
	field("pod_ip", t.PodIP)
	field("node_ip", t.NodeIP)
	field("port_name", t.PortName)
	field("port_number", t.PortNumber)
	field("port_protocol", t.PortProtocol)
	field("service_port_name", t.ServicePortName)
	field("service_type", t.ServiceType)
	field("service_cluster_ip", t.ServiceClusterIP)
	field("service_external_name", t.ServiceExternalName)
	field("node_provider_id", t.NodeProviderID)
	field("endpoint_ready", t.EndpointReady)
	field("endpoint_hostname", t.EndpointHostname)
	field("endpoint_node_name", t.EndpointNodeName)
	field("endpoint_zone", t.EndpointZone)
	field("endpoint_address_type", t.EndpointAddressType)
	field("endpointslice_name", t.EndpointSliceName)
	field("ingress_class_name", t.IngressClassName)
	field("ingress_path", t.IngressPath)
	field("ingress_scheme", t.IngressScheme)

	b.WriteString(", \"node_addresses\": ")
	if len(t.NodeAddresses) > 0 {
		b.WriteByte('{')
		first := true
		for _, k := range sortedKeys(t.NodeAddresses) {
			if !first {
				b.WriteString(", ")
			}
			first = false
			b.WriteString(`"node_address_`)
			b.WriteString(k)
			b.WriteString(`": "`)
			b.WriteString(t.NodeAddresses[k])
			b.WriteByte('"')
		}
		b.WriteByte('}')
	} else {
		b.WriteString("{}")
	}
	b.WriteString(", \"labels\": ")
	if len(t.Labels) > 0 {
		b.WriteByte('{')
		first := true
		for _, k := range sortedKeys(t.Labels) {
			if !first {
				b.WriteString(", ")
			}
			first = false
			writeQuoted(b, k)
			b.WriteString(": ")
			writeQuoted(b, t.Labels[k])
		}
		b.WriteByte('}')
	} else {
		b.WriteString("{}")
	}
	b.WriteString("},\n")
}

// writeQuoted writes a double-quoted string matching strconv.Quote output.
// The common case (printable ASCII without quotes or backslashes) takes a
// zero-allocation fast path; rare strings needing escapes fall back to
// strconv.Quote.
func writeQuoted(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' || c < 0x20 || c >= 0x7f {
			b.WriteString(strconv.Quote(s))
			return
		}
	}
	b.WriteByte('"')
	b.WriteString(s)
	b.WriteByte('"')
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

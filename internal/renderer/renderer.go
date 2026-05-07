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
	Type               string   `yaml:"type"`
	Endpoints          []string `yaml:"endpoints"`
	ScrapeIntervalSecs float64  `yaml:"scrape_interval_secs"`
	ScrapeTimeoutSecs  float64  `yaml:"scrape_timeout_secs"`
	HonorLabels        bool     `yaml:"honor_labels,omitempty"`
}

// TransformConfig represents a Vector remap transform.
type TransformConfig struct {
	Type   string   `yaml:"type"`
	Inputs []string `yaml:"inputs"`
	Source string   `yaml:"source"`
}

// Render generates a Vector config fragment (YAML) from the discovered targets.
// The output contains sources and a remap transform that enriches metrics
// with Kubernetes metadata (namespace, pod, node, service) and a cluster label.
func Render(targets []discovery.Target, cfg Config) ([]byte, error) {
	if len(targets) == 0 {
		return RenderEmpty(cfg)
	}

	grouped := groupBySchemePath(targets)
	sources := make(map[string]SourceConfig, len(grouped))
	var sourceNames []string

	for name, group := range grouped {
		endpoints := make([]string, 0, len(group))
		for _, t := range group {
			endpoints = append(endpoints, t.URL)
		}
		sort.Strings(endpoints)
		sources[name] = SourceConfig{
			Type:               "prometheus_scrape",
			Endpoints:          endpoints,
			ScrapeIntervalSecs: cfg.ScrapeIntervalSecs,
			ScrapeTimeoutSecs:  cfg.ScrapeTimeoutSecs,
			HonorLabels:        cfg.HonorLabels,
		}
		sourceNames = append(sourceNames, name)
	}
	sort.Strings(sourceNames)

	// Build a remap transform that enriches metrics with k8s labels
	// using an instance-based lookup table.
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
	// Use a blackhole source so the config is valid even with no targets.
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

// targetGroup groups targets by their scheme+path combo so we can
// create fewer Vector sources (prometheus_scrape requires all endpoints
// in one source to share the same scrape interval).
type targetGroup struct {
	scheme string
	path   string
}

func groupBySchemePath(targets []discovery.Target) map[string][]discovery.Target {
	groups := make(map[targetGroup][]discovery.Target)
	for _, t := range targets {
		key := targetGroup{
			scheme: extractScheme(t.URL),
			path:   extractPath(t.URL),
		}
		groups[key] = append(groups[key], t)
	}

	result := make(map[string][]discovery.Target)
	for grp, tgts := range groups {
		name := fmt.Sprintf("k8s_metrics_%s_%s",
			sanitize(grp.scheme),
			sanitize(strings.Trim(grp.path, "/")),
		)
		result[name] = tgts
	}
	return result
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
		parts := []string{}
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

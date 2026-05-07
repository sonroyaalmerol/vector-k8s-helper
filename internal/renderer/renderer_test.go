package renderer

import (
	"testing"

	"github.com/sgl/vector-k8s-helper/internal/discovery"
	"gopkg.in/yaml.v3"
)

func TestRenderEmpty(t *testing.T) {
	cfg := Config{
		ScrapeIntervalSecs: 30,
	}
	data, err := RenderEmpty(cfg)
	if err != nil {
		t.Fatalf("RenderEmpty: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Sources) != 1 {
		t.Errorf("expected 1 source, got %d", len(result.Sources))
	}
}

func TestRenderWithTargets(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:      "pod_production_myapp_app",
			URL:       "http://10.0.0.5:9090/metrics",
			Namespace: "production",
			Pod:       "myapp-abc123",
			Node:      "node-1",
			Container: "app",
			Instance:  "10.0.0.5:9090",
		},
		{
			Name:      "svc_production_myapp",
			URL:       "http://10.96.0.50:8080/metrics",
			Namespace: "production",
			Service:   "myapp",
			Instance:  "10.96.0.50:8080",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ClusterLabel:       "dmz-prod-1",
	}

	data, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(result.Sources) == 0 {
		t.Error("expected at least 1 source")
	}

	// Verify transform exists with cluster label.
	transform, ok := result.Transforms["enrich_metrics"]
	if !ok {
		t.Fatal("expected enrich_metrics transform")
	}
	if transform.Type != "remap" {
		t.Errorf("expected remap transform, got %s", transform.Type)
	}
	if len(transform.Inputs) == 0 {
		t.Error("expected transform to have inputs")
	}
	if transform.Source == "" {
		t.Error("expected remap source to be non-empty")
	}
}

func TestRenderNoTargetsProducesValidConfig(t *testing.T) {
	cfg := Config{
		ScrapeIntervalSecs: 30,
	}
	data, err := Render(nil, cfg)
	if err != nil {
		t.Fatalf("Render with nil targets: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// With no targets we should still get valid YAML with the no_targets fallback.
	if len(result.Sources) == 0 {
		t.Error("expected at least 1 source in fallback config")
	}
}

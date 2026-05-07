package renderer

import (
	"testing"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/discovery"
	"gopkg.in/yaml.v3"
)

func TestRenderEmpty(t *testing.T) {
	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
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
	// Verify scrape_timeout_secs is set.
	for _, src := range result.Sources {
		if src.ScrapeTimeoutSecs != 10 {
			t.Errorf("expected scrape_timeout_secs=10, got %v", src.ScrapeTimeoutSecs)
		}
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
			Name:      "ep_production_myapp_10_0_1_5",
			URL:       "http://10.0.1.5:9090/metrics",
			Namespace: "production",
			Service:   "myapp",
			Pod:       "myapp-abc123",
			Instance:  "10.0.1.5:9090",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
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

	// Verify scrape_timeout_secs and honor_labels on sources.
	for name, src := range result.Sources {
		if src.ScrapeTimeoutSecs != 10 {
			t.Errorf("source %s: expected scrape_timeout_secs=10, got %v", name, src.ScrapeTimeoutSecs)
		}
	}
}

func TestRenderNoTargetsProducesValidConfig(t *testing.T) {
	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
	}
	data, err := Render(nil, cfg)
	if err != nil {
		t.Fatalf("Render with nil targets: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Sources) == 0 {
		t.Error("expected at least 1 source in fallback config")
	}
}

func TestRenderHonorLabels(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:      "pod_test_app",
			URL:       "http://10.0.0.1:9090/metrics",
			Namespace: "default",
			Pod:       "test-pod",
			Instance:  "10.0.0.1:9090",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
		HonorLabels:        true,
	}

	data, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, src := range result.Sources {
		if !src.HonorLabels {
			t.Error("expected honor_labels=true")
		}
	}
}

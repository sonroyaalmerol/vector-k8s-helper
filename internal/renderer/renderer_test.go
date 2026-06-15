package renderer

import (
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(transform.Source, "metadata = {") {
		t.Errorf("expected metadata table in remap, got:\n%s", transform.Source)
	}
	if !strings.Contains(transform.Source, `.tags.cluster = "dmz-prod-1"`) {
		t.Errorf("expected cluster label assignment in remap")
	}

	for _, src := range result.Sources {
		if src.ScrapeTimeoutSecs != 10 {
			t.Errorf("source: expected scrape_timeout_secs=10, got %v", src.ScrapeTimeoutSecs)
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

func TestRenderWithAuth(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:              "pod_production_myapp_app",
			URL:               "http://10.0.0.5:9090/metrics",
			Namespace:         "production",
			Pod:               "myapp",
			Instance:          "10.0.0.5:9090",
			BasicAuthUserEnv:  "MY_USER",
			BasicAuthPassword: "MY_PASS",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
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
		if src.Auth == nil {
			t.Fatal("expected auth config in source")
		}
		if src.Auth.Strategy != "basic" {
			t.Errorf("expected auth strategy basic, got %s", src.Auth.Strategy)
		}
		if src.Auth.User != "${MY_USER}" {
			t.Errorf("expected user ${MY_USER}, got %s", src.Auth.User)
		}
		if src.Auth.Password != "${MY_PASS}" {
			t.Errorf("expected password ${MY_PASS}, got %s", src.Auth.Password)
		}
		if src.Auth.Token != "" {
			t.Errorf("expected empty token for basic auth, got %s", src.Auth.Token)
		}
	}

	if !strings.Contains(string(data), "strategy: basic") {
		t.Errorf("expected flat auth strategy in YAML, got:\n%s", data)
	}
	if strings.Contains(string(data), "basic:") {
		t.Errorf("auth must not use nested basic block, got:\n%s", data)
	}
}

func TestRenderWithTLS(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:                  "pod_production_myapp_app",
			URL:                   "https://10.0.0.5:8443/metrics",
			Namespace:             "production",
			Pod:                   "myapp",
			Instance:              "10.0.0.5:8443",
			TLSServerName:         "my-server",
			TLSInsecureSkipVerify: true,
			TLSCAFile:             "/etc/tls/ca.crt",
			TLSCertFile:           "/etc/tls/client.crt",
			TLSKeyFile:            "/etc/tls/client.key",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
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
		if src.TLS == nil {
			t.Fatal("expected TLS config in source")
		}
		if src.TLS.ServerName != "my-server" {
			t.Errorf("expected server_name my-server, got %s", src.TLS.ServerName)
		}
		if src.TLS.CAFile != "/etc/tls/ca.crt" {
			t.Errorf("expected ca_file /etc/tls/ca.crt, got %s", src.TLS.CAFile)
		}
		if src.TLS.CrtFile != "/etc/tls/client.crt" {
			t.Errorf("expected crt_file /etc/tls/client.crt, got %s", src.TLS.CrtFile)
		}
		if src.TLS.KeyFile != "/etc/tls/client.key" {
			t.Errorf("expected key_file /etc/tls/client.key, got %s", src.TLS.KeyFile)
		}
		if src.TLS.VerifyCertificate == nil || *src.TLS.VerifyCertificate != false {
			t.Error("expected verify_certificate=false for insecure")
		}
	}
}

func TestRenderWithBearerToken(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:        "pod_production_myapp_app",
			URL:         "http://10.0.0.5:9090/metrics",
			Namespace:   "production",
			Pod:         "myapp",
			Instance:    "10.0.0.5:9090",
			BearerToken: "my-token",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
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
		if src.Auth == nil {
			t.Fatal("expected auth config in source")
		}
		if src.Auth.Strategy != "bearer" {
			t.Errorf("expected auth strategy bearer, got %s", src.Auth.Strategy)
		}
		if src.Auth.Token != "my-token" {
			t.Errorf("expected token my-token, got %s", src.Auth.Token)
		}
	}
}

func TestRenderNoProxyField(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:      "pod_production_myapp_app",
			URL:       "http://10.0.0.5:9090/metrics",
			Namespace: "production",
			Pod:       "myapp",
			Instance:  "10.0.0.5:9090",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
	}

	data, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	if strings.Contains(string(data), "proxy") {
		t.Errorf("proxy must not appear in generated config, got:\n%s", data)
	}
}

func TestRenderWithCustomInterval(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:           "pod_production_myapp_app",
			URL:            "http://10.0.0.5:9090/metrics",
			Namespace:      "production",
			Pod:            "myapp",
			Instance:       "10.0.0.5:9090",
			ScrapeInterval: 15 * time.Second,
			ScrapeTimeout:  5 * time.Second,
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
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
		if src.ScrapeIntervalSecs != 15 {
			t.Errorf("expected scrape_interval_secs=15, got %v", src.ScrapeIntervalSecs)
		}
		if src.ScrapeTimeoutSecs != 5 {
			t.Errorf("expected scrape_timeout_secs=5, got %v", src.ScrapeTimeoutSecs)
		}
	}
}

func TestRenderSeparatesDifferentAuth(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:              "pod_a",
			URL:               "http://10.0.0.1:9090/metrics",
			Namespace:         "default",
			Pod:               "pod-a",
			Instance:          "10.0.0.1:9090",
			BasicAuthUserEnv:  "USER_A",
			BasicAuthPassword: "PASS_A",
		},
		{
			Name:              "pod_b",
			URL:               "http://10.0.0.2:9090/metrics",
			Namespace:         "default",
			Pod:               "pod-b",
			Instance:          "10.0.0.2:9090",
			BasicAuthUserEnv:  "USER_B",
			BasicAuthPassword: "PASS_B",
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
	}

	data, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(result.Sources) < 2 {
		t.Errorf("expected at least 2 sources for different auth, got %d", len(result.Sources))
	}
}

func TestRenderMetadataIncludesLabels(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:      "pod_app",
			URL:       "http://10.0.0.5:9090/metrics",
			Namespace: "production",
			Pod:       "myapp",
			Instance:  "10.0.0.5:9090",
			Role:      "pod",
			Labels: map[string]string{
				"pod_label_app": "myapp",
			},
		},
	}

	cfg := Config{
		ScrapeIntervalSecs: 30,
		ScrapeTimeoutSecs:  10,
	}

	data, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	var result VectorSources
	if err := yaml.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	transform := result.Transforms["enrich_metrics"]
	if !strings.Contains(transform.Source, `"pod_label_app": "myapp"`) {
		t.Errorf("expected pod label in metadata table, got:\n%s", transform.Source)
	}
	if !strings.Contains(transform.Source, ".tags = merge(.tags, m.labels)") {
		t.Errorf("expected labels merge in remap, got:\n%s", transform.Source)
	}
}

func TestRenderDeterministicOutput(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:      "pod_a",
			URL:       "http://10.0.0.1:9090/metrics",
			Namespace: "default",
			Pod:       "pod-a",
			Instance:  "10.0.0.1:9090",
			Labels:    map[string]string{"pod_label_z": "1", "pod_label_a": "2"},
		},
		{
			Name:      "pod_b",
			URL:       "http://10.0.0.2:9090/metrics",
			Namespace: "default",
			Pod:       "pod-b",
			Instance:  "10.0.0.2:9090",
		},
	}

	cfg := Config{ScrapeIntervalSecs: 30, ScrapeTimeoutSecs: 10}

	first, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render first: %v", err)
	}
	second, err := Render(targets, cfg)
	if err != nil {
		t.Fatalf("Render second: %v", err)
	}
	if string(first) != string(second) {
		t.Errorf("Render output is not deterministic")
	}
}

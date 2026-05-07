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

func TestRenderWithJobAnnotation(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:      "pod_production_myapp_app",
			URL:       "http://10.0.0.5:9090/metrics",
			Namespace: "production",
			Pod:       "myapp-abc123",
			Node:      "node-1",
			Instance:  "10.0.0.5:9090",
			Job:       "my-job",
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
	if transform.Source == "" {
		t.Fatal("expected non-empty remap source")
	}
	// Job label should be in the VRL source.
	source := transform.Source
	if !containsStr(source, `"job": "my-job"`) {
		t.Errorf("expected job label in remap source, got:\n%s", source)
	}
}

func TestRenderWithAuth(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:                    "pod_production_myapp_app",
			URL:                     "http://10.0.0.5:9090/metrics",
			Namespace:               "production",
			Pod:                     "myapp",
			Instance:                "10.0.0.5:9090",
			BasicAuthUserEnvVar:     "MY_USER",
			BasicAuthPasswordEnvVar: "MY_PASS",
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
		if src.Auth.Basic == nil {
			t.Fatal("expected basic auth config")
		}
		if src.Auth.Basic.User != "${MY_USER}" {
			t.Errorf("expected user ${MY_USER}, got %s", src.Auth.Basic.User)
		}
		if src.Auth.Basic.Password != "${MY_PASS}" {
			t.Errorf("expected password ${MY_PASS}, got %s", src.Auth.Basic.Password)
		}
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
	}
}

func TestRenderWithProxy(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:         "pod_production_myapp_app",
			URL:          "http://10.0.0.5:9090/metrics",
			Namespace:    "production",
			Pod:          "myapp",
			Instance:     "10.0.0.5:9090",
			HTTPProxyURL: "http://proxy:3128",
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
		if src.Proxy == nil {
			t.Fatal("expected proxy config in source")
		}
		if src.Proxy.HTTP != "http://proxy:3128" {
			t.Errorf("expected proxy http://proxy:3128, got %s", src.Proxy.HTTP)
		}
	}
}

func TestRenderWithBearerToken(t *testing.T) {
	targets := []discovery.Target{
		{
			Name:                 "pod_production_myapp_app",
			URL:                  "http://10.0.0.5:9090/metrics",
			Namespace:            "production",
			Pod:                  "myapp",
			Instance:             "10.0.0.5:9090",
			ServiceAccountBearer: "my-token",
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
		if src.Auth.Bearer == nil {
			t.Fatal("expected bearer auth config")
		}
		if src.Auth.Bearer.Token != "my-token" {
			t.Errorf("expected token my-token, got %s", src.Auth.Bearer.Token)
		}
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
	// Targets with different auth settings should be in separate sources.
	targets := []discovery.Target{
		{
			Name:                    "pod_a",
			URL:                     "http://10.0.0.1:9090/metrics",
			Namespace:               "default",
			Pod:                     "pod-a",
			Instance:                "10.0.0.1:9090",
			BasicAuthUserEnvVar:     "USER_A",
			BasicAuthPasswordEnvVar: "PASS_A",
		},
		{
			Name:                    "pod_b",
			URL:                     "http://10.0.0.2:9090/metrics",
			Namespace:               "default",
			Pod:                     "pod-b",
			Instance:                "10.0.0.2:9090",
			BasicAuthUserEnvVar:     "USER_B",
			BasicAuthPasswordEnvVar: "PASS_B",
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

	// Different auth should produce separate sources.
	if len(result.Sources) < 2 {
		t.Errorf("expected at least 2 sources for different auth, got %d", len(result.Sources))
	}
}

func containsStr(s, substr string) bool {
	return strings.Contains(s, substr)
}

package renderer

import (
	"testing"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/discovery"
)

func makeRenderTargets(n int) []discovery.Target {
	out := make([]discovery.Target, n)
	for i := range out {
		out[i] = discovery.Target{
			Name:      "pod_default_app",
			URL:       "http://10.0.0.1:9090/metrics",
			Instance:  "10.0.0.1:9090",
			Role:      "pod",
			Namespace: "default",
			Pod:       "app",
			Node:      "node-1",
			Labels:    map[string]string{"pod_label_app": "myapp"},
		}
	}
	return out
}

func BenchmarkRender(b *testing.B) {
	targets := makeRenderTargets(500)
	cfg := Config{ScrapeIntervalSecs: 30, ScrapeTimeoutSecs: 10, ClusterLabel: "dmz-prod-1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Render(targets, cfg)
	}
}

func BenchmarkBuildRemapSource(b *testing.B) {
	targets := makeRenderTargets(500)
	cfg := Config{ScrapeIntervalSecs: 30, ScrapeTimeoutSecs: 10, ClusterLabel: "dmz-prod-1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildRemapSource(targets, cfg)
	}
}

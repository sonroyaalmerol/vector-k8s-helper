package discovery

import (
	"testing"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func makeBenchTargets(n int) []Target {
	out := make([]Target, n)
	for i := range out {
		out[i] = Target{
			Name:      "pod_default_app",
			URL:       "http://10.0.0.1:9090/metrics",
			Instance:  "10.0.0.1:9090",
			Role:      "pod",
			Namespace: "default",
			Pod:       "app",
			Node:      "node-1",
		}
	}
	return out
}

func BenchmarkSummarizeTargets(b *testing.B) {
	targets := makeBenchTargets(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SummarizeTargets(targets)
	}
}

func BenchmarkTargetsFromPod(b *testing.B) {
	cfg := config.Config{AnnotationPrefix: "prometheus.io", IncludeLabels: true}
	k := buildKeys(cfg)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bench-pod",
			Namespace: "default",
			Labels:    map[string]string{"app": "bench", "team": "infra"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{
				{
					Name: "web",
					Ports: []corev1.ContainerPort{
						{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
						{Name: "grpc", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
					},
				},
			},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.21", Phase: corev1.PodRunning},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = targetsFromPod(pod, cfg, k)
	}
}

package discovery

import (
	"testing"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)


func TestTargetsFromPod(t *testing.T) {
	tests := []struct {
		name    string
		pod     *corev1.Pod
		want    int
		wantURL string
	}{
		{
			name: "annotated_pod_with_port",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "myapp-abc123",
					Namespace: "production",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
						config.AnnotationPort:   "9090",
						config.AnnotationPath:   "/metrics",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node-1",
					Containers: []corev1.Container{
						{
							Name: "app",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 8080},
							},
						},
					},
				},
				Status: corev1.PodStatus{
					PodIP: "10.0.0.5",
				},
			},
			want:    1,
			wantURL: "http://10.0.0.5:9090/metrics",
		},
		{
			name: "pod_without_annotation",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-annotation",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "app"},
					},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.1"},
			},
			want: 0,
		},
		{
			name: "excluded_pod",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "excluded-pod",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
						"vector.dev/exclude":    "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app"}},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.2"},
			},
			want: 0,
		},
		{
			name: "pod_with_https_scheme",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "secure-app",
					Namespace: "monitoring",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
						config.AnnotationScheme: "https",
						config.AnnotationPort:   "8443",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node-2",
					Containers: []corev1.Container{
						{Name: "app"},
					},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.10"},
			},
			want:    1,
			wantURL: "https://10.0.0.10:8443/metrics",
		},
		{
			name: "pod_without_ip",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pending-pod",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app"}},
				},
				Status: corev1.PodStatus{PodIP: ""},
			},
			want: 0,
		},
		{
			name: "pod_default_port_from_container",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default-port",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
					},
				},
				Spec: corev1.PodSpec{
					NodeName: "node-3",
					Containers: []corev1.Container{
						{
							Name: "web",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 8080},
							},
						},
					},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.20"},
			},
			want:    1,
			wantURL: "http://10.0.0.20:8080/metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targets := targetsFromPod(tt.pod)
			if len(targets) != tt.want {
				t.Errorf("got %d targets, want %d", len(targets), tt.want)
			}
			if tt.want > 0 && tt.wantURL != "" {
				if targets[0].URL != tt.wantURL {
					t.Errorf("got URL %q, want %q", targets[0].URL, tt.wantURL)
				}
			}
		})
	}
}

func TestTargetsFromEndpointSlice(t *testing.T) {
	tests := []struct {
		name    string
		epSlice *discoveryv1.EndpointSlice
		svc     *corev1.Service
		want    int
		wantURL string
		wantPod string
		wantSvc string
	}{
		{
			name: "annotated_service_with_ready_endpoints",
			epSlice: &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "myapp-abc",
					Namespace: "production",
					Labels: map[string]string{
						discoveryv1.LabelServiceName: "myapp",
					},
				},
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses: []string{"10.0.1.5"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: new(true),
						},
						TargetRef: &corev1.ObjectReference{
							Kind: "Pod",
							Name: "myapp-abc123",
						},
					},
					{
						Addresses: []string{"10.0.1.6"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: new(true),
						},
						TargetRef: &corev1.ObjectReference{
							Kind: "Pod",
							Name: "myapp-def456",
						},
					},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "myapp",
					Namespace: "production",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
						config.AnnotationPort:   "9090",
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.0.50",
					Ports: []corev1.ServicePort{
						{Port: 80},
					},
				},
			},
			want:    2,
			wantURL: "http://10.0.1.5:9090/metrics",
			wantPod: "myapp-abc123",
			wantSvc: "myapp",
		},
		{
			name: "service_without_scrape_annotation",
			epSlice: &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-scrape-abc",
					Namespace: "default",
					Labels: map[string]string{
						discoveryv1.LabelServiceName: "no-scrape",
					},
				},
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses: []string{"10.0.0.1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: new(true),
						},
					},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-scrape",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.0.1",
				},
			},
			want: 0,
		},
		{
			name: "endpoints_with_not_ready",
			epSlice: &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "notready-abc",
					Namespace: "default",
					Labels: map[string]string{
						discoveryv1.LabelServiceName: "notready",
					},
				},
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses: []string{"10.0.0.1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: new(false),
						},
					},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "notready",
					Namespace: "default",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.0.2",
					Ports: []corev1.ServicePort{
						{Port: 8080},
					},
				},
			},
			want: 0,
		},
		{
			name: "service_with_https_and_custom_path",
			epSlice: &discoveryv1.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "secure-svc-abc",
					Namespace: "monitoring",
					Labels: map[string]string{
						discoveryv1.LabelServiceName: "secure-svc",
					},
				},
				Endpoints: []discoveryv1.Endpoint{
					{
						Addresses: []string{"10.0.2.10"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: new(true),
						},
					},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "secure-svc",
					Namespace: "monitoring",
					Annotations: map[string]string{
						config.AnnotationScrape: "true",
						config.AnnotationScheme: "https",
						config.AnnotationPath:   "/custom/metrics",
						config.AnnotationPort:   "8443",
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "10.96.0.100",
					Ports: []corev1.ServicePort{
						{Port: 443},
					},
				},
			},
			want:    1,
			wantURL: "https://10.0.2.10:8443/custom/metrics",
			wantSvc: "secure-svc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targets := targetsFromEndpointSlice(tt.epSlice, tt.svc)
			if len(targets) != tt.want {
				t.Errorf("got %d targets, want %d", len(targets), tt.want)
			}
			if tt.want > 0 && tt.wantURL != "" {
				if targets[0].URL != tt.wantURL {
					t.Errorf("got URL %q, want %q", targets[0].URL, tt.wantURL)
				}
				if targets[0].Pod != tt.wantPod {
					t.Errorf("got Pod %q, want %q", targets[0].Pod, tt.wantPod)
				}
				if targets[0].Service != tt.wantSvc {
					t.Errorf("got Service %q, want %q", targets[0].Service, tt.wantSvc)
				}
			}
		})
	}
}

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"pod_kube_system_coredns", "pod_kube_system_coredns"},
		{"pod.monitor-app_v2", "pod_monitor_app_v2"},
		{"svc/production/api", "svc_production_api"},
		{"ep_10_0_1_5", "ep_10_0_1_5"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeName(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

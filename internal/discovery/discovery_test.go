package discovery

import (
	"testing"
	"time"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ptrBool(v bool) *bool { b := v; return &b }

func defaultConfig() config.Config {
	return config.Config{AnnotationPrefix: "prometheus.io", IncludeLabels: true}
}

func defaultKeys() keys { return buildKeys(defaultConfig()) }

func TestTargetsFromPod(t *testing.T) {
	cfg := defaultConfig()
	k := defaultKeys()

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
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9090",
						"prometheus.io/path":   "/metrics",
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
				Status: corev1.PodStatus{PodIP: "10.0.0.5"},
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
					Containers: []corev1.Container{{Name: "app"}},
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
						"prometheus.io/scrape": "true",
						"vector.dev/exclude":   "true",
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
						"prometheus.io/scrape": "true",
						"prometheus.io/scheme": "https",
						"prometheus.io/port":   "8443",
					},
				},
				Spec: corev1.PodSpec{
					NodeName:   "node-2",
					Containers: []corev1.Container{{Name: "app"}},
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
						"prometheus.io/scrape": "true",
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
						"prometheus.io/scrape": "true",
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
		{
			name: "pod_multiple_ports",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multiport",
					Namespace: "default",
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "web",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 8080},
								{ContainerPort: 9090},
							},
						},
					},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.21"},
			},
			want: 2,
		},
		{
			name: "pod_with_params",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "params-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9090",
						"prometheus.io/params": "key=val&other=123",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app"}},
				},
				Status: corev1.PodStatus{PodIP: "10.0.0.1"},
			},
			want:    1,
			wantURL: "http://10.0.0.1:9090/metrics?key=val&other=123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targets := targetsFromPod(tt.pod, cfg, k)
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

func TestTargetsFromPodExtendedAuth(t *testing.T) {
	cfg := defaultConfig()
	k := defaultKeys()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "auth-pod",
			Namespace: "monitoring",
			Annotations: map[string]string{
				"prometheus.io/scrape":                      "true",
				"prometheus.io/port":                        "9090",
				"prometheus.io/httpBasicAuthUsernameEnvVar": "MY_USER",
				"prometheus.io/httpBasicAuthPasswordEnvVar": "MY_PASS",
				"prometheus.io/tlsCAFile":                   "/etc/tls/ca.crt",
				"prometheus.io/tlsCertFile":                 "/etc/tls/client.crt",
				"prometheus.io/tlsKeyFile":                  "/etc/tls/client.key",
				"prometheus.io/collectionInterval":          "15s",
				"prometheus.io/collectionTimeout":           "5s",
				"prometheus.io/serviceAccountBearerToken":   "my-token",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-1",
			Containers: []corev1.Container{{Name: "app"}},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.5"},
	}

	targets := targetsFromPod(pod, cfg, k)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	tgt := targets[0]
	if tgt.BasicAuthUserEnv != "MY_USER" {
		t.Errorf("BasicAuthUserEnv = %q, want MY_USER", tgt.BasicAuthUserEnv)
	}
	if tgt.BasicAuthPassword != "MY_PASS" {
		t.Errorf("BasicAuthPassword = %q, want MY_PASS", tgt.BasicAuthPassword)
	}
	if tgt.TLSCAFile != "/etc/tls/ca.crt" {
		t.Errorf("TLSCAFile = %q, want /etc/tls/ca.crt", tgt.TLSCAFile)
	}
	if tgt.TLSCertFile != "/etc/tls/client.crt" {
		t.Errorf("TLSCertFile = %q, want /etc/tls/client.crt", tgt.TLSCertFile)
	}
	if tgt.TLSKeyFile != "/etc/tls/client.key" {
		t.Errorf("TLSKeyFile = %q, want /etc/tls/client.key", tgt.TLSKeyFile)
	}
	if tgt.ScrapeInterval != 15*time.Second {
		t.Errorf("ScrapeInterval = %v, want 15s", tgt.ScrapeInterval)
	}
	if tgt.ScrapeTimeout != 5*time.Second {
		t.Errorf("ScrapeTimeout = %v, want 5s", tgt.ScrapeTimeout)
	}
	if tgt.BearerToken != "my-token" {
		t.Errorf("BearerToken = %q, want my-token", tgt.BearerToken)
	}
}

func TestTargetsFromPodCustomPrefix(t *testing.T) {
	cfg := config.Config{AnnotationPrefix: "custom.io", IncludeLabels: true}
	k := buildKeys(cfg)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prefixed-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"custom.io/scrape":     "true",
				"custom.io/port":       "9090",
				"prometheus.io/scrape": "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app"}},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.5"},
	}

	targets := targetsFromPod(pod, cfg, k)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target with custom prefix, got %d", len(targets))
	}
	if targets[0].URL != "http://10.0.0.5:9090/metrics" {
		t.Errorf("URL = %q, want http://10.0.0.5:9090/metrics", targets[0].URL)
	}
}

func TestTargetsFromEndpointSlice(t *testing.T) {
	cfg := defaultConfig()
	k := defaultKeys()

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
							Ready: ptrBool(true),
						},
						TargetRef: &corev1.ObjectReference{
							Kind: "Pod",
							Name: "myapp-abc123",
						},
					},
					{
						Addresses: []string{"10.0.1.6"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptrBool(true),
						},
						TargetRef: &corev1.ObjectReference{
							Kind: "Pod",
							Name: "myapp-def456",
						},
					},
				},
				Ports: []discoveryv1.EndpointPort{
					{Port: new(int32(9090))},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "myapp",
					Namespace: "production",
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9090",
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
							Ready: ptrBool(true),
						},
					},
				},
				Ports: []discoveryv1.EndpointPort{
					{Port: new(int32(80))},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "no-scrape",
					Namespace: "default",
				},
				Spec: corev1.ServiceSpec{ClusterIP: "10.96.0.1"},
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
							Ready: ptrBool(false),
						},
					},
				},
				Ports: []discoveryv1.EndpointPort{
					{Port: new(int32(8080))},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "notready",
					Namespace: "default",
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
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
							Ready: ptrBool(true),
						},
					},
				},
				Ports: []discoveryv1.EndpointPort{
					{Port: new(int32(8443))},
				},
			},
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "secure-svc",
					Namespace: "monitoring",
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/scheme": "https",
						"prometheus.io/path":   "/custom/metrics",
						"prometheus.io/port":   "8443",
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
			wantSvc: "secure-svc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targets := targetsFromEndpointSlice(tt.epSlice, tt.svc, cfg, k, nil, nil, nil)
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

func TestTargetsFromService(t *testing.T) {
	cfg := defaultConfig()
	k := defaultKeys()

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "myapp",
			Namespace: "production",
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "9090",
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.96.0.50",
			Ports: []corev1.ServicePort{
				{Port: 80},
			},
		},
	}

	targets := targetsFromService(svc, cfg, k, nil)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].URL != "http://10.96.0.50:9090/metrics" {
		t.Errorf("URL = %q, want http://10.96.0.50:9090/metrics", targets[0].URL)
	}
	if targets[0].Role != "service" {
		t.Errorf("Role = %q, want service", targets[0].Role)
	}
}

func TestTargetsFromNode(t *testing.T) {
	cfg := config.Config{AnnotationPrefix: "prometheus.io", IncludeLabels: true, NodeScrapePort: 10250}
	k := buildKeys(cfg)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
			},
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
			},
		},
	}

	targets := targetsFromNode(node, cfg, k)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].URL != "https://10.0.0.1:10250/metrics" {
		t.Errorf("URL = %q, want https://10.0.0.1:10250/metrics", targets[0].URL)
	}
	if targets[0].Role != "node" {
		t.Errorf("Role = %q, want node", targets[0].Role)
	}
}

func TestTargetsFromIngress(t *testing.T) {
	cfg := defaultConfig()
	k := defaultKeys()

	host := "example.com"
	ing := &netv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mying",
			Namespace: "production",
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
			},
		},
		Spec: netv1.IngressSpec{
			Rules: []netv1.IngressRule{
				{Host: host},
			},
		},
	}

	targets := targetsFromIngress(ing, cfg, k)
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
	if targets[0].URL != "http://example.com:80/metrics" {
		t.Errorf("URL = %q, want http://example.com:80/metrics", targets[0].URL)
	}
	if targets[0].Role != "ingress" {
		t.Errorf("Role = %q, want ingress", targets[0].Role)
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

func TestBuildURL(t *testing.T) {
	tests := []struct {
		scheme, host, path, params, want string
	}{
		{"http", "10.0.0.5:9090", "/metrics", "", "http://10.0.0.5:9090/metrics"},
		{"http", "10.0.0.5:9090", "/metrics", "key=val", "http://10.0.0.5:9090/metrics?key=val"},
		{"https", "10.0.0.5:8443", "/custom", "a=1&b=2", "https://10.0.0.5:8443/custom?a=1&b=2"},
	}
	for _, tt := range tests {
		got := buildURL(tt.scheme, tt.host, tt.path, tt.params)
		if got != tt.want {
			t.Errorf("buildURL(%q,%q,%q,%q) = %q, want %q", tt.scheme, tt.host, tt.path, tt.params, got, tt.want)
		}
	}
}

func TestSanitizeMetaKey(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"app.kubernetes.io/name", "app_kubernetes_io_name"},
		{"app", "app"},
		{"Foo-Bar", "foo_bar"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeMetaKey(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeMetaKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

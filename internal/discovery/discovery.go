// Package discovery watches the Kubernetes API for pods and endpoint slices
// annotated with prometheus.io/scrape and produces Target lists.
package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Target represents a discovered scrape endpoint.
type Target struct {
	Name      string
	URL       string // Fully-qualified: scheme://host:port/path
	Namespace string
	Pod       string
	Service   string
	Node      string
	Container string
	Instance  string // host:port, used as VRL lookup key
}

// SanitizedName returns a DNS-safe identifier suitable for Vector source names.
func (t Target) SanitizedName() string {
	return sanitizeName(t.Name)
}

// Watcher watches Kubernetes pods and endpoint slices for scrape annotations
// and emits the full target list whenever the set changes.
type Watcher struct {
	cfg      config.Config
	client   kubernetes.Interface
	logger   *slog.Logger
	mu       sync.Mutex
	targets  map[string]Target
	output   chan []Target
	podStore cache.Store
	svcStore cache.Store
	epStore  cache.Store
}

// NewWatcher creates a Watcher using the provided K8s client and configuration.
func NewWatcher(client kubernetes.Interface, cfg config.Config, logger *slog.Logger) *Watcher {
	return &Watcher{
		cfg:     cfg,
		client:  client,
		logger:  logger,
		targets: make(map[string]Target),
		output:  make(chan []Target, 1),
	}
}

// Output returns a channel that receives the current target list on each change.
func (w *Watcher) Output() <-chan []Target {
	return w.output
}

// Run starts watching and blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	opts := []informers.SharedInformerOption{}
	if w.cfg.Namespace != "" {
		opts = append(opts, informers.WithNamespace(w.cfg.Namespace))
	}
	factory := informers.NewSharedInformerFactoryWithOptions(w.client, w.cfg.ResyncInterval, opts...)

	podInf := factory.Core().V1().Pods().Informer()
	svcInf := factory.Core().V1().Services().Informer()
	epInf := factory.Discovery().V1().EndpointSlices().Informer()

	w.podStore = podInf.GetStore()
	w.svcStore = svcInf.GetStore()
	w.epStore = epInf.GetStore()

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { w.reconcile() },
		UpdateFunc: func(_, _ any) { w.reconcile() },
		DeleteFunc: func(_ any) { w.reconcile() },
	}
	if _, err := podInf.AddEventHandler(handler); err != nil {
		return fmt.Errorf("failed to add pod handler: %w", err)
	}
	if _, err := svcInf.AddEventHandler(handler); err != nil {
		return fmt.Errorf("failed to add service handler: %w", err)
	}
	if _, err := epInf.AddEventHandler(handler); err != nil {
		return fmt.Errorf("failed to add endpoints handler: %w", err)
	}

	factory.Start(ctx.Done())
	w.logger.Info("started kubernetes informers", "namespace", cfgNamespace(w.cfg.Namespace))

	if !cache.WaitForCacheSync(ctx.Done(), podInf.HasSynced, svcInf.HasSynced, epInf.HasSynced) {
		return fmt.Errorf("informer cache sync cancelled")
	}
	w.logger.Info("informer cache synced")
	w.reconcile()

	<-ctx.Done()
	return ctx.Err()
}

func cfgNamespace(ns string) string {
	if ns == "" {
		return "<all>"
	}
	return ns
}

func (w *Watcher) reconcile() {
	w.mu.Lock()
	defer w.mu.Unlock()

	newTargets := make(map[string]Target)

	// Pod targets: pods with prometheus.io/scrape annotation.
	for _, obj := range w.podStore.List() {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		for _, t := range targetsFromPod(pod) {
			newTargets[t.Name] = t
		}
	}

	// Endpoint targets: services with prometheus.io/scrape annotated,
	// resolved to ready pod IPs via EndpointSlice objects.
	for _, obj := range w.epStore.List() {
		epSlice, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			continue
		}
		svcName := epSlice.Labels[discoveryv1.LabelServiceName]
		if svcName == "" {
			continue
		}
		// Look up the corresponding Service from the store.
		svcObj, exists, err := w.svcStore.GetByKey(fmt.Sprintf("%s/%s", epSlice.Namespace, svcName))
		if err != nil || !exists {
			continue
		}
		svc, ok := svcObj.(*corev1.Service)
		if !ok {
			continue
		}
		for _, t := range targetsFromEndpointSlice(epSlice, svc) {
			newTargets[t.Name] = t
		}
	}

	w.targets = newTargets
	w.emit()
}

func (w *Watcher) emit() {
	snapshot := make([]Target, 0, len(w.targets))
	for _, t := range w.targets {
		snapshot = append(snapshot, t)
	}

	select {
	case w.output <- snapshot:
	default:
		select {
		case <-w.output:
		default:
		}
		w.output <- snapshot
	}
}

// targetsFromPod extracts scrape targets from a Pod's annotations.
// This covers the Alloy discovery.relabel "pods" path.
func targetsFromPod(pod *corev1.Pod) []Target {
	ann := pod.Annotations
	if ann == nil {
		return nil
	}
	if ann["vector.dev/exclude"] == "true" {
		return nil
	}
	if !config.ParseBool(ann[config.AnnotationScrape], false) {
		return nil
	}
	if pod.Status.PodIP == "" {
		return nil
	}

	scheme := ann[config.AnnotationScheme]
	if scheme == "" {
		scheme = "http"
	}
	path := ann[config.AnnotationPath]
	if path == "" {
		path = "/metrics"
	}

	scrapePortStr := ann[config.AnnotationPort]

	var targets []Target
	for _, c := range pod.Spec.Containers {
		port := resolveContainerPort(c, scrapePortStr)
		if port == 0 {
			continue
		}

		hostPort := net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(port)))
		name := fmt.Sprintf("pod_%s_%s_%s", pod.Namespace, pod.Name, c.Name)

		targets = append(targets, Target{
			Name:      sanitizeName(name),
			URL:       fmt.Sprintf("%s://%s%s", scheme, hostPort, path),
			Namespace: pod.Namespace,
			Pod:       pod.Name,
			Node:      pod.Spec.NodeName,
			Container: c.Name,
			Instance:  hostPort,
		})
	}
	return targets
}

// targetsFromEndpointSlice extracts scrape targets from an EndpointSlice and
// its associated Service. This covers the Alloy discovery.relabel "endpoints"
// path — it resolves services to their ready pod IPs rather than the ClusterIP,
// matching Alloy's behavior of using endpoint role discovery.
func targetsFromEndpointSlice(epSlice *discoveryv1.EndpointSlice, svc *corev1.Service) []Target {
	ann := svc.Annotations
	if ann == nil {
		return nil
	}
	if !config.ParseBool(ann[config.AnnotationScrape], false) {
		return nil
	}

	scheme := ann[config.AnnotationScheme]
	if scheme == "" {
		scheme = "http"
	}
	path := ann[config.AnnotationPath]
	if path == "" {
		path = "/metrics"
	}

	portStr := ann[config.AnnotationPort]
	port := resolveServicePort(svc, portStr)
	if port == 0 {
		return nil
	}

	var targets []Target
	for _, ep := range epSlice.Endpoints {
		if ep.Conditions.Ready == nil || !*ep.Conditions.Ready {
			continue
		}
		for _, addr := range ep.Addresses {
			hostPort := net.JoinHostPort(addr, strconv.Itoa(int(port)))
			podName := ""
			if ep.TargetRef != nil && ep.TargetRef.Kind == "Pod" {
				podName = ep.TargetRef.Name
			}
			name := fmt.Sprintf("ep_%s_%s_%s", epSlice.Namespace, svc.Name, sanitizeIP(addr))
			targets = append(targets, Target{
				Name:      sanitizeName(name),
				URL:       fmt.Sprintf("%s://%s%s", scheme, hostPort, path),
				Namespace: epSlice.Namespace,
				Service:   svc.Name,
				Pod:       podName,
				Instance:  hostPort,
			})
		}
	}
	return targets
}

// resolveContainerPort finds the scrape port for a container.
// If scrapePortStr is set, it's used directly; otherwise the first container port.
func resolveContainerPort(c corev1.Container, scrapePortStr string) int32 {
	if scrapePortStr != "" {
		p, err := strconv.ParseInt(scrapePortStr, 10, 32)
		if err == nil && p > 0 {
			return int32(p)
		}
	}
	if len(c.Ports) == 0 {
		return 0
	}
	return c.Ports[0].ContainerPort
}

// resolveServicePort finds the scrape port for a service.
func resolveServicePort(svc *corev1.Service, portStr string) int32 {
	if portStr != "" {
		p, err := strconv.ParseInt(portStr, 10, 32)
		if err == nil && p > 0 {
			return int32(p)
		}
	}
	if len(svc.Spec.Ports) == 0 {
		return 0
	}
	return svc.Spec.Ports[0].Port
}

// sanitizeIP replaces dots and colons in IP addresses for use in names.
var sanitizeIPReplacer = strings.NewReplacer(".", "_", ":", "_")

func sanitizeIP(ip string) string {
	return sanitizeIPReplacer.Replace(ip)
}

// sanitizeName replaces characters invalid in Vector component names.
var sanitizeNameReplacer = strings.NewReplacer(".", "_", "-", "_", "/", "_")

func sanitizeName(name string) string {
	return sanitizeNameReplacer.Replace(name)
}

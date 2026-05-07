// Package discovery watches the Kubernetes API for pods and services
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

// Watcher watches Kubernetes pods and services for scrape annotations and
// emits the full target list whenever the set changes.
type Watcher struct {
	cfg      config.Config
	client   kubernetes.Interface
	logger   *slog.Logger
	mu       sync.Mutex
	targets  map[string]Target
	output   chan []Target
	podStore cache.Store
	svcStore cache.Store
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

	w.podStore = podInf.GetStore()
	w.svcStore = svcInf.GetStore()

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

	factory.Start(ctx.Done())
	w.logger.Info("started kubernetes informers")

	if !cache.WaitForCacheSync(ctx.Done(), podInf.HasSynced, svcInf.HasSynced) {
		return fmt.Errorf("informer cache sync cancelled")
	}
	w.logger.Info("informer cache synced")
	w.reconcile()

	<-ctx.Done()
	return ctx.Err()
}

func (w *Watcher) reconcile() {
	w.mu.Lock()
	defer w.mu.Unlock()

	newTargets := make(map[string]Target)

	for _, obj := range w.podStore.List() {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			continue
		}
		for _, t := range targetsFromPod(pod, w.cfg) {
			newTargets[t.Name] = t
		}
	}

	for _, obj := range w.svcStore.List() {
		svc, ok := obj.(*corev1.Service)
		if !ok {
			continue
		}
		for _, t := range targetsFromService(svc, w.cfg) {
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
		// Drain stale entry and resend.
		select {
		case <-w.output:
		default:
		}
		w.output <- snapshot
	}
}

// targetsFromPod extracts scrape targets from a Pod's annotations.
func targetsFromPod(pod *corev1.Pod, cfg config.Config) []Target {
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

// targetsFromService extracts scrape targets from a Service's annotations.
func targetsFromService(svc *corev1.Service, cfg config.Config) []Target {
	ann := svc.Annotations
	if ann == nil {
		return nil
	}
	if !config.ParseBool(ann[config.AnnotationScrape], false) {
		return nil
	}
	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
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

	hostPort := net.JoinHostPort(svc.Spec.ClusterIP, strconv.Itoa(int(port)))
	name := fmt.Sprintf("svc_%s_%s", svc.Namespace, svc.Name)

	return []Target{{
		Name:      sanitizeName(name),
		URL:       fmt.Sprintf("%s://%s%s", scheme, hostPort, path),
		Namespace: svc.Namespace,
		Service:   svc.Name,
		Instance:  hostPort,
	}}
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

// sanitizeName replaces characters invalid in Vector component names.
func sanitizeName(name string) string {
	r := strings.NewReplacer(
		".", "_",
		"-", "_",
		"/", "_",
	)
	return r.Replace(name)
}

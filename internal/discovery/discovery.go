package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type Target struct {
	Name      string
	URL       string
	Instance  string
	Role      string
	Namespace string
	Pod       string
	Service   string
	Node      string
	Container string
	Job       string

	PodUID          string
	PodPhase        string
	PodReady        string
	ControllerKind  string
	ControllerName  string
	ContainerImage  string
	ContainerID     string
	ContainerInit   string
	HostIP          string
	PodIP           string
	NodeIP          string
	ServicePortName string

	PortName     string
	PortNumber   string
	PortProtocol string

	ServiceType         string
	ServiceClusterIP    string
	ServiceExternalName string

	NodeProviderID string
	NodeAddresses  map[string]string

	EndpointReady       string
	EndpointHostname    string
	EndpointNodeName    string
	EndpointZone        string
	EndpointAddressType string
	EndpointSliceName   string

	IngressClassName string
	IngressPath      string
	IngressScheme    string

	ScrapeInterval time.Duration
	ScrapeTimeout  time.Duration
	Params         string

	BasicAuthUserEnv  string
	BasicAuthPassword string
	BearerTokenEnv    string
	BearerToken       string

	TLSServerName         string
	TLSInsecureSkipVerify bool
	TLSCAFile             string
	TLSCertFile           string
	TLSKeyFile            string

	Labels      map[string]string
	Annotations map[string]string
}

type TargetSummary struct {
	Total      int
	ByRole     map[string]int
	Namespaces map[string]int
	Sources    int
	Sample     []string
}

func SummarizeTargets(targets []Target) TargetSummary {
	s := TargetSummary{
		Total:      len(targets),
		ByRole:     make(map[string]int),
		Namespaces: make(map[string]int),
	}
	sources := make(map[string]struct{})
	sampleN := min(len(targets), 3)
	s.Sample = make([]string, 0, sampleN)
	for i, t := range targets {
		s.ByRole[t.Role]++
		s.Namespaces[t.Namespace]++
		sources[t.URL] = struct{}{}
		if i < sampleN {
			s.Sample = append(s.Sample, t.URL)
		}
	}
	s.Sources = len(sources)
	return s
}

type keys struct {
	scrape    string
	port      string
	path      string
	scheme    string
	params    string
	job       string
	interval  string
	timeout   string
	basicUser string
	basicPass string
	bearerEnv string
	svcBearer string
	tlsName   string
	tlsSkip   string
	tlsCA     string
	tlsCert   string
	tlsKey    string
	exclude   string
}

func buildKeys(cfg config.Config) keys {
	p := cfg.AnnotationPrefix + "/"
	return keys{
		scrape:    p + config.AnnotationScrape,
		port:      p + config.AnnotationPort,
		path:      p + config.AnnotationPath,
		scheme:    p + config.AnnotationScheme,
		params:    p + config.AnnotationParams,
		job:       p + config.AnnotationJob,
		interval:  p + config.AnnotationCollectionInterval,
		timeout:   p + config.AnnotationCollectionTimeout,
		basicUser: p + config.AnnotationHTTPBasicAuthUsernameEnv,
		basicPass: p + config.AnnotationHTTPBasicAuthPasswordEnv,
		bearerEnv: p + config.AnnotationHTTPBearerTokenEnv,
		svcBearer: p + config.AnnotationServiceAccountBearerToken,
		tlsName:   p + config.AnnotationTLSServerName,
		tlsSkip:   p + config.AnnotationTLSInsecureSkipVerify,
		tlsCA:     p + config.AnnotationTLSCAFile,
		tlsCert:   p + config.AnnotationTLSCertFile,
		tlsKey:    p + config.AnnotationTLSKeyFile,
		exclude:   config.ExclusionAnnotation,
	}
}

type Watcher struct {
	cfg    config.Config
	client kubernetes.Interface
	logger *slog.Logger
	keys   keys
	ctx    context.Context

	mu      sync.Mutex
	targets map[string]Target
	output  chan []Target
	trigger chan struct{}

	podStore       cache.Store
	svcStore       cache.Store
	epStore        cache.Store
	endpointsStore cache.Store
	nodeStore      cache.Store
	ingressStore   cache.Store
	namespaceStore cache.Store
}

func NewWatcher(client kubernetes.Interface, cfg config.Config, logger *slog.Logger) *Watcher {
	return &Watcher{
		cfg:     cfg,
		client:  client,
		logger:  logger,
		keys:    buildKeys(cfg),
		targets: make(map[string]Target),
		output:  make(chan []Target, 1),
		trigger: make(chan struct{}, 1),
	}
}

func (w *Watcher) Output() <-chan []Target {
	return w.output
}

func (w *Watcher) Run(ctx context.Context) error {
	w.ctx = ctx

	informers := w.buildInformers()
	if len(informers) == 0 {
		return fmt.Errorf("no discovery roles enabled")
	}

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { w.scheduleReconcile() },
		UpdateFunc: func(_, _ any) { w.scheduleReconcile() },
		DeleteFunc: func(_ any) { w.scheduleReconcile() },
	}

	var synced []cache.InformerSynced
	for label, inf := range informers {
		if _, err := inf.AddEventHandler(handler); err != nil {
			return fmt.Errorf("failed to add %s handler: %w", label, err)
		}
		go inf.Run(ctx.Done())
		synced = append(synced, inf.HasSynced)
	}

	w.logger.Info("started kubernetes informers",
		"roles", w.cfg.Roles.String(),
		"namespace", "<all>")

	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		return fmt.Errorf("informer cache sync cancelled")
	}
	w.logger.Info("informer cache synced")
	w.reconcile()

	w.reconcileLoop(ctx)
	return ctx.Err()
}

func (w *Watcher) buildInformers() map[string]cache.SharedInformer {
	out := make(map[string]cache.SharedInformer, 6)
	if w.cfg.Roles.Pod {
		out["pod"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.PodLabel, w.cfg.Selectors.PodField
				return w.client.CoreV1().Pods("").List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.PodLabel, w.cfg.Selectors.PodField
				return w.client.CoreV1().Pods("").Watch(w.ctx, o)
			},
			&corev1.Pod{}, func(s cache.Store) { w.podStore = s })
	}
	if w.cfg.Roles.EndpointSlice || w.cfg.Roles.ServiceAddress || w.cfg.Roles.Endpoints {
		out["service"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.ServiceLabel, w.cfg.Selectors.ServiceField
				return w.client.CoreV1().Services("").List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.ServiceLabel, w.cfg.Selectors.ServiceField
				return w.client.CoreV1().Services("").Watch(w.ctx, o)
			},
			&corev1.Service{}, func(s cache.Store) { w.svcStore = s })
	}
	if w.cfg.Roles.EndpointSlice {
		out["endpointslice"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.EndpointSliceLabel, w.cfg.Selectors.EndpointSliceField
				return w.client.DiscoveryV1().EndpointSlices("").List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.EndpointSliceLabel, w.cfg.Selectors.EndpointSliceField
				return w.client.DiscoveryV1().EndpointSlices("").Watch(w.ctx, o)
			},
			&discoveryv1.EndpointSlice{}, func(s cache.Store) { w.epStore = s })
	}
	if w.cfg.Roles.Endpoints {
		out["endpoints"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.EndpointsLabel, w.cfg.Selectors.EndpointsField
				return w.client.CoreV1().Endpoints("").List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.EndpointsLabel, w.cfg.Selectors.EndpointsField
				return w.client.CoreV1().Endpoints("").Watch(w.ctx, o)
			},
			&corev1.Endpoints{}, func(s cache.Store) { w.endpointsStore = s }) //nolint:staticcheck // legacy endpoints role for parity
	}
	if w.cfg.Roles.Node {
		out["node"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.NodeLabel, w.cfg.Selectors.NodeField
				return w.client.CoreV1().Nodes().List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.NodeLabel, w.cfg.Selectors.NodeField
				return w.client.CoreV1().Nodes().Watch(w.ctx, o)
			},
			&corev1.Node{}, func(s cache.Store) { w.nodeStore = s })
	}
	if w.cfg.Roles.Ingress {
		out["ingress"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.IngressLabel, w.cfg.Selectors.IngressField
				return w.client.NetworkingV1().Ingresses("").List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				o.LabelSelector, o.FieldSelector = w.cfg.Selectors.IngressLabel, w.cfg.Selectors.IngressField
				return w.client.NetworkingV1().Ingresses("").Watch(w.ctx, o)
			},
			&netv1.Ingress{}, func(s cache.Store) { w.ingressStore = s })
	}
	if w.cfg.AttachNsMetadata && w.cfg.Roles.Any() {
		out["namespace"] = w.newInformer(
			func(o metav1.ListOptions) (runtime.Object, error) {
				return w.client.CoreV1().Namespaces().List(w.ctx, o)
			},
			func(o metav1.ListOptions) (watch.Interface, error) {
				return w.client.CoreV1().Namespaces().Watch(w.ctx, o)
			},
			&corev1.Namespace{}, func(s cache.Store) { w.namespaceStore = s })
	}
	return out
}

func (w *Watcher) newInformer(listFn func(metav1.ListOptions) (runtime.Object, error),
	watchFn func(metav1.ListOptions) (watch.Interface, error),
	objType runtime.Object, setStore func(cache.Store)) cache.SharedInformer {
	lw := &cache.ListWatch{ListFunc: listFn, WatchFunc: watchFn}
	inf := cache.NewSharedInformer(lw, objType, w.cfg.ResyncInterval)
	setStore(inf.GetStore())
	return inf
}

func (w *Watcher) scheduleReconcile() {
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

func (w *Watcher) reconcileLoop(ctx context.Context) {
	debounce := w.cfg.DebounceInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.trigger:
			if debounce > 0 {
				timer := time.NewTimer(debounce)
			drain:
				for {
					select {
					case <-w.trigger:
						if !timer.Stop() {
							<-timer.C
						}
						timer.Reset(debounce)
					case <-timer.C:
						break drain
					case <-ctx.Done():
						timer.Stop()
						return
					}
				}
			}
			w.reconcile()
		}
	}
}

func (w *Watcher) reconcile() {
	w.mu.Lock()
	defer w.mu.Unlock()

	k := w.keys
	newTargets := make(map[string]Target, len(w.targets))

	if w.cfg.Roles.Pod && w.podStore != nil {
		for _, obj := range w.podStore.List() {
			pod, ok := obj.(*corev1.Pod)
			if !ok || !w.namespaceOK(pod.Namespace) {
				continue
			}
			for _, t := range targetsFromPod(pod, w.cfg, k) {
				newTargets[t.Name] = t
			}
		}
	}

	if w.cfg.Roles.EndpointSlice && w.epStore != nil {
		for _, obj := range w.epStore.List() {
			ep, ok := obj.(*discoveryv1.EndpointSlice)
			if !ok || !w.namespaceOK(ep.Namespace) {
				continue
			}
			svcName := ep.Labels[discoveryv1.LabelServiceName]
			if svcName == "" {
				continue
			}
			svc, _ := w.lookupService(ep.Namespace, svcName)
			if svc == nil {
				continue
			}
			for _, t := range targetsFromEndpointSlice(ep, svc, w.cfg, k, w.podStore, w.nodeStore, w.namespaceStore) {
				newTargets[t.Name] = t
			}
		}
	}

	if w.cfg.Roles.ServiceAddress && w.svcStore != nil {
		for _, obj := range w.svcStore.List() {
			svc, ok := obj.(*corev1.Service)
			if !ok || !w.namespaceOK(svc.Namespace) {
				continue
			}
			for _, t := range targetsFromService(svc, w.cfg, k, w.namespaceStore) {
				newTargets[t.Name] = t
			}
		}
	}

	if w.cfg.Roles.Endpoints && w.endpointsStore != nil {
		for _, obj := range w.endpointsStore.List() {
			eps, ok := obj.(*corev1.Endpoints) //nolint:staticcheck // legacy endpoints role for parity
			if !ok || !w.namespaceOK(eps.Namespace) {
				continue
			}
			svc, _ := w.lookupService(eps.Namespace, eps.Name)
			for _, t := range targetsFromEndpoints(eps, svc, w.cfg, k, w.podStore, w.nodeStore, w.namespaceStore) {
				newTargets[t.Name] = t
			}
		}
	}

	if w.cfg.Roles.Node && w.nodeStore != nil {
		for _, obj := range w.nodeStore.List() {
			node, ok := obj.(*corev1.Node)
			if !ok {
				continue
			}
			for _, t := range targetsFromNode(node, w.cfg, k) {
				newTargets[t.Name] = t
			}
		}
	}

	if w.cfg.Roles.Ingress && w.ingressStore != nil {
		for _, obj := range w.ingressStore.List() {
			ing, ok := obj.(*netv1.Ingress)
			if !ok || !w.namespaceOK(ing.Namespace) {
				continue
			}
			for _, t := range targetsFromIngress(ing, w.cfg, k) {
				newTargets[t.Name] = t
			}
		}
	}

	if len(newTargets) == len(w.targets) {
		same := true
		for name := range newTargets {
			if _, ok := w.targets[name]; !ok {
				same = false
				break
			}
		}
		if same {
			return
		}
	}

	w.targets = newTargets
	w.emit()
}

func (w *Watcher) namespaceOK(namespace string) bool {
	return w.cfg.Namespaces.Allowed(namespace)
}

func (w *Watcher) lookupService(namespace, name string) (*corev1.Service, bool) {
	if w.svcStore == nil {
		return nil, false
	}
	obj, exists, err := w.svcStore.GetByKey(namespace + "/" + name)
	if err != nil || !exists {
		return nil, false
	}
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return nil, false
	}
	return svc, true
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

func scrapeEnabled(annotations map[string]string, k keys) bool {
	if annotations == nil {
		return false
	}
	if annotations[k.exclude] == "true" {
		return false
	}
	return config.ParseBool(annotations[k.scrape], false)
}

func applyScrapeSettings(t *Target, ann map[string]string, k keys) {
	t.Job = ann[k.job]
	t.ScrapeInterval = durStr(ann[k.interval], 0)
	t.ScrapeTimeout = durStr(ann[k.timeout], 0)
	t.Params = ann[k.params]

	t.BasicAuthUserEnv = ann[k.basicUser]
	t.BasicAuthPassword = ann[k.basicPass]
	t.BearerTokenEnv = ann[k.bearerEnv]
	t.BearerToken = ann[k.svcBearer]

	t.TLSServerName = ann[k.tlsName]
	t.TLSInsecureSkipVerify = config.ParseBool(ann[k.tlsSkip], false)
	t.TLSCAFile = ann[k.tlsCA]
	t.TLSCertFile = ann[k.tlsCert]
	t.TLSKeyFile = ann[k.tlsKey]
}

func metadataMaps(prefix string, labels, annotations map[string]string, cfg config.Config) (lblOut, annOut map[string]string) {
	if cfg.IncludeLabels && len(labels) > 0 {
		lblOut = make(map[string]string, len(labels)*2)
		for key, v := range labels {
			sk := sanitizeMetaKey(key)
			lblOut[prefix+"_label_"+sk] = v
			lblOut[prefix+"_labelpresent_"+sk] = "true"
		}
	}
	if cfg.IncludeAnnotations && len(annotations) > 0 {
		annOut = make(map[string]string, len(annotations)*2)
		for key, v := range annotations {
			if strings.HasPrefix(key, cfg.AnnotationPrefix+"/") {
				continue
			}
			sk := sanitizeMetaKey(key)
			annOut[prefix+"_annotation_"+sk] = v
			annOut[prefix+"_annotationpresent_"+sk] = "true"
		}
	}
	return lblOut, annOut
}

func attachNodeLabels(t *Target, nodeName string, nodeStore cache.Store, attach bool) {
	if !attach || nodeStore == nil || nodeName == "" {
		return
	}
	obj, exists, err := nodeStore.GetByKey(nodeName)
	if err != nil || !exists {
		return
	}
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	if t.Labels == nil {
		t.Labels = make(map[string]string, len(node.Labels)*2)
	}
	for key, v := range node.Labels {
		sk := sanitizeMetaKey(key)
		t.Labels["node_label_"+sk] = v
		t.Labels["node_labelpresent_"+sk] = "true"
	}
}

func attachNamespaceLabels(t *Target, namespace string, nsStore cache.Store, attach bool) {
	if !attach || nsStore == nil || namespace == "" {
		return
	}
	obj, exists, err := nsStore.GetByKey(namespace)
	if err != nil || !exists {
		return
	}
	ns, ok := obj.(*corev1.Namespace)
	if !ok {
		return
	}
	if t.Labels == nil {
		t.Labels = make(map[string]string, len(ns.Labels)*2)
	}
	for key, v := range ns.Labels {
		sk := sanitizeMetaKey(key)
		t.Labels["namespace_label_"+sk] = v
		t.Labels["namespace_labelpresent_"+sk] = "true"
	}
}

func controllerOf(pod *corev1.Pod) (kind, name string) {
	refs := pod.GetOwnerReferences()
	for i := range refs {
		if refs[i].Controller != nil && *refs[i].Controller {
			return refs[i].Kind, refs[i].Name
		}
	}
	if len(refs) > 0 {
		return refs[0].Kind, refs[0].Name
	}
	return "", ""
}

func containerStatusFor(statuses []corev1.ContainerStatus, name string) (image, id string) {
	for i := range statuses {
		if statuses[i].Name == name {
			return statuses[i].Image, statuses[i].ContainerID
		}
	}
	return "", ""
}

type podPort struct {
	container string
	name      string
	number    int32
	protocol  string
	isInit    bool
}

func targetsFromPod(pod *corev1.Pod, cfg config.Config, k keys) []Target {
	if !scrapeEnabled(pod.Annotations, k) {
		return nil
	}
	if pod.Status.PodIP == "" {
		return nil
	}

	scheme := defaultStr(pod.Annotations[k.scheme], "http")
	path := defaultStr(pod.Annotations[k.path], "/metrics")
	scrapePortStr := pod.Annotations[k.port]

	ckind, cname := controllerOf(pod)

	var ports []podPort
	if scrapePortStr != "" {
		if p, err := strconv.ParseInt(scrapePortStr, 10, 32); err == nil && p > 0 {
			ports = append(ports, findContainerPort(pod, int32(p)))
		}
	}
	if len(ports) == 0 {
		for i := range pod.Spec.Containers {
			c := &pod.Spec.Containers[i]
			for j := range c.Ports {
				cp := &c.Ports[j]
				ports = append(ports, podPort{
					container: c.Name,
					name:      cp.Name,
					number:    cp.ContainerPort,
					protocol:  string(cp.Protocol),
				})
			}
			if len(ports) > 0 {
				break
			}
		}
	}
	if len(ports) == 0 {
		for i := range pod.Spec.InitContainers {
			c := &pod.Spec.InitContainers[i]
			for j := range c.Ports {
				cp := &c.Ports[j]
				ports = append(ports, podPort{
					container: c.Name,
					name:      cp.Name,
					number:    cp.ContainerPort,
					protocol:  string(cp.Protocol),
					isInit:    true,
				})
			}
		}
	}
	if len(ports) == 0 {
		return nil
	}

	base := Target{}
	base.Role = "pod"
	base.Namespace = pod.Namespace
	base.Pod = pod.Name
	base.Node = pod.Spec.NodeName
	base.PodUID = string(pod.UID)
	base.PodPhase = string(pod.Status.Phase)
	base.PodReady = podReady(pod)
	base.ControllerKind = ckind
	base.ControllerName = cname
	base.HostIP = pod.Status.HostIP
	base.PodIP = pod.Status.PodIP
	applyScrapeSettings(&base, pod.Annotations, k)
	base.Labels, base.Annotations = metadataMaps("pod", pod.Labels, pod.Annotations, cfg)

	targets := make([]Target, 0, len(ports))
	for _, pp := range ports {
		t := base
		portStr := strconv.Itoa(int(pp.number))
		hostPort := net.JoinHostPort(pod.Status.PodIP, portStr)
		t.Instance = hostPort
		var nb strings.Builder
		nb.Grow(len(pod.Namespace) + len(pod.Name) + 20)
		nb.WriteString("pod_")
		sanitizeWrite(&nb, pod.Namespace)
		nb.WriteByte('_')
		sanitizeWrite(&nb, pod.Name)
		nb.WriteByte('_')
		var pbuf [12]byte
		nb.Write(strconv.AppendInt(pbuf[:0], int64(pp.number), 10))
		t.Name = nb.String()
		t.URL = buildURL(scheme, hostPort, path, t.Params)
		t.Container = pp.container
		t.PortName = pp.name
		t.PortNumber = portStr
		t.PortProtocol = pp.protocol
		t.ContainerInit = boolStr(pp.isInit)
		cImage, cID := containerStatusFor(pod.Status.ContainerStatuses, pp.container)
		if cImage == "" {
			cImage, cID = containerStatusFor(pod.Status.InitContainerStatuses, pp.container)
		}
		t.ContainerImage = cImage
		t.ContainerID = cID
		targets = append(targets, t)
	}
	return targets
}

func podReady(pod *corev1.Pod) string {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return string(pod.Status.Conditions[i].Status)
		}
	}
	return ""
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func findContainerPort(pod *corev1.Pod, port int32) podPort {
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		for j := range c.Ports {
			if c.Ports[j].ContainerPort == port {
				return podPort{
					container: c.Name,
					name:      c.Ports[j].Name,
					number:    c.Ports[j].ContainerPort,
					protocol:  string(c.Ports[j].Protocol),
				}
			}
		}
	}
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		for j := range c.Ports {
			if c.Ports[j].ContainerPort == port {
				return podPort{
					container: c.Name,
					name:      c.Ports[j].Name,
					number:    c.Ports[j].ContainerPort,
					protocol:  string(c.Ports[j].Protocol),
					isInit:    true,
				}
			}
		}
	}
	return podPort{number: port}
}

func targetsFromEndpointSlice(epSlice *discoveryv1.EndpointSlice, svc *corev1.Service,
	cfg config.Config, k keys, podStore, nodeStore, nsStore cache.Store) []Target {
	if !scrapeEnabled(svc.Annotations, k) {
		return nil
	}

	scheme := defaultStr(svc.Annotations[k.scheme], "http")
	path := defaultStr(svc.Annotations[k.path], "/metrics")
	scrapePortStr := svc.Annotations[k.port]

	ports := resolveEndpointPorts(epSlice, scrapePortStr)
	if len(ports) == 0 {
		return nil
	}

	svcLbl, svcAnn := metadataMaps("service", svc.Labels, svc.Annotations, cfg)

	var targets []Target
	for _, port := range ports {
		portNum := int32(0)
		portName := ""
		portProto := ""
		if port.Port != nil {
			portNum = *port.Port
		}
		if port.Name != nil {
			portName = *port.Name
		}
		if port.Protocol != nil {
			portProto = string(*port.Protocol)
		}
		portStr := strconv.FormatInt(int64(portNum), 10)
		for _, ep := range epSlice.Endpoints {
			ready := ep.Conditions.Ready != nil && *ep.Conditions.Ready
			for _, addr := range ep.Addresses {
				hostPort := net.JoinHostPort(addr, portStr)
				t := Target{}
				t.Role = "endpointslice"
				t.Namespace = epSlice.Namespace
				t.Service = svc.Name
				t.Instance = hostPort
				t.URL = buildURL(scheme, hostPort, path, svc.Annotations[k.params])
				var nb strings.Builder
				nb.Grow(len(epSlice.Namespace) + len(svc.Name) + len(addr) + 20)
				nb.WriteString("ep_")
				sanitizeWrite(&nb, epSlice.Namespace)
				nb.WriteByte('_')
				sanitizeWrite(&nb, svc.Name)
				nb.WriteByte('_')
				sanitizeWrite(&nb, addr)
				nb.WriteByte('_')
				nb.WriteString(portStr)
				t.Name = nb.String()
				t.ServicePortName = portName
				t.PortName = portName
				t.PortNumber = portStr
				t.PortProtocol = portProto
				t.EndpointSliceName = epSlice.Name
				t.EndpointReady = boolStr(ready)
				t.EndpointAddressType = string(epSlice.AddressType)
				if ep.Hostname != nil {
					t.EndpointHostname = *ep.Hostname
				}
				if ep.NodeName != nil {
					t.EndpointNodeName = *ep.NodeName
					t.Node = *ep.NodeName
				}
				t.EndpointZone = endpointZone(ep)
				applyScrapeSettings(&t, svc.Annotations, k)
				t.Labels = cloneMap(svcLbl)
				t.Annotations = cloneMap(svcAnn)

				if ep.TargetRef != nil {
					t.Pod = ep.TargetRef.Name
					if ep.TargetRef.Kind == "Pod" && podStore != nil {
						if pod, ok := lookupPod(podStore, epSlice.Namespace, ep.TargetRef.Name); ok {
							t.Node = pod.Spec.NodeName
							t.PodUID = string(pod.UID)
							t.PodPhase = string(pod.Status.Phase)
							t.PodReady = podReady(pod)
							t.ControllerKind, t.ControllerName = controllerOf(pod)
							t.HostIP = pod.Status.HostIP
							t.PodIP = pod.Status.PodIP
							pLbl, _ := metadataMaps("pod", pod.Labels, pod.Annotations, cfg)
							mergeMaps(&t.Labels, pLbl)
							attachNodeLabels(&t, pod.Spec.NodeName, nodeStore, cfg.AttachNodeMetadata)
						}
					}
				}
				attachNamespaceLabels(&t, epSlice.Namespace, nsStore, cfg.AttachNsMetadata)
				targets = append(targets, t)
			}
		}
	}
	return targets
}

func endpointZone(ep discoveryv1.Endpoint) string {
	if ep.DeprecatedTopology != nil {
		if z, ok := ep.DeprecatedTopology["topology.kubernetes.io/zone"]; ok {
			return z
		}
	}
	if ep.Zone != nil {
		return *ep.Zone
	}
	return ""
}

func targetsFromEndpoints(eps *corev1.Endpoints, svc *corev1.Service, cfg config.Config, k keys, podStore, nodeStore, nsStore cache.Store) []Target { //nolint:staticcheck // legacy endpoints role for parity
	if svc == nil {
		return nil
	}
	if !scrapeEnabled(svc.Annotations, k) {
		return nil
	}

	scheme := defaultStr(svc.Annotations[k.scheme], "http")
	path := defaultStr(svc.Annotations[k.path], "/metrics")
	scrapePortStr := svc.Annotations[k.port]

	svcLbl, svcAnn := metadataMaps("endpoints", svc.Labels, svc.Annotations, cfg)

	var targets []Target
	for i := range eps.Subsets {
		subset := &eps.Subsets[i]
		for _, sp := range resolveSubsetPorts(subset.Ports, scrapePortStr) {
			portNum := sp.Port
			portStr := strconv.FormatInt(int64(portNum), 10)
			for _, addr := range subset.Addresses {
				hostPort := net.JoinHostPort(addr.IP, portStr)
				t := Target{}
				t.Role = "endpoints"
				t.Namespace = eps.Namespace
				t.Service = svc.Name
				t.Instance = hostPort
				t.URL = buildURL(scheme, hostPort, path, svc.Annotations[k.params])
				t.Name = sanitizeName("ep_" + eps.Namespace + "_" + svc.Name + "_" + sanitizeIP(addr.IP) + "_" + portStr)
				t.PortName = sp.Name
				t.PortNumber = portStr
				t.PortProtocol = string(sp.Protocol)
				t.EndpointReady = "true"
				if addr.Hostname != "" {
					t.EndpointHostname = addr.Hostname
				}
				if addr.NodeName != nil {
					t.EndpointNodeName = *addr.NodeName
					t.Node = *addr.NodeName
				}
				applyScrapeSettings(&t, svc.Annotations, k)
				t.Labels = cloneMap(svcLbl)
				t.Annotations = cloneMap(svcAnn)
				if addr.TargetRef != nil {
					t.Pod = addr.TargetRef.Name
					if addr.TargetRef.Kind == "Pod" && podStore != nil {
						if pod, ok := lookupPod(podStore, eps.Namespace, addr.TargetRef.Name); ok {
							t.Node = pod.Spec.NodeName
							t.PodUID = string(pod.UID)
							t.PodPhase = string(pod.Status.Phase)
							t.PodReady = podReady(pod)
							t.ControllerKind, t.ControllerName = controllerOf(pod)
							t.HostIP = pod.Status.HostIP
							t.PodIP = pod.Status.PodIP
							pLbl, _ := metadataMaps("pod", pod.Labels, pod.Annotations, cfg)
							mergeMaps(&t.Labels, pLbl)
							attachNodeLabels(&t, pod.Spec.NodeName, nodeStore, cfg.AttachNodeMetadata)
						}
					}
				}
				attachNamespaceLabels(&t, eps.Namespace, nsStore, cfg.AttachNsMetadata)
				targets = append(targets, t)
			}
		}
	}
	return targets
}

func targetsFromService(svc *corev1.Service, cfg config.Config, k keys, nsStore cache.Store) []Target {
	if !scrapeEnabled(svc.Annotations, k) {
		return nil
	}

	scheme := defaultStr(svc.Annotations[k.scheme], "http")
	path := defaultStr(svc.Annotations[k.path], "/metrics")
	scrapePortStr := svc.Annotations[k.port]

	host := svc.Spec.ClusterIP
	external := ""
	if svc.Spec.Type == corev1.ServiceTypeExternalName {
		external = svc.Spec.ExternalName
		host = external
	}
	if host == "" || host == "None" {
		host = svc.Name + "." + svc.Namespace + "." + cfg.ServiceDNSSuffix
	}

	type svcPort struct {
		number   int32
		name     string
		protocol string
	}
	var ports []svcPort
	if scrapePortStr != "" {
		if p, err := strconv.ParseInt(scrapePortStr, 10, 32); err == nil && p > 0 {
			sp := svcPort{number: int32(p)}
			for i := range svc.Spec.Ports {
				if svc.Spec.Ports[i].Port == int32(p) {
					sp.name = svc.Spec.Ports[i].Name
					sp.protocol = string(svc.Spec.Ports[i].Protocol)
					break
				}
			}
			ports = []svcPort{sp}
		}
	}
	if len(ports) == 0 {
		for i := range svc.Spec.Ports {
			sp := &svc.Spec.Ports[i]
			ports = append(ports, svcPort{
				number:   sp.Port,
				name:     sp.Name,
				protocol: string(sp.Protocol),
			})
		}
	}
	if len(ports) == 0 {
		return nil
	}

	svcLbl, svcAnn := metadataMaps("service", svc.Labels, svc.Annotations, cfg)

	targets := make([]Target, 0, len(ports))
	for _, sp := range ports {
		t := Target{}
		portStr := strconv.Itoa(int(sp.number))
		t.Role = "service"
		t.Namespace = svc.Namespace
		t.Service = svc.Name
		t.Instance = net.JoinHostPort(host, portStr)
		t.URL = buildURL(scheme, t.Instance, path, svc.Annotations[k.params])
		t.Name = sanitizeName("svc_" + svc.Namespace + "_" + svc.Name + "_" + portStr)
		t.ServiceType = string(svc.Spec.Type)
		t.ServiceClusterIP = svc.Spec.ClusterIP
		t.ServiceExternalName = external
		t.ServicePortName = sp.name
		t.PortName = sp.name
		t.PortNumber = portStr
		t.PortProtocol = sp.protocol
		applyScrapeSettings(&t, svc.Annotations, k)
		t.Labels = cloneMap(svcLbl)
		t.Annotations = cloneMap(svcAnn)
		attachNamespaceLabels(&t, svc.Namespace, nsStore, cfg.AttachNsMetadata)
		targets = append(targets, t)
	}
	return targets
}

func targetsFromNode(node *corev1.Node, cfg config.Config, k keys) []Target {
	if !scrapeEnabled(node.Annotations, k) {
		return nil
	}

	scheme := defaultStr(node.Annotations[k.scheme], "https")
	path := defaultStr(node.Annotations[k.path], "/metrics")

	port := cfg.NodeScrapePort
	if ps := node.Annotations[k.port]; ps != "" {
		if p, err := strconv.ParseInt(ps, 10, 32); err == nil && p > 0 {
			port = int32(p)
		}
	}

	host := nodeInternalIP(node)
	if host == "" {
		return nil
	}

	t := Target{}
	t.Role = "node"
	t.Node = node.Name
	t.NodeIP = host
	t.NodeProviderID = node.Spec.ProviderID
	t.NodeAddresses = nodeAddresses(node)
	portStr := strconv.Itoa(int(port))
	t.Instance = net.JoinHostPort(host, portStr)
	t.URL = buildURL(scheme, t.Instance, path, node.Annotations[k.params])
	t.Name = sanitizeName("node_" + node.Name + "_" + portStr)
	applyScrapeSettings(&t, node.Annotations, k)
	t.Labels, t.Annotations = metadataMaps("node", node.Labels, node.Annotations, cfg)
	return []Target{t}
}

func nodeAddresses(node *corev1.Node) map[string]string {
	if len(node.Status.Addresses) == 0 {
		return nil
	}
	out := make(map[string]string, len(node.Status.Addresses))
	for _, addr := range node.Status.Addresses {
		out[strings.ToLower(string(addr.Type))] = addr.Address
	}
	return out
}

func targetsFromIngress(ing *netv1.Ingress, cfg config.Config, k keys) []Target {
	if !scrapeEnabled(ing.Annotations, k) {
		return nil
	}

	defaultScheme := "http"
	if len(ing.Spec.TLS) > 0 {
		defaultScheme = "https"
	}
	scheme := defaultStr(ing.Annotations[k.scheme], defaultScheme)
	path := defaultStr(ing.Annotations[k.path], "/metrics")
	portStr := ing.Annotations[k.port]
	port := int32(80)
	if scheme == "https" {
		port = 443
	}
	if portStr != "" {
		if p, err := strconv.ParseInt(portStr, 10, 32); err == nil && p > 0 {
			port = int32(p)
		}
	}

	ingLbl, ingAnn := metadataMaps("ingress", ing.Labels, ing.Annotations, cfg)

	portNumStr := strconv.Itoa(int(port))
	var targets []Target
	seen := make(map[string]struct{})
	className := ""
	if ing.Spec.IngressClassName != nil {
		className = *ing.Spec.IngressClassName
	}
	addHost := func(host, rulePath string) {
		if host == "" {
			host = ing.Name
		}
		if rulePath == "" {
			rulePath = "/"
		}
		key := host + ":" + portNumStr
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		t := Target{}
		t.Role = "ingress"
		t.Namespace = ing.Namespace
		t.Instance = key
		t.URL = buildURL(scheme, key, path, ing.Annotations[k.params])
		t.Name = sanitizeName("ing_" + ing.Namespace + "_" + ing.Name + "_" + sanitizeIP(host))
		t.IngressClassName = className
		t.IngressPath = rulePath
		t.IngressScheme = scheme
		applyScrapeSettings(&t, ing.Annotations, k)
		t.Labels = cloneMap(ingLbl)
		t.Annotations = cloneMap(ingAnn)
		targets = append(targets, t)
	}

	for _, rule := range ing.Spec.Rules {
		rulePath := ""
		if rule.HTTP != nil && len(rule.HTTP.Paths) > 0 {
			rulePath = rule.HTTP.Paths[0].Path
		}
		addHost(rule.Host, rulePath)
	}
	if len(targets) == 0 {
		addHost("", "")
	}
	return targets
}

func resolveEndpointPorts(epSlice *discoveryv1.EndpointSlice, scrapePortStr string) []discoveryv1.EndpointPort {
	if scrapePortStr == "" {
		var out []discoveryv1.EndpointPort
		for i := range epSlice.Ports {
			if epSlice.Ports[i].Port != nil {
				out = append(out, epSlice.Ports[i])
			}
		}
		return out
	}
	p, err := strconv.ParseInt(scrapePortStr, 10, 32)
	if err != nil {
		return nil
	}
	for i := range epSlice.Ports {
		if epSlice.Ports[i].Port != nil && *epSlice.Ports[i].Port == int32(p) {
			return []discoveryv1.EndpointPort{epSlice.Ports[i]}
		}
	}
	tp := int32(p)
	return []discoveryv1.EndpointPort{{Port: &tp}}
}

func resolveSubsetPorts(ports []corev1.EndpointPort, scrapePortStr string) []corev1.EndpointPort {
	if scrapePortStr == "" {
		return ports
	}
	p, err := strconv.ParseInt(scrapePortStr, 10, 32)
	if err != nil {
		return nil
	}
	for i := range ports {
		if ports[i].Port == int32(p) {
			return []corev1.EndpointPort{ports[i]}
		}
	}
	return []corev1.EndpointPort{{Port: int32(p)}}
}

func lookupPod(store cache.Store, namespace, name string) (*corev1.Pod, bool) {
	obj, exists, err := store.GetByKey(namespace + "/" + name)
	if err != nil || !exists {
		return nil, false
	}
	pod, ok := obj.(*corev1.Pod)
	return pod, ok
}

func nodeInternalIP(node *corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeExternalIP {
			return addr.Address
		}
	}
	return ""
}

func mergeMaps(dst *map[string]string, src map[string]string) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]string, len(src))
	}
	maps.Copy((*dst), src)
}

func cloneMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	maps.Copy(out, src)
	return out
}

func buildURL(scheme, hostPort, path, params string) string {
	if params == "" {
		return scheme + "://" + hostPort + path
	}
	return scheme + "://" + hostPort + path + "?" + params
}

func durStr(v string, fallback time.Duration) time.Duration {
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

var sanitizeIPReplacer = strings.NewReplacer(".", "_", ":", "_")

func sanitizeIP(ip string) string {
	if !strings.ContainsAny(ip, "./:") {
		return ip
	}
	return sanitizeIPReplacer.Replace(ip)
}

var sanitizeNameReplacer = strings.NewReplacer(".", "_", "-", "_", "/", "_")

func sanitizeName(name string) string {
	if !strings.ContainsAny(name, ".-/") {
		return name
	}
	return sanitizeNameReplacer.Replace(name)
}

// sanitizeWrite appends s to b with '.', '-', '/' replaced by '_'.
func sanitizeWrite(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '.' || c == '-' || c == '/' {
			c = '_'
		}
		b.WriteByte(c)
	}
}

func sanitizeMetaKey(key string) string {
	// Fast path: already lowercase alphanumeric, nothing to transform.
	if isCleanMetaKey(key) {
		return key
	}
	var b strings.Builder
	b.Grow(len(key))
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteByte(c)
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c + 32)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func isCleanMetaKey(key string) bool {
	for i := 0; i < len(key); i++ {
		c := key[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

# API Reference

Internal package documentation for `vector-k8s-helper`.

## Packages

```
github.com/sonroyaalmerol/vector-k8s-helper
├── cmd/vector-k8s-helper   # Entrypoint
├── internal/config          # Environment configuration
├── internal/discovery       # Kubernetes informer-based target discovery
├── internal/renderer        # Vector YAML config generation
└── internal/writer          # ConfigMap persistence
```

---

## `internal/config`

```go
import "github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
```

### Types

#### `Config`

All runtime configuration. Populated by `Load` from environment variables.

```go
type Config struct {
    Namespace        string
    ConfigMapName    string
    ConfigMapKey     string
    ScrapeInterval   time.Duration
    ScrapeTimeout    time.Duration
    HonorLabels      bool
    ResyncInterval   time.Duration
    DebounceInterval time.Duration
    MetricsAddr      string
    ClusterLabel     string
    AdditionalLabels map[string]string
    AnnotationPrefix string

    Roles              Roles
    Selectors          Selectors
    Namespaces         NamespaceFilter
    AttachNodeMetadata bool
    AttachNsMetadata   bool
    IncludeAnnotations bool
    IncludeLabels      bool
    NodeScrapePort     int32
    ServiceDNSSuffix   string
}
```

#### `Roles`

Bitset of enabled discovery roles.

```go
type Roles struct {
    Pod            bool
    Endpoints      bool
    EndpointSlice  bool
    Node           bool
    Ingress        bool
    ServiceAddress bool
}
```

Methods: `Any() bool` (true if any role is on), `Slice() []string` (role names),
`String() string` (comma-joined).

#### `Selectors`

Label and field selectors per role, applied at informer construction.

```go
type Selectors struct {
    PodLabel, PodField                         string
    ServiceLabel, ServiceField                 string
    NodeLabel, NodeField                       string
    IngressLabel, IngressField                 string
    EndpointSliceLabel, EndpointSliceField     string
    EndpointsLabel, EndpointsField             string
}
```

#### `NamespaceFilter`

```go
type NamespaceFilter struct {
    Include []string
    Exclude []string
}

func (nf NamespaceFilter) Allowed(namespace string) bool
```

`Allowed` returns `false` if the namespace is in `Exclude`; otherwise `true` if
`Include` is empty or contains the namespace.

### Functions

#### `Load() (Config, error)`

Reads and validates all configuration from environment variables. Returns an
error if `CONFIGMAP_NAME` is empty, `SCRAPE_INTERVAL` is below 5s, or
`DEBOUNCE_INTERVAL` is negative.

#### `ParseBool(s string, fallback bool) bool`

Parses a boolean string with a fallback. Used for annotation value parsing.

### Constants

Annotation suffixes (combined with `AnnotationPrefix` at runtime):

`AnnotationScrape`, `AnnotationPort`, `AnnotationPath`, `AnnotationScheme`,
`AnnotationParams`, `AnnotationJob`, `AnnotationCollectionInterval`,
`AnnotationCollectionTimeout`, `AnnotationServiceAccountBearerToken`,
`AnnotationTLSServerName`, `AnnotationTLSInsecureSkipVerify`,
`AnnotationHTTPBasicAuthUsernameEnv`, `AnnotationHTTPBasicAuthPasswordEnv`,
`AnnotationHTTPBearerTokenEnv`, `AnnotationTLSCAFile`, `AnnotationTLSCertFile`,
`AnnotationTLSKeyFile`.

`ExclusionAnnotation` = `"vector.dev/exclude"`.

`defaultAnnotationPrefix` = `"prometheus.io"`.

---

## `internal/discovery`

```go
import "github.com/sonroyaalmerol/vector-k8s-helper/internal/config"
```

### types

#### `Target`

A single scrape target with its full metadata payload.

```go
type Target struct {
    Name, URL, Instance, Role string
    Namespace, Pod, Service, Node, Container, Job string

    PodUID, PodPhase, PodReady                 string
    ControllerKind, ControllerName             string
    ContainerImage, ContainerID, ContainerInit string
    HostIP, PodIP, NodeIP, ServicePortName     string

    PortName, PortNumber, PortProtocol         string

    ServiceType, ServiceClusterIP, ServiceExternalName string

    NodeProviderID    string
    NodeAddresses     map[string]string

    EndpointReady, EndpointHostname, EndpointNodeName string
    EndpointZone, EndpointAddressType, EndpointSliceName string

    IngressClassName, IngressPath, IngressScheme string

    ScrapeInterval, ScrapeTimeout time.Duration
    Params                        string

    BasicAuthUserEnv, BasicAuthPassword string
    BearerTokenEnv, BearerToken         string

    TLSServerName         string
    TLSInsecureSkipVerify bool
    TLSCAFile, TLSCertFile, TLSKeyFile string

    Labels, Annotations map[string]string
}
```

`Instance` is always `host:port` and is the key used by the renderer's VRL
lookup table.

#### `Watcher`

```go
type Watcher struct { /* unexported fields */ }
```

Watches Kubernetes resources for scrape annotations across the enabled roles
and emits full target snapshots.

##### `NewWatcher(client kubernetes.Interface, cfg config.Config, logger *slog.Logger) *Watcher`

Creates a Watcher. Does not start until `Run` is called.

##### `(w *Watcher) Output() <-chan []Target`

Returns a buffered (capacity 1) channel of target snapshots. The latest
snapshot replaces any pending one.

##### `(w *Watcher) Run(ctx context.Context) error`

Starts the per-role informers, waits for cache sync, and blocks until `ctx` is
cancelled. Returns an error if no roles are enabled or cache sync fails.

### Target extractors

Each role has an extractor that builds `[]Target` from a single object (plus,
for endpoint roles, the owning Service and supporting stores):

| Function | Role | Signature |
|---|---|---|
| `targetsFromPod` | pod | `(pod *corev1.Pod, cfg config.Config, k keys) []Target` |
| `targetsFromEndpointSlice` | endpointslice | `(epSlice *discoveryv1.EndpointSlice, svc *corev1.Service, cfg config.Config, k keys, podStore, nodeStore, nsStore cache.Store) []Target` |
| `targetsFromEndpoints` | endpoints | `(eps *corev1.Endpoints, svc *corev1.Service, cfg config.Config, k keys, podStore, nodeStore, nsStore cache.Store) []Target` |
| `targetsFromService` | service | `(svc *corev1.Service, cfg config.Config, k keys, nsStore cache.Store) []Target` |
| `targetsFromNode` | node | `(node *corev1.Node, cfg config.Config, k keys) []Target` |
| `targetsFromIngress` | ingress | `(ing *netv1.Ingress, cfg config.Config, k keys) []Target` |

Port resolution helpers:

- `resolveEndpointPorts(ports []discoveryv1.EndpointPort, scrapePortStr string) []discoveryv1.EndpointPort` — selects the subset port matching the service's `prometheus.io/port` annotation, or returns all ports when unset.
- `resolveSubsetPorts(ports []corev1.EndpointPort, scrapePortStr string) []corev1.EndpointPort` — same logic for legacy `Endpoints`.

---

## `internal/renderer`

```go
import "github.com/sonroyaalmerol/vector-k8s-helper/internal/renderer"
```

### Types

#### `Config`

```go
type Config struct {
    ScrapeIntervalSecs float64
    ScrapeTimeoutSecs  float64
    HonorLabels        bool
    ClusterLabel       string
    AdditionalLabels   map[string]string
}
```

#### `VectorSources`

The top-level shape of the generated YAML:

```go
type VectorSources struct {
    Sources    map[string]SourceConfig    `yaml:"sources"`
    Transforms map[string]TransformConfig `yaml:"transforms,omitempty"`
}
```

#### `SourceConfig`

```go
type SourceConfig struct {
    Type               string      `yaml:"type"`               // always "prometheus_scrape"
    Endpoints          []string    `yaml:"endpoints"`
    ScrapeIntervalSecs float64     `yaml:"scrape_interval_secs"`
    ScrapeTimeoutSecs  float64     `yaml:"scrape_timeout_secs"`
    HonorLabels        bool        `yaml:"honor_labels,omitempty"`
    Auth               *AuthConfig `yaml:"auth,omitempty"`
    TLS                *TLSConfig  `yaml:"tls,omitempty"`
}
```

#### `AuthConfig`

Flattened internally-tagged enum matching Vector's `auth` schema
(`{strategy: basic, user, password}` or `{strategy: bearer, token}`).

#### `TLSConfig`

```go
type TLSConfig struct {
    VerifyCertificate *bool  `yaml:"verify_certificate,omitempty"`
    VerifyHostname    *bool  `yaml:"verify_hostname,omitempty"`
    CAFile            string `yaml:"ca_file,omitempty"`
    CrtFile           string `yaml:"crt_file,omitempty"`
    KeyFile           string `yaml:"key_file,omitempty"`
    ServerName        string `yaml:"server_name,omitempty"`
}
```

#### `TransformConfig`

```go
type TransformConfig struct {
    Type   string   `yaml:"type"`   // "remap"
    Inputs []string `yaml:"inputs"`
    Source string   `yaml:"source"` // VRL
}
```

### Functions

#### `Render(targets []discovery.Target, cfg Config) ([]byte, error)`

Generates a Vector config fragment. Groups targets into `prometheus_scrape`
sources by scheme, path, interval, timeout, TLS, and auth; produces an
`enrich_metrics` remap transform with a VRL metadata lookup table keyed on
`instance`. Returns `RenderEmpty` when `targets` is empty.

#### `RenderEmpty(cfg Config) ([]byte, error)`

Produces a minimal valid config with one `no_targets` source and zero
endpoints.

---

## `internal/writer`

```go
import "github.com/sonroyaalmerol/vector-k8s-helper/internal/writer"
```

### types

#### `Writer`

```go
type Writer struct { /* unexported fields */ }
```

Persists rendered Vector config to a Kubernetes ConfigMap.

##### `NewWriter(client kubernetes.Interface, namespace, configMapKey string, logger *slog.Logger) *Writer`

Creates a Writer for the given namespace and ConfigMap data key.

##### `(w *Writer) Upsert(ctx context.Context, name string, content []byte) error`

Creates the ConfigMap if absent, otherwise applies a JSON merge patch. Retries
on conflict (`409`) and transient network errors with exponential backoff, up
to three attempts. Returns an error if `namespace` is empty or all attempts
fail.

---

## `cmd/vector-k8s-helper`

The entrypoint (`main` package). Wires the four stages together:

1. Loads config via `config.Load`.
2. Builds the in-cluster Kubernetes client.
3. Starts the health server (`/health`, `204`) on `METRICS_LISTEN_ADDR`.
4. Starts `Watcher.Run` in a goroutine.
5. In the main loop, reads snapshots, renders, dedupes bytewise against the
   previous render, and writes via `Writer.Upsert`.
6. Shuts down on `SIGINT`/`SIGTERM`.

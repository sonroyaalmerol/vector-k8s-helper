# API Reference

Internal package documentation for `vector-k8s-helper`.

## Packages

```
github.com/sonroyaalmerol/vector-k8s-helper
├── cmd/vector-k8s-helper   # Entrypoint
├── internal/config          # Configuration
├── internal/discovery       # K8s informer-based target discovery
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

```go
type Config struct {
    Namespace         string            // K8s namespace to watch ("": all)
    ConfigMapName     string            // Target ConfigMap name
    ConfigMapKey      string            // ConfigMap data key
    ScrapeInterval    time.Duration     // Prometheus scrape interval
    ResyncInterval    time.Duration     // Informer resync interval
    MetricsAddr       string            // Health server listen address
    TargetAnnotation  string            // Annotation key for enabling scrape
    ExcludeAnnotation string            // Annotation key for excluding pods
    ClusterLabel      string            // Static cluster label
    AdditionalLabels  map[string]string // Extra labels to add
}
```

### Functions

#### `Load() (Config, error)`

Reads all configuration from environment variables. Returns an error if
required values are invalid.

#### `ParseBool(s string, fallback bool) bool`

Parses a boolean string, returning `fallback` on failure. Used for
annotation value parsing.

### Constants

| Constant | Value |
|---|---|
| `AnnotationScrape` | `"prometheus.io/scrape"` |
| `AnnotationPort` | `"prometheus.io/port"` |
| `AnnotationPath` | `"prometheus.io/path"` |
| `AnnotationScheme` | `"prometheus.io/scheme"` |

---

## `internal/discovery`

```go
import "github.com/sonroyaalmerol/vector-k8s-helper/internal/discovery"
```

### Types

#### `Target`

```go
type Target struct {
    Name      string // Sanitized component name (e.g., pod_kube_system_coredns_app)
    URL       string // Fully-qualified: scheme://host:port/path
    Namespace string // K8s namespace
    Pod       string // Pod name (empty for service targets)
    Service   string // Service name (empty for pod targets)
    Node      string // Node name (pod targets only)
    Container string // Container name (pod targets only)
    Instance  string // host:port, used as VRL lookup key
}
```

#### `Watcher`

```go
type Watcher struct { /* unexported fields */ }
```

Watches K8s Pods and Services for scrape annotations. Emits the full
target list on every change via a buffered channel.

##### `NewWatcher(client kubernetes.Interface, cfg config.Config, logger *slog.Logger) *Watcher`

Creates a new Watcher. Does not start watching until `Run()` is called.

##### `(w *Watcher) Output() <-chan []Target`

Returns a channel that receives snapshot target lists. The channel has
buffer size 1; stale snapshots are replaced.

##### `(w *Watcher) Run(ctx context.Context) error`

Starts informers and blocks until `ctx` is cancelled. Returns
`ctx.Err()` on normal shutdown.

### Helper Functions

#### `targetsFromPod(pod *corev1.Pod, cfg config.Config) []Target`

Extracts scrape targets from a single Pod. Returns one target per
container when `prometheus.io/scrape` is `"true"`. Skips pods with
`vector.dev/exclude: "true"`, pods without an IP, or pods where no
port can be determined.

#### `targetsFromService(svc *corev1.Service, cfg config.Config) []Target`

Extracts a scrape target from a single Service. Skips headless services
(`ClusterIP: None`) and services without the scrape annotation.

#### `sanitizeName(name string) string`

Replaces `.`, `-`, `/` with `_` to produce valid Vector component names.

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
    ClusterLabel       string
    AdditionalLabels   map[string]string
}
```

### Functions

#### `Render(targets []discovery.Target, cfg Config) ([]byte, error)`

Generates a Vector config fragment (YAML) from the discovered targets.
The output contains:

- `sources` — one `prometheus_scrape` source per (scheme, path) group
- `transforms` — one `remap` transform (`enrich_metrics`) with a VRL
  lookup table mapping `instance` labels to k8s metadata

When no targets are provided, calls `RenderEmpty()`.

#### `RenderEmpty(cfg Config) ([]byte, error)`

Produces a minimal valid config with a single `no_targets` source that
has zero endpoints. Ensures Vector always has a parseable config even
before any scrape targets are discovered.

---

## `internal/writer`

```go
import "github.com/sonroyaalmerol/vector-k8s-helper/internal/writer"
```

### Types

#### `Writer`

```go
type Writer struct { /* unexported fields */ }
```

Persists generated Vector config to a Kubernetes ConfigMap.

##### `NewWriter(client kubernetes.Interface, namespace, configMapKey string, logger *slog.Logger) *Writer`

Creates a Writer targeting the given namespace and ConfigMap data key.

##### `(w *Writer) Upsert(ctx context.Context, name string, content []byte) error`

Creates the ConfigMap if it doesn't exist, otherwise patches it using
strategic merge patch. Returns an error if the namespace is empty or the
API call fails.
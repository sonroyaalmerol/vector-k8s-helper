# vector-k8s-helper

A lightweight Kubernetes controller that discovers Pods and Services annotated
with `prometheus.io/scrape` and dynamically generates Vector
`prometheus_scrape` source configuration.

It bridges the gap between Kubernetes service discovery (which Vector lacks
natively) and Vector's static `prometheus_scrape` source by watching the
K8s API in real time and writing generated config to a ConfigMap that Vector
hot-reloads.

## Features

- **Real-time discovery** — watches Pods and Services via K8s informers,
  updates config within seconds of changes
- **Full annotation support** — `prometheus.io/scrape`, `port`, `path`,
  `scheme`
- **Pod exclusion** — pods labeled `vector.dev/exclude: "true"` are skipped
- **Kubernetes label enrichment** — generates a VRL remap transform that
  adds `namespace`, `pod`, `node`, `service`, and `container` labels to
  scraped metrics
- **Cluster label** — optional static `cluster` tag for multi-cluster
  environments
- **Zero-downtime updates** — Vector picks up ConfigMap changes via
  `--watch-config`; no pod restarts required
- **Single binary, minimal footprint** — < 64 MiB memory, no dependencies
  beyond the K8s API

## How It Works

```
 Kubernetes API            vector-k8s-helper               ConfigMap               Vector Agent
┌──────────────┐      ┌─────────────────────┐      ┌──────────────────┐      ┌───────────────┐
│  Pods         │─────►│  Informers watch     │      │  scrape_sources  │◄────│  --config-dir  │
│  Services    │      │  annotation changes │─────►│  .yaml           │      │  /etc/vector/  │
│              │      │  → render YAML      │      │  (auto-updated)  │      │               │
└──────────────┘      └─────────────────────┘      └──────────────────┘      └───────────────┘
```

1. Informers watch all Pods and Services (or a single namespace) for
   `prometheus.io/scrape: "true"`
2. On any change, the full target set is reconciled and rendered into a
   Vector config fragment containing:
   - `prometheus_scrape` sources grouped by scheme and path
   - A `remap` transform (`enrich_metrics`) with a VRL lookup table that
     enriches each metric with Kubernetes metadata
3. The fragment is written to the `vector-scrape-config` ConfigMap via
   strategic merge patch
4. Vector's `--config-dir /etc/vector/` picks up the mounted ConfigMap
   file and reloads

## Supported Annotations

| Annotation | Default | Description |
|---|---|---|
| `prometheus.io/scrape` | `false` | Enable scraping for this pod/service |
| `prometheus.io/port` | first container/svc port | Port number to scrape |
| `prometheus.io/path` | `/metrics` | HTTP path to scrape |
| `prometheus.io/scheme` | `http` | URL scheme (`http` or `https`) |
| `vector.dev/exclude` | — | Set to `"true"` on a pod to exclude it |

## Quick Start

### Deploy the Helper

```bash
kubectl apply -f deploy/
```

This creates:
- A `ServiceAccount` and `ClusterRole` with pod/service read and configmap
  read/write permissions
- A `Deployment` running `vector-k8s-helper` in `kube-system`
- An initial `ConfigMap` (`vector-scrape-config`) with an empty scrape config

### Configure Vector

Add the following to your Vector Helm values to mount the generated config
and route metrics:

```yaml
extraVolumes:
  - name: scrape-config
    configMap:
      name: vector-scrape-config

extraVolumeMounts:
  - name: scrape-config
    mountPath: /etc/vector/scrape_sources.yaml
    subPath: scrape_sources.yaml
    readOnly: true

customConfig:
  data_dir: /vector-data-dir

  sources:
    kubernetes_logs:
      type: "kubernetes_logs"
    internal_metrics:
      type: "internal_metrics"

  sinks:
    vector_logs:
      type: "vector"
      inputs: ["kubernetes_logs"]
      address: "nabu.sgl.com:6000"

    vector_metrics:
      type: "vector"
      inputs:
        - "internal_metrics"
        - "enrich_metrics"    # ← from the generated config
      address: "nabu.sgl.com:6000"
```

Vector's default `--config-dir /etc/vector/` will discover and merge
`scrape_sources.yaml` automatically.

### Annotate Your Workloads

```yaml
apiVersion: v1
kind: Service
metadata:
  name: myapp
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "9090"
    prometheus.io/path: "/metrics"
spec:
  ports:
    - port: 9090
```

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: myapp
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
spec:
  containers:
    - name: app
      image: myapp:latest
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `NAMESPACE` | `""` (all) | K8s namespace to watch; empty = all namespaces |
| `CONFIGMAP_NAME` | `vector-scrape-config` | Target ConfigMap name |
| `CONFIGMAP_KEY` | `scrape_sources.yaml` | Data key inside the ConfigMap |
| `SCRAPE_INTERVAL` | `30s` | Prometheus scrape interval (minimum 5s) |
| `RESYNC_INTERVAL` | `5m` | Informer full resync interval |
| `CLUSTER_LABEL` | `""` | Static cluster label added to all metrics |
| `ADDITIONAL_LABELS` | `""` | Extra labels as `key1=val1,key2=val2` |
| `METRICS_LISTEN_ADDR` | `:9090` | Health/metrics listen address |

### Cluster Labels

For multi-cluster setups, set `CLUSTER_LABEL` per environment:

```yaml
# Production
env:
  - name: CLUSTER_LABEL
    value: "dmz-prod-1"

# Staging
env:
  - name: CLUSTER_LABEL
    value: "lan-dev-1"
```

## Generated Config Example

When two pods and one service are discovered, the helper generates a
ConfigMap entry like:

```yaml
sources:
  k8s_metrics_http_metrics:
    type: prometheus_scrape
    endpoints:
      - http://10.0.0.5:9090/metrics
      - http://10.0.0.10:8080/metrics
      - http://10.96.0.50:9090/metrics
    scrape_interval_secs: 30

transforms:
  enrich_metrics:
    type: remap
    inputs:
      - k8s_metrics_http_metrics
    source: |
      # Auto-generated Kubernetes metadata enrichment.
      metadata = {
        "10.0.0.5:9090": {"namespace": "production", "pod": "myapp-abc123", "node": "node-1", "container": "app"},
        "10.96.0.50:9090": {"namespace": "production", "service": "myapp"},
      }

      inst = .tags.instance
      if inst != null && metadata[inst] != null {
        .tags.namespace = metadata[inst].namespace
        .tags.pod = metadata[inst].pod
        .tags.node = metadata[inst].node
        .tags.service = metadata[inst].service
        .tags.container = metadata[inst].container
      }
      .tags.cluster = "dmz-prod-1"
```

## Development

```bash
# Build
go build ./cmd/vector-k8s-helper

# Test
go test ./... -race

# Lint
golangci-lint run ./...

# Docker
docker build -t vector-k8s-helper .
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for the component diagram
and data flow details.

## API Reference

See [docs/api.md](docs/api.md) for the full internal package documentation.

## Deployment Reference

See [docs/deployment.md](docs/deployment.md) for RBAC details,
resource requirements, and multi-environment patterns.

## License

MIT
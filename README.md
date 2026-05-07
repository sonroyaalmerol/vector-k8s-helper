# vector-k8s-helper

A lightweight Kubernetes controller that watches Pods and Services annotated
with `prometheus.io/scrape: "true"` and generates Vector `prometheus_scrape`
source configuration dynamically.

## How It Works

1. Watches the Kubernetes API for Pods and Services with
   `prometheus.io/scrape` annotations
2. Builds a target list from discovered endpoints
3. Generates a Vector config fragment containing:
   - `prometheus_scrape` sources with all discovered endpoints
   - A `remap` transform that enriches metrics with Kubernetes labels
     (namespace, pod, node, service, container) using an instance-based
     lookup table
4. Writes the config fragment to a ConfigMap (`vector-scrape-config`)
5. Vector picks up the updated ConfigMap and reloads automatically

## Supported Annotations

| Annotation | Description |
|---|---|
| `prometheus.io/scrape: "true"` | Enable scraping for this pod/service |
| `prometheus.io/port: "9090"` | Port to scrape (defaults to first container port) |
| `prometheus.io/path: "/metrics"` | Metrics path (defaults to `/metrics`) |
| `prometheus.io/scheme: "https"` | URL scheme (defaults to `http`) |
| `vector.dev/exclude: "true"` | Exclude this pod from scraping |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NAMESPACE` | `""` (all namespaces) | Kubernetes namespace to watch |
| `CONFIGMAP_NAME` | `vector-scrape-config` | Target ConfigMap name |
| `CONFIGMAP_KEY` | `scrape_sources.yaml` | Key in the ConfigMap |
| `SCRAPE_INTERVAL` | `30s` | Prometheus scrape interval |
| `RESYNC_INTERVAL` | `5m` | Informer resync interval |
| `CLUSTER_LABEL` | `""` | Static cluster label added to all metrics |
| `ADDITIONAL_LABELS` | `""` | Extra labels as `key1=val1,key2=val2` |
| `METRICS_LISTEN_ADDR` | `:9090` | Metrics server address |

## Vector Integration

Mount the generated ConfigMap as an additional config file in your Vector
deployment. Update the Vector Helm values to include:

```yaml
customConfig:
  data_dir: /vector-data-dir
  # ... your main config ...

# Mount the generated config as an additional file
extraVolumes:
  - name: scrape-config
    configMap:
      name: vector-scrape-config
extraVolumeMounts:
  - name: scrape-config
    mountPath: /etc/vector/scrape_sources.yaml
    subPath: scrape_sources.yaml
    readOnly: true
```

Vector's `--config-dir /etc/vector/` will automatically discover and merge this
file with the main configuration.

The remap transform output (`enrich_metrics`) should be connected to your
metrics sink in the main Vector config:

```yaml
sinks:
  vector_metrics:
    type: vector
    inputs:
      - internal_metrics
      - enrich_metrics
    address: "nabu.sgl.com:6000"
```

## Development

```bash
go build ./cmd/vector-k8s-helper
go test ./...
```

## Docker Build

```bash
docker build -t vector-k8s-helper .
```

## Deployment

```bash
kubectl apply -f deploy/
```

The helper runs as a single-replica Deployment in `kube-system` with a
ClusterRole that grants read access to Pods/Services and read/write access
to the target ConfigMap.
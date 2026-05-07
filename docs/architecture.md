# Architecture

## Component Diagram

```
┌───────────────────────────────────────────────────────────────┐
│                     vector-k8s-helper Pod                      │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  discovery    │  │  renderer    │  │  writer      │         │
│  │              │  │              │  │              │         │
│  │  Pod informer│  │  Target[] →  │  │  YAML →      │         │
│  │  Svc informer├─►│  Vector YAML ├─►│  ConfigMap   │         │
│  │              │  │  (sources +  │  │  (upsert)    │         │
│  │  Store cache │  │   transform) │  │              │         │
│  └──────┬───────┘  └──────────────┘  └──────┬───────┘         │
│         │                                   │                 │
│    watch K8s API                    write ConfigMap           │
└─────────┼───────────────────────────────────┼─────────────────┘
          │                                   │
          ▼                                   ▼
┌──────────────────┐              ┌────────────────────────────┐
│  Kubernetes API   │              │  ConfigMap                  │
│  Server           │              │  vector-scrape-config       │
│                   │              │  ┌───────────────────────┐ │
│  pods             │              │  │ scrape_sources.yaml   │ │
│  services         │              │  │  sources:             │ │
│                   │              │  │  transforms:          │ │
└──────────────────┘              │  └───────────────────────┘ │
                                  └────────────┬───────────────┘
                                               │ mount
                                               ▼
                                  ┌────────────────────────────┐
                                  │  Vector Agent DaemonSet     │
                                  │  /etc/vector/               │
                                  │  ├── vector.yaml (main)     │
                                  │  └── scrape_sources.yaml    │
                                  │         (dynamic, mounted)  │
                                  └────────────────────────────┘
```

## Data Flow

1. **Watch** — `discovery.Watcher` starts K8s SharedInformerFactory with
   Pod and Service informers. Event handlers trigger `reconcile()` on every
   add/update/delete.

2. **Reconcile** — Reads all objects from the informer store, extracts
   targets from annotated pods/services, deduplicates by name, and emits
   the full target snapshot on the output channel.

3. **Render** — `renderer.Render()` groups targets by `(scheme, path)` to
   minimize the number of `prometheus_scrape` sources. For each group it
   creates a source with the full endpoint URLs. It also generates a
   `remap` transform (`enrich_metrics`) that uses a VRL lookup table
   keyed by `instance` (host:port) to enrich metrics with k8s labels.

4. **Write** — `writer.Upsert()` creates or patches the ConfigMap using
   strategic merge patch. A content hash comparison prevents unnecessary
   writes when the config hasn't changed.

5. **Reload** — Vector's `--config-dir /etc/vector/` watches for file
   changes. When kubelet syncs the updated ConfigMap volume, Vector
   detects the change and hot-reloads the config.

## Target Resolution

### Pod Targets

For each container in an annotated pod:

```
pod.annotations["prometheus.io/scrape"] == "true"
  AND pod.status.podIP != ""
  AND NOT pod.annotations["vector.dev/exclude"] == "true"
```

The scrape URL is built as:

```
scheme://<podIP>:<port><path>
```

Where:
- `scheme` = `prometheus.io/scheme` annotation (default: `http`)
- `port` = `prometheus.io/port` annotation, or first `containerPort`
- `path` = `prometheus.io/path` annotation (default: `/metrics`)

Each container in a multi-container pod produces a separate target.

### Service Targets

For annotated services with a ClusterIP:

```
service.annotations["prometheus.io/scrape"] == "true"
  AND service.spec.clusterIP not in ("", "None")
```

The scrape URL is built as:

```
scheme://<clusterIP>:<port><path>
```

Where:
- `port` = `prometheus.io/port` annotation, or first service port
- `path` = `prometheus.io/path` annotation (default: `/metrics`)

### Name Sanitization

Target names are derived from the K8s resource identity and sanitized
for Vector component name compatibility (dots, dashes, slashes →
underscores):

- Pod: `pod_<namespace>_<name>_<container>` → `pod_kube_system_coredns_app`
- Service: `svc_<namespace>_<name>` → `svc_production_myapp`

### Source Grouping

Targets sharing the same scheme and metrics path are grouped into a
single `prometheus_scrape` source to minimize config size:

```
k8s_metrics_http_metrics     ← all http + /metrics targets
k8s_metrics_https_metrics     ← all https + /metrics targets
k8s_metrics_http_custom_path  ← http + /custom/path targets
```

## Label Enrichment

The generated VRL remap transform uses an instance-based lookup table:

```vrl
metadata = {
  "10.0.0.5:9090": {"namespace": "production", "pod": "myapp-abc", "node": "node-1", "container": "app"},
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

The `instance` label is set automatically by Vector's `prometheus_scrape`
source from the endpoint URL's `host:port`, providing the lookup key.

## Graceful Degradation

- **No targets discovered** → an empty `no_targets` source with zero
  endpoints is generated so the config remains valid
- **ConfigMap doesn't exist** → created on first write
- **ConfigMap already exists** → updated via strategic merge patch
- **Content unchanged** → write is skipped (dedup via string comparison)
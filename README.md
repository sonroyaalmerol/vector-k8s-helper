# vector-k8s-helper

A sidecar that discovers Kubernetes Prometheus scrape targets and writes a
Vector `prometheus_scrape` config to a shared volume — a drop-in replacement
for Grafana Alloy's `discovery.kubernetes` when your metrics pipeline is
Vector.

Runs alongside Vector in the same pod. Each instance watches only its own
node's pods via a field selector (`spec.nodeName=$VECTOR_SELF_NODE_NAME`),
so every DaemonSet pod scrapes only local targets. No ConfigMaps, no
cross-node traffic, no init containers.

## What it does

- Discovers scrape targets across **six roles**: `pod`, `endpointslice`,
  `endpoints`, `service`, `node`, and `ingress`.
- Reads `prometheus.io/*` annotations for scrape enablement, port, path,
  scheme, TLS, and auth.
- Emits the full `__meta_kubernetes_*` metadata surface as Vector tags via
  a generated VRL lookup table keyed on `instance`.
- Groups targets into minimal `prometheus_scrape` sources.
- Writes the rendered config to a file on a shared `emptyDir`. Vector reads
  it and scrapes immediately.

## Quick start

```bash
kubectl apply -f deploy/
```

This creates a DaemonSet where each pod has a helper sidecar and a Vector
container. The helper discovers only its own node's targets and writes to
`/etc/vector/discovered/scrape_sources.yaml` on a shared volume. Vector
loads the config and scrapes.

Annotate a workload to expose metrics:

```bash
kubectl annotate pod myapp prometheus.io/scrape=true prometheus.io/port=9090
```

## How it works

```
DaemonSet pod
┌──────────────────────────────────────┐
│  helper (sidecar)     vector (main)  │
│       │                     │        │
│  watches only          reads config  │
│  local-node pods       scrapes only  │
│  via field selector    local targets │
│       │                     │        │
│  writes to ── shared ── reads from   │
│           emptyDir volume            │
└──────────────────────────────────────┘
```

1. **Watch** — per-role ListWatch informers stream pod/service/endpoint
   events from the API server. The pod informer is filtered by
   `spec.nodeName` so only local pods enter the store.
2. **Debounce** — events coalesce into a single reconcile.
3. **Reconcile** — resolves targets from each enabled role, deduplicates by
   name.
4. **Render** — builds `prometheus_scrape` sources and a VRL `remap`
   transform (`enrich_metrics`) that attaches Kubernetes metadata as tags.
5. **Write** — writes the YAML to a shared volume. Vector hot-reloads via
   `--watch-config`.

See [docs/architecture.md](docs/architecture.md).

## Configuration

All via environment variables.

### Core

| Variable | Default | Description |
|---|---|---|
| `VECTOR_SELF_NODE_NAME` | — | Node name (typically from downward API). Filters the pod informer. |
| `OUTPUT_PATH` | `/etc/vector/discovered/scrape_sources.yaml` | File written to the shared volume. |
| `SCRAPE_INTERVAL` | `30s` | Scrape interval for every source. |
| `SCRAPE_TIMEOUT` | `10s` | Scrape timeout. |
| `HONOR_LABELS` | `false` | Passed through to `prometheus_scrape.honor_labels`. |
| `RESYNC_INTERVAL` | `5m` | Informer full-resync. |
| `DEBOUNCE_INTERVAL` | `250ms` | Coalescing window for reconcile triggers. |
| `METRICS_LISTEN_ADDR` | `:9090` | Health server listen address. |
| `CLUSTER_LABEL` | `""` | Static cluster tag (e.g. `dmz-prod-1`). |
| `ADDITIONAL_LABELS` | `""` | Extra static tags (`k1=v1,k2=v2`). |
| `ANNOTATION_PREFIX` | `prometheus.io` | Annotation prefix. |

### Discovery

| Variable | Default | Description |
|---|---|---|
| `ROLES` | `pod,endpointslice` | Roles to enable: `pod`, `endpointslice`, `endpoints`, `service`, `node`, `ingress`. |
| `NAMESPACE_INCLUDE` | `""` (all) | Namespace allowlist. |
| `NAMESPACE_EXCLUDE` | `""` | Namespace denylist. |
| `INCLUDE_LABELS` | `true` | Attach object labels as `label_*` tags. |
| `INCLUDE_ANNOTATIONS` | `false` | Attach object annotations as `annotation_*` tags. |
| `ATTACH_NODE_METADATA` | `false` | Attach node name/IP to pod targets. |
| `ATTACH_NAMESPACE_METADATA` | `false` | Attach namespace labels. |
| `NODE_SCRAPE_PORT` | `10250` | Port for `node` role. |
| `SERVICE_DNS_SUFFIX` | `svc.cluster.local` | DNS suffix for `service` role. |

### Selectors

Each role supports label and field selectors, applied at the informer level:

| Variable | Role |
|---|---|
| `POD_LABEL_SELECTOR` / `POD_FIELD_SELECTOR` | `pod` |
| `SERVICE_LABEL_SELECTOR` / `SERVICE_FIELD_SELECTOR` | `service` |
| `ENDPOINTSLICE_LABEL_SELECTOR` / `ENDPOINTSLICE_FIELD_SELECTOR` | `endpointslice` |
| `ENDPOINTS_LABEL_SELECTOR` / `ENDPOINTS_FIELD_SELECTOR` | `endpoints` |
| `NODE_LABEL_SELECTOR` / `NODE_FIELD_SELECTOR` | `node` |
| `INGRESS_LABEL_SELECTOR` / `INGRESS_FIELD_SELECTOR` | `ingress` |

## Scrape annotations

The helper reads standard Prometheus annotations (prefix defaults to
`prometheus.io`):

| Annotation | Applies to | Description |
|---|---|---|
| `{prefix}/scrape` | pod, service | Enable scraping (`true`/`false`) |
| `{prefix}/port` | pod, service | Target port (numeric) |
| `{prefix}/path` | pod, service | Metrics path (default `/metrics`) |
| `{prefix}/scheme` | pod, service | `http` or `https` (default `http`) |
| `{prefix}/collectionInterval` | pod, service | Per-target scrape interval |
| `{prefix}/collectionTimeout` | pod, service | Per-target scrape timeout |
| `{prefix}/tlsServerName` | pod, service | TLS SNI |
| `{prefix}/tlsInsecureSkipVerify` | pod, service | Disable TLS verification |
| `{prefix}/tlsCAFile` | pod, service | CA bundle path |
| `{prefix}/tlsCertFile` | pod, service | Client cert path |
| `{prefix}/tlsKeyFile` | pod, service | Client key path |
| `{prefix}/httpBasicAuthUsernameEnvVar` | pod, service | Basic auth username env var |
| `{prefix}/httpBasicAuthPasswordEnvVar` | pod, service | Basic auth password env var |
| `{prefix}/httpBearerTokenEnvVar` | pod, service | Bearer token env var |
| `{prefix}/serviceAccountBearerToken` | service | Use SA token |
| `vector.dev/exclude` | all | Exclude from discovery |

## Metadata tags

Every target carries role-specific metadata attached as Vector tags:

| Tag | Roles | Source |
|---|---|---|
| `namespace` | all | object namespace |
| `pod` | pod, endpointslice, endpoints | pod name |
| `service` | endpointslice, endpoints, service | service name |
| `node` | pod, node | node name |
| `container` | pod | container name |
| `pod_uid`, `pod_phase`, `pod_ready` | pod | pod status |
| `controller_kind`, `controller_name` | pod | owner reference |
| `host_ip`, `pod_ip` | pod, node | IPs |
| `port_name`, `port_number`, `port_protocol` | pod, endpointslice, endpoints | resolved port |
| `service_type`, `service_cluster_ip` | endpointslice, endpoints, service | service spec |
| `endpoint_ready`, `endpoint_node_name`, `endpoint_zone` | endpointslice, endpoints | endpoint metadata |
| `ingress_class_name`, `ingress_path` | ingress | ingress spec |
| `label_*`, `labelpresent_*` | all (when `INCLUDE_LABELS`) | object labels |

## Deployment

The `deploy/` directory contains reference manifests:

| File | Contents |
|---|---|
| `deploy/deployment.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding, DaemonSet with sidecar helper + Vector. |
| `deploy/configmap.yaml` | Static Vector config (data dir, internal_metrics, remote_write sink). |
| `deploy/vector.yaml` | Vector log collector DaemonSet (kubernetes_logs source).

See [docs/deployment.md](docs/deployment.md).

## Development

```bash
go test -race ./...
go vet ./...
golangci-lint run
go build ./cmd/vector-k8s-helper
```

Requires Go 1.26+.

## License

MIT

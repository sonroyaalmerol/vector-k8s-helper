# vector-k8s-helper

A Kubernetes controller that discovers Prometheus scrape targets and renders
them into a [Vector](https://vector.dev) `prometheus_scrape` configuration —
a drop-in replacement for Grafana Alloy's `discovery.kubernetes` when your
metrics pipeline is Vector, not Prometheus.

It watches the Kubernetes API for annotated workloads, generates a Vector
config fragment (sources + an enrichment transform), and writes it to a
ConfigMap that Vector hot-reloads at runtime. No restarts, no sidecars, no
Prometheus required.

## What it does

- Discovers scrape targets across **six roles**: `pod`, `endpointslice`,
  `endpoints`, `service`, `node`, and `ingress` — the same role set as
  Prometheus `k8s_sd_configs` / Alloy `discovery.kubernetes`.
- Reads the standard `prometheus.io/*` annotations (configurable prefix) for
  scrape enablement, port, path, scheme, interval, TLS, and auth.
- Emits the full `__meta_kubernetes_*` metadata surface (pod phase, endpoint
  zone, service type, node addresses, controller info, label/annotation
  presence flags, etc.) as Vector tags via a generated VRL lookup table keyed
  on `instance`.
- Groups targets into the minimum number of `prometheus_scrape` sources
  (one per scheme + path + TLS + auth combination).
- Deduplicates writes with a content hash so the ConfigMap is only patched
  when something actually changes.

## Quick start

```bash
# build and push (or use a released image)
docker build -t vector-k8s-helper .

# deploy the helper + Vector consumer
kubectl apply -f deploy/

# annotate a workload to expose metrics
kubectl annotate pod myapp prometheus.io/scrape=true prometheus.io/port=9090
```

The helper writes its generated config to the `vector-scrape-config` ConfigMap.
Vector mounts that ConfigMap and reloads automatically.

## How it works

The helper runs a single reconciler loop driven by Kubernetes informers:

1. **Watch** — per-role ListWatch informers (with optional label/field
   selectors) stream add/update/delete events from the API server.
2. **Debounce** — events are coalesced by a configurable debounce window so a
   burst of pod churn produces one reconcile, not fifty.
3. **Reconcile** — on each tick the helper walks every informer store,
   resolves targets from each enabled role, and builds a deduplicated snapshot
   keyed by target name.
4. **Render** — the snapshot is grouped into `prometheus_scrape` sources and a
   VRL `remap` transform (`enrich_metrics`) that attaches Kubernetes metadata
   as tags.
5. **Write** — the rendered YAML is upserted into the target ConfigMap via a
   JSON merge patch, with conflict retries and content-hash dedup.

Vector reads the ConfigMap from a mounted volume. With `--watch-config`, it
hot-reloads whenever the kubelet syncs the updated volume (typically under a
minute).

See [docs/architecture.md](docs/architecture.md) for the full design.

## Configuration

All configuration is via environment variables.

### Core

| Variable | Default | Description |
|---|---|---|
| `NAMESPACE` | `""` (all namespaces) | Namespace to watch. Empty = cluster-wide. |
| `CONFIGMAP_NAME` | `vector-scrape-config` | ConfigMap the rendered config is written to. |
| `CONFIGMAP_KEY` | `scrape_sources.yaml` | Data key inside the ConfigMap. |
| `SCRAPE_INTERVAL` | `30s` | Default scrape interval applied to every source. |
| `SCRAPE_TIMEOUT` | `10s` | Default scrape timeout. Overridable per-target. |
| `HONOR_LABELS` | `false` | Passed through to `prometheus_scrape.honor_labels`. |
| `RESYNC_INTERVAL` | `5m` | Informer full-resync interval. |
| `DEBOUNCE_INTERVAL` | `250ms` | Coalescing window for reconcile triggers. |
| `METRICS_LISTEN_ADDR` | `:9090` | Health/readiness server listen address. |
| `CLUSTER_LABEL` | `""` | Static cluster tag attached to every metric (e.g. `dmz-prod-1`). |
| `ADDITIONAL_LABELS` | `""` | Extra static tags (`k1=v1,k2=v2`). |
| `ANNOTATION_PREFIX` | `prometheus.io` | Annotation prefix. Change for non-default namespaces. |

### Discovery

| Variable | Default | Description |
|---|---|---|
| `ROLES` | `pod,endpointslice` | Comma-separated roles to enable. One or more of: `pod`, `endpointslice`, `endpoints`, `service`, `node`, `ingress`. |
| `NAMESPACE_INCLUDE` | `""` (allow all) | Comma-separated namespace allowlist. |
| `NAMESPACE_EXCLUDE` | `""` | Comma-separated namespace denylist. Evaluated after the allowlist. |
| `INCLUDE_LABELS` | `true` | Attach discovered object labels as `label_*` tags. |
| `INCLUDE_ANNOTATIONS` | `false` | Attach discovered object annotations as `annotation_*` tags. |
| `ATTACH_NODE_METADATA` | `false` | Attach node name/IP to pod targets. |
| `ATTACH_NAMESPACE_METADATA` | `false` | Attach namespace labels as `namespace_label_*` tags. |
| `NODE_SCRAPE_PORT` | `10250` | Port used for `node` role targets. |
| `SERVICE_DNS_SUFFIX` | `svc.cluster.local` | DNS suffix for `service` role targets. |

### Selectors

Each role supports an optional label and field selector, applied at the
informer level so unmatched objects never enter the store:

| Variable | Applies to |
|---|---|
| `POD_LABEL_SELECTOR` / `POD_FIELD_SELECTOR` | `pod` |
| `SERVICE_LABEL_SELECTOR` / `SERVICE_FIELD_SELECTOR` | `service` |
| `ENDPOINTSLICE_LABEL_SELECTOR` / `ENDPOINTSLICE_FIELD_SELECTOR` | `endpointslice` |
| `ENDPOINTS_LABEL_SELECTOR` / `ENDPOINTS_FIELD_SELECTOR` | `endpoints` |
| `NODE_LABEL_SELECTOR` / `NODE_FIELD_SELECTOR` | `node` |
| `INGRESS_LABEL_SELECTOR` / `INGRESS_FIELD_SELECTOR` | `ingress` |

## Scrape annotations

The helper reads the standard Prometheus annotations under the configured
prefix (default `prometheus.io`):

| Annotation | Applies to | Description |
|---|---|---|
| `prometheus.io/scrape` | pod, service | Enables scraping (`true`/`false`). |
| `prometheus.io/port` | pod, service | Target port (numeric). Overrides declared port. |
| `prometheus.io/path` | pod, service | Metrics path (default `/metrics`). |
| `prometheus.io/scheme` | pod, service | `http` or `https` (default `http`). |
| `prometheus.io/params` | pod, service | Query string appended to the URL. |
| `prometheus.io/job` | pod, service | Overrides the `job` tag. |
| `prometheus.io/collectionInterval` | pod, service | Per-target scrape interval (Go duration). |
| `prometheus.io/collectionTimeout` | pod, service | Per-target scrape timeout (Go duration). |
| `prometheus.io/tlsServerName` | pod, service | TLS server name (SNI). |
| `prometheus.io/tlsInsecureSkipVerify` | pod, service | Disable TLS verification (`true`). |
| `prometheus.io/tlsCAFile` | pod, service | Path to a CA bundle. |
| `prometheus.io/tlsCertFile` | pod, service | Path to a client cert. |
| `prometheus.io/tlsKeyFile` | pod, service | Path to a client key. |
| `prometheus.io/httpBasicAuthUsernameEnvVar` | pod, service | Env var holding the basic-auth username. |
| `prometheus.io/httpBasicAuthPasswordEnvVar` | pod, service | Env var holding the basic-auth password. |
| `prometheus.io/httpBearerTokenEnvVar` | pod, service | Env var holding a bearer token. |
| `prometheus.io/serviceAccountBearerToken` | service | Use the service account token. |
| `vector.dev/exclude` | all | Exclude the object from discovery (`true`). |

## Metadata tags

Every discovered target carries role-specific metadata that the generated VRL
transform attaches as Vector tags. This mirrors the Prometheus
`__meta_kubernetes_*` label set.

| Tag | Roles | Source |
|---|---|---|
| `namespace` | all | object namespace |
| `pod` | pod, endpointslice, endpoints | pod name |
| `service` | endpointslice, endpoints, service, ingress | service name |
| `node` | pod, node | node name |
| `container` | pod | container name |
| `job` | all | job label (annotation or derived) |
| `pod_uid`, `pod_phase`, `pod_ready` | pod | pod status |
| `controller_kind`, `controller_name` | pod | owner reference |
| `container_image`, `container_id`, `container_init` | pod | container spec/status |
| `host_ip`, `pod_ip`, `node_ip` | pod, node | IPs |
| `port_name`, `port_number`, `port_protocol` | pod, endpointslice, endpoints | resolved port |
| `service_port_name`, `service_type`, `service_cluster_ip`, `service_external_name` | endpointslice, endpoints, service | service spec |
| `node_provider_id`, `node_address_*` | node | node status |
| `endpoint_ready`, `endpoint_hostname`, `endpoint_node_name`, `endpoint_zone`, `endpoint_address_type`, `endpointslice_name` | endpointslice, endpoints | endpoint |
| `ingress_class_name`, `ingress_path`, `ingress_scheme` | ingress | ingress spec |
| `label_*`, `labelpresent_*` | all (when `INCLUDE_LABELS`) | object labels |
| `annotation_*`, `annotationpresent_*` | all (when `INCLUDE_ANNOTATIONS`) | object annotations |

## Deployment

The `deploy/` directory contains ready-to-apply manifests:

| File | Contents |
|---|---|
| `deploy/deployment.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding, and the helper Deployment. |
| `deploy/configmap.yaml` | Initial empty ConfigMap (seeded with a valid no-targets config). |
| `deploy/vector.yaml` | A complete Vector consumer: ServiceAccount, RBAC, main/sinks ConfigMaps, and Deployment. |

See [docs/deployment.md](docs/deployment.md) for the full guide, including
RBAC, multi-cluster patterns, and troubleshooting.

## Development

```bash
go test -race ./...          # run tests with the race detector
go vet ./...                 # static analysis
golangci-lint run            # lint (config in .golangci.yml / v2)
go build ./cmd/vector-k8s-helper
```

Requires Go 1.26+ (see `go.mod`).

## License

MIT

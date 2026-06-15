# Deployment

The `deploy/` directory contains reference manifests for a complete Vector
metrics and logs pipeline.

| File | Contents |
|---|---|
| `deploy/deployment.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding, DaemonSet with sidecar helper + Vector. |
| `deploy/configmap.yaml` | Static Vector config (data dir, internal_metrics, remote_write sink). |
| `deploy/vector.yaml` | Vector log collector DaemonSet (kubernetes_logs source). |

## Metrics DaemonSet

```bash
kubectl apply -f deploy/deployment.yaml
kubectl apply -f deploy/configmap.yaml
```

This starts a DaemonSet in `kube-system` where each pod contains:

- **helper** — discovers only its own node's targets (pod informer filtered
  by `spec.nodeName`) and writes a `prometheus_scrape` config to
  `/etc/vector/discovered/scrape_sources.yaml` on a shared `emptyDir`.
- **vector** — loads the static config (main + sinks) and the discovered
  config from the shared volume. Scrapes local targets and remote-writes
  to the configured Mimir/VictoriaMetrics endpoint.

The ClusterRole grants list/watch access to pods, services, endpoints,
nodes, and namespaces. The ClusterRoleBinding attaches it to the
DaemonSet's service account.

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `VECTOR_SELF_NODE_NAME` | yes | Node name from downward API (`fieldRef: spec.nodeName`). |
| `ROLES` | no | Roles to enable. Default: `pod,endpointslice`. |
| `CLUSTER_LABEL` | no | Static cluster tag. |
| `SCRAPE_INTERVAL` | no | Scrape interval. Default: `30s`. |
| `SCRAPE_TIMEOUT` | no | Scrape timeout. Default: `10s`. |
| `NAMESPACE_EXCLUDE` | no | Denylist. `kube-system` excludes control plane metrics. |
| `INCLUDE_LABELS` | no | Attach object labels. Set to `false` if you hit cardinality limits. |
| `METRICS_LISTEN_ADDR` | no | Health server port. Default: `:9090`. |

## Log DaemonSet

```bash
kubectl apply -f deploy/vector.yaml
```

A separate DaemonSet that tails `/var/log/pods` on every node and ships
events to nabu via the native `vector` sink (not OTLP). This is separate
from the metrics DaemonSet because log collection must be node-local.

The ClusterRole grants list/watch access to pods, nodes, and namespaces
(required by the `kubernetes_logs` source for metadata enrichment).

## Multi-cluster

To deploy across multiple clusters, change the cluster-specific values:

- `CLUSTER_LABEL` / `VECTOR_CLUSTER` — the cluster name tag
- `prometheus_remote_write.endpoint` — the Mimir/VictoriaMetrics destination
- `vector` sink `address` — the nabu destination

All other manifests are identical between environments. The
`alloy-to-vector-migration` repository (outside this project) has
per-environment manifests for staging and prod.

## Troubleshooting

### Helper not discovering targets

Check that targets have the `prometheus.io/scrape: "true"` annotation and
a valid `prometheus.io/port`. Verify the helper sees the right node name:

```bash
kubectl logs -n kube-system -l app=vector-metrics -c helper | grep "node_name"
```

### Vector exits with code 78

This means a configuration error. Check the Vector container logs for the
specific error. Common causes:

- `enrich_metrics` not found — the helper's seed config wasn't written in
  time. Restart the pod.
- VRL syntax errors — the generated VRL may have issues. Check the helper
  version matches the Vector version.

### High cardinality in metrics backend

The helper attaches Kubernetes metadata as tags. If you exceed your
backend's label limits, set `INCLUDE_LABELS=false` to drop all object
labels. The core metadata tags (namespace, pod, node, service) are always
included.

### Connection refused to remote_write endpoint

Verify the pod can reach the endpoint from within the cluster. The
`internal_metrics` source produces operational metrics that will show
connection errors in `component_errors_total`.

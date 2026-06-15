# Deployment

## Manifests

The `deploy/` directory contains everything needed to run the helper and a
Vector consumer:

| File | Contents |
|---|---|
| `deploy/deployment.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding, and the helper Deployment. |
| `deploy/configmap.yaml` | Initial empty ConfigMap seeded with a valid no-targets config. |
| `deploy/vector.yaml` | Complete Vector consumer: ServiceAccount, RBAC, main/sinks ConfigMaps, and Deployment. |

Apply the helper first, then Vector:

```bash
kubectl apply -f deploy/deployment.yaml
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/vector.yaml
```

Or all at once — ordering is only relevant on first apply:

```bash
kubectl apply -f deploy/
```

## RBAC

The helper's ClusterRole grants read-only `list`/`watch` on every resource
type it can discover, plus `get`/`list`/`watch`/`create`/`update`/`patch` on
ConfigMaps (to write the generated config):

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "nodes", "namespaces"]
    verbs: ["list", "watch"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["list", "watch"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["list", "watch"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
```

Discovery is cluster-scoped by design — the helper needs to see workloads
across all namespaces to discover targets. The ConfigMap write permission can
be narrowed to a single namespace by swapping the ClusterRole/Binding for a
Role/RoleBinding scoped to the ConfigMap's namespace (see
[Narrowing ConfigMap permissions](#narrowing-configmap-permissions)).

The Vector consumer only needs to read the generated ConfigMap, so it ships
with a namespaced Role + RoleBinding limited to `configmaps` `get`/`list`/`watch`.

## Resource requirements

The helper is lightweight. Defaults in the Deployment manifest:

| Resource | Request | Limit |
|---|---|---|
| CPU | 50m | 200m |
| Memory | 64Mi | 256Mi |

These handle clusters with several thousand scrape targets. Scale memory
upward for very large clusters (the target snapshot and rendered YAML are held
in memory).

## Health

The helper serves `GET /health` on `METRICS_LISTEN_ADDR` (default `:9090`),
returning `204 No Content`. Wire this into the Deployment as a readiness/liveness
probe so the pod is only marked ready once the server is up.

## Multi-environment patterns

### Per-cluster deployment

Each cluster runs its own helper with a distinct `CLUSTER_LABEL`. All metrics
from that cluster are tagged, making them distinguishable downstream:

```yaml
env:
  - name: CLUSTER_LABEL
    value: "dmz-prod-1"
  - name: SCRAPE_INTERVAL
    value: "30s"
```

```yaml
env:
  - name: CLUSTER_LABEL
    value: "lan-dev-1"
  - name: SCRAPE_INTERVAL
    value: "15s"
```

### Namespace-scoped discovery

Set `NAMESPACE` to restrict the informers to a single namespace. This lowers
API server load and limits discovery scope:

```yaml
env:
  - name: NAMESPACE
    value: "monitoring"
```

### Namespace include/exclude

For cluster-wide discovery with exclusions, use the allowlist/denylist. The
allowlist is empty by default (allow all); the denylist is applied after it:

```yaml
env:
  - name: NAMESPACE_EXCLUDE
    value: "kube-system,kube-public"
```

```yaml
env:
  - name: NAMESPACE_INCLUDE
    value: "production,staging"
```

### Role selection

Enable only the roles you need. Most setups need `pod` plus one endpoint role:

```yaml
env:
  - name: ROLES
    value: "pod,endpointslice"
```

For full parity with a Prometheus `endpoints`-role config:

```yaml
env:
  - name: ROLES
    value: "pod,endpoints"
```

Enable all six roles for maximum coverage (at the cost of more API traffic):

```yaml
env:
  - name: ROLES
    value: "pod,endpointslice,endpoints,service,node,ingress"
```

### Narrowing ConfigMap permissions

To restrict ConfigMap writes to the helper's own namespace, replace the
ClusterRole + ClusterRoleBinding for ConfigMaps with a namespaced pair. Discovery
resources (pods, services, etc.) still require a ClusterRole:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: vector-k8s-helper-configmap
  namespace: kube-system
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: vector-k8s-helper-configmap
  namespace: kube-system
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: vector-k8s-helper-configmap
subjects:
  - kind: ServiceAccount
    name: vector-k8s-helper
    namespace: kube-system
```

Then remove the `configmaps` entry from the ClusterRole.

## Container image

### Build

```bash
docker build -t vector-k8s-helper .
```

The Dockerfile is a two-stage build:
1. **Builder** — `golang:1.26-bookworm`, produces a static CGO-free binary.
2. **Runtime** — `gcr.io/distroless/static-debian12:nonroot`, minimal attack
   surface, runs as non-root.

### Released images

The release workflow publishes multi-arch (`linux/amd64`, `linux/arm64`)
images to GHCR on every tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Images land at `ghcr.io/sonroyaalmerol/vector-k8s-helper:<version>`,
`ghcr.io/sonroyaalmerol/vector-k8s-helper:<major>.<minor>`, and
`ghcr.io/sonroyaalmerol/vector-k8s-helper:<major>`.

### Private registry

Update the Deployment's `image` field:

```yaml
image: your-registry.example.com/vector-k8s-helper:v0.1.0
```

## Vector integration

The sample `deploy/vector.yaml` is a complete consumer. It:

1. Mounts the generated `vector-scrape-config` ConfigMap at
   `/etc/vector/discovered/`.
2. Mounts a `vector-main` ConfigMap whose `vector.yaml` includes both the
   discovered config and the sinks config via `configs:`.
3. Runs Vector with `--watch-config` so it hot-reloads when the ConfigMap
   volume syncs.
4. Pipes `enrich_metrics` (the helper's transform) into a
   `prometheus_remote_write` sink.

Point the sink at your remote write endpoint (Mimir, Thanos, Cortex, or any
Prometheus-remote-write-compatible store):

```yaml
sinks:
  prometheus_remote_write:
    type: prometheus_remote_write
    inputs: ["enrich_metrics"]
    endpoint: "https://mimir.example.com/api/v1/push"
    tls:
      verify_certificate: true
      include_system_ca_certs_pool: true
```

## Troubleshooting

### Helper logs

```bash
kubectl logs -n kube-system -l app=vector-k8s-helper -f
```

Logs are JSON. Key events:

```json
{"level":"INFO","msg":"configuration loaded","roles":"pod,endpointslice","scrape_interval":"30s"}
{"level":"INFO","msg":"started kubernetes informers","roles":"pod,endpointslice"}
{"level":"INFO","msg":"targets changed","count":42}
{"level":"INFO","msg":"updated configmap","namespace":"kube-system","name":"vector-scrape-config","size":9831}
```

### ConfigMap not updating

- Check RBAC: `kubectl auth can-i patch configmap vector-scrape-config --as=system:serviceaccount:kube-system:vector-k8s-helper -n kube-system`
- Confirm the helper's `NAMESPACE` env matches the ConfigMap's namespace.
- Look for `failed to write configmap` errors in the logs (conflict retries
  are normal transient; persistent failures indicate an RBAC or API issue).

### No targets discovered

- Verify annotations: `kubectl get pod <name> -o jsonpath='{.metadata.annotations}'`
  — look for `prometheus.io/scrape: "true"`.
- Confirm the pod has an IP: `kubectl get pod <name> -o jsonpath='{.status.podIP}'`
- Check for `vector.dev/exclude: "true"`.
- Check namespace filters — `NAMESPACE_INCLUDE` / `NAMESPACE_EXCLUDE` may be
  hiding the namespace.
- Check the `ROLES` env — the role matching your workload must be enabled.

### Vector not picking up changes

- Confirm the ConfigMap is mounted:
  `kubectl exec <vector-pod> -- ls /etc/vector/discovered/`
- Vector must run with `--watch-config`. The sample manifest includes it.
- ConfigMap volume mounts sync on the kubelet's relist interval (up to ~60s by
  default). This is a Kubernetes limitation, not a Vector one.

### Verify the generated config

```bash
kubectl get configmap vector-scrape-config -n kube-system -o jsonpath='{.data.scrape_sources\.yaml}'
```

Check that the `sources:` section lists your endpoints and the
`enrich_metrics` transform's metadata table contains your targets' `instance`
keys.

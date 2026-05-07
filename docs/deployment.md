# Deployment

## Manifests

The `deploy/` directory contains all required Kubernetes resources:

| File | Resource | Purpose |
|---|---|---|
| `deployment.yaml` | ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment | Runs the helper |
| `configmap.yaml` | ConfigMap | Initial empty scrape config |

```bash
kubectl apply -f deploy/
```

## RBAC

The helper needs cluster-scope read access to Pods and Services (to discover
scrape targets across namespaces) and read/write access to the target
ConfigMap in its own namespace:

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods", "services"]
    verbs: ["list", "watch"]         # read-only for discovery
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "create", "update", "patch"]  # write generated config
```

> **Note**: ConfigMap access is currently cluster-scoped. To restrict to a
> single namespace, replace the ClusterRole with a Role and
> ClusterRoleBinding with a RoleBinding.

## Resource Requirements

The helper is lightweight. Default limits in the Deployment manifest:

| Resource | Request | Limit |
|---|---|---|
| CPU | 50m | 200m |
| Memory | 64Mi | 128Mi |

These are sufficient for clusters with up to ~5,000 scrape targets. Scale
memory to 256Mi for larger clusters.

## Multi-Environment Patterns

### Separate Clusters

Each cluster runs its own helper with a unique `CLUSTER_LABEL`:

```yaml
# Production cluster
env:
  - name: CLUSTER_LABEL
    value: "dmz-prod-1"
  - name: SCRAPE_INTERVAL
    value: "30s"

# Staging cluster
env:
  - name: CLUSTER_LABEL
    value: "lan-dev-1"
  - name: SCRAPE_INTERVAL
    value: "15s"
```

### Single Cluster, Multiple Namespaces

To watch only a specific namespace, set `NAMESPACE`:

```yaml
env:
  - name: NAMESPACE
    value: "monitoring"
```

This uses a namespace-scoped informer, reducing API server load and
limiting discovery to that namespace.

### Namespaced ConfigMap

For environments where the helper should only manage ConfigMaps in its
own namespace, change the RBAC from ClusterRole to Role:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: vector-k8s-helper
  namespace: kube-system
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: vector-k8s-helper
  namespace: kube-system
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: vector-k8s-helper
subjects:
  - kind: ServiceAccount
    name: vector-k8s-helper
    namespace: kube-system
```

Pod and Service watch still require a ClusterRole (the helper must see
pods and services across namespaces to discover all scrape targets).

## Container Image

### Build

```bash
docker build -t ghcr.io/sonroyaalmerol/vector-k8s-helper:latest .
```

The Dockerfile uses a two-stage build:
1. **Builder** — `golang:1.26-bookworm`, compiles a static binary
2. **Runtime** — `gcr.io/distroless/static-debian12:nonroot`, minimal attack
   surface

### Multi-Architecture

The CI workflow builds for `linux/amd64` and `linux/arm64`. Push a tag to
trigger a release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Images are published to `ghcr.io/sonroyaalmerol/vector-k8s-helper`.

### Custom Registry

Update the Deployment manifest's `image` field:

```yaml
image: your-registry.example.com/vector-k8s-helper:v0.1.0
```

## Vector Integration Checklist

1. **Deploy** the helper (`kubectl apply -f deploy/`)
2. **Mount** the ConfigMap in Vector's DaemonSet:

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
   ```

3. **Add** `enrich_metrics` to your metrics sink inputs:

   ```yaml
   sinks:
     vector_metrics:
       type: vector
       inputs:
         - internal_metrics
         - enrich_metrics
       address: "nabu.sgl.com:6000"
   ```

4. **Verify** the ConfigMap is being updated:

   ```bash
   kubectl get configmap vector-scrape-config -n kube-system -o yaml
   ```

5. **Verify** Vector has loaded the dynamic config:

   ```bash
   kubectl exec -n kube-system <vector-pod> -- vector config
   ```

## Troubleshooting

### Helper logs

```bash
kubectl logs -n kube-system -l app=vector-k8s-helper -f
```

JSON-formatted logs show reconciliation events:

```json
{"level":"INFO","msg":"targets changed","count":12}
{"level":"INFO","msg":"updated configmap","namespace":"kube-system","name":"vector-scrape-config","size":1847}
```

### ConfigMap not updating

- Check RBAC permissions: `kubectl auth can-i patch configmap vector-scrape-config -n kube-system`
- Check the helper's `NAMESPACE` env var matches the ConfigMap's namespace

### Vector not picking up changes

- Confirm the ConfigMap is mounted: `kubectl exec <vector-pod> -- ls /etc/vector/`
- Vector watches for file changes via `--watch-config` or `--config-dir`; check
  that the Vector pod's `args` include `--config-dir /etc/vector/`
- ConfigMap volume mounts can take up to 60 seconds to sync (kubelet sync
  interval). This is a K8s limitation, not a Vector limitation.

### No targets discovered

- Verify pod/service annotations: `kubectl get pod <name> -o jsonpath='{.metadata.annotations}'`
- Confirm the pod has an IP: `kubectl get pod <name> -o jsonpath='{.status.podIP}'`
- Check for `vector.dev/exclude: "true"` label
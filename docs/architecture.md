# Architecture

## Overview

`vector-k8s-helper` is a single-process Kubernetes controller that turns
Prometheus-style scrape annotations into a live Vector `prometheus_scrape`
configuration. It replaces the discovery half of Alloy's
`discovery.kubernetes` + `prometheus.scrape` pipeline, leaving Vector to do
the actual scraping and remote write.

The controller has four internal stages: **discovery**, **reconcile**,
**render**, and **write**. Each runs in the same process and communicates
through in-memory channels.

## Components

### discovery (`internal/discovery`)

The `Watcher` owns one Kubernetes informer per enabled discovery role. Each
informer is a ListWatch built from the API server with an optional label and
field selector, so objects that can never match are filtered at the source
rather than in the reconciler.

Supported roles:

- **pod** — `corev1.Pod`. One target per resolved container port. Reads pod
  annotations for port, path, scheme, interval, TLS, and auth.
- **endpointslice** — `discoveryv1.EndpointSlice`. One target per endpoint
  address × resolved port. Backed by its owning Service for annotations and
  port resolution. Endpoint ready state, zone, hostname, and node name are
  emitted as metadata (not used to filter — all endpoints are scraped, matching
  Prometheus semantics).
- **endpoints** — `corev1.Endpoints` (legacy). Same target model as
  EndpointSlice. Honors the owning Service's `prometheus.io/port` annotation,
  selecting the matching subset port or synthesizing one.
- **service** — `corev1.Service`. Scrapes the service's ClusterIP (or
  ExternalName / DNS name). Reads service annotations.
- **node** — `corev1.Node`. Scrapes the node's primary address at
  `NODE_SCRAPE_PORT`.
- **ingress** — `networkingv1.Ingress`. One target per host × path, using the
  ingress scheme and the default backend.

Every informer event (add, update, delete) signals a single debounce timer.
When the timer fires — after `DEBOUNCE_INTERVAL` of quiescence — the reconciler
runs once, regardless of how many events arrived.

### reconcile (`Watcher.reconcile`)

On each run the reconciler iterates every informer store, calls the
role-specific target extractor, and collects results into a map keyed by target
name. Keying by name means the same workload discovered through two roles
(for example, a service seen via both `endpoints` and `endpointslice`) collapses
to a single target instead of producing duplicates.

Namespace filtering is applied here: `NAMESPACE_INCLUDE` acts as an allowlist
(empty = allow all), `NAMESPACE_EXCLUDE` as a denylist, evaluated in that order.

The snapshot is emitted on a buffered output channel (capacity 1). If the
consumer is slow, the latest snapshot replaces the pending one — the helper
never blocks on rendering and never serves stale configs.

### render (`internal/renderer`)

`Render` converts a `[]Target` snapshot into a Vector YAML fragment.

**Source grouping.** Targets that share scheme, path, scrape interval, scrape
timeout, TLS, and auth settings are merged into one `prometheus_scrape` source.
A cluster with a thousand pods all exposing `http://:9090/metrics` produces a
single source with a thousand endpoints.

**Enrichment transform.** A `remap` transform named `enrich_metrics` is
generated. It embeds a VRL lookup table that maps each `instance`
(`host:port`) to its full Kubernetes metadata. Vector's `prometheus_scrape`
sets `instance` to the endpoint's `host:port` automatically, so the lookup key
is always present. The transform copies metadata fields onto `.tags`, merges
object labels, and injects the static `cluster` and additional labels.

**Empty state.** When no targets are discovered, `RenderEmpty` emits a single
`no_targets` source with a zero-length endpoint list so the config always
parses.

### write (`internal/writer`)

`Writer.Upsert` creates the ConfigMap if it does not exist, otherwise applies a
JSON merge patch to its `data` key. Writes are retried on conflict
(`409`) and transient network errors with exponential backoff, up to three
attempts.

The caller (main loop) compares each rendered blob to the previous one bytewise
and skips the write entirely when nothing changed, avoiding needless API server
load and ConfigMap version churn.

## Target resolution rules

### Pod targets

A pod is scraped when:

- `prometheus.io/scrape` is `"true"` (or truthy), and
- the pod has a `podIP`, and
- it is not excluded via `vector.dev/exclude: "true"`.

Port resolution, in priority order:

1. `prometheus.io/port` (numeric) — produces a single target at that port.
2. The first declared container port that matches — used for name/protocol
   enrichment.
3. A port-less target if no port can be determined.

Scheme, path, params, interval, timeout, TLS, and auth are read from the pod
annotations with the standard defaults.

### Endpoint targets (endpointslice + endpoints)

A service's endpoints are scraped when the owning Service has
`prometheus.io/scrape: "true"`. Port resolution mirrors pods: the Service's
`prometheus.io/port` annotation selects a single subset port; otherwise every
subset port produces a target.

All endpoint addresses are emitted regardless of ready state — ready/not-ready
is metadata (`endpoint_ready`), not a filter. This matches Prometheus
`k8s_sd_configs` behavior.

### Service targets

Scrapes the Service directly. Uses `ClusterIP` for ClusterIP/NodePort/LoadBalancer
services, `ExternalName` for ExternalName services, and the
`SERVICE_DNS_SUFFIX` DNS name otherwise.

### Node targets

Uses the node's primary internal address at `NODE_SCRAPE_PORT` (default
`10250`, the kubelet secure port). Enabled via the `node` role.

### Ingress targets

One target per ingress host × path, using the ingress-defined scheme and the
default backend path. The `ingress` role is for ingresses that terminate TLS
and proxy to a metrics endpoint.

## Metadata model

Each target carries a flat set of string fields (the `Target` struct). The
renderer writes these into the VRL lookup table and the transform copies them
onto `.tags`. Roles that do not produce a field leave it empty, and empty
fields are omitted from the table.

Label and annotation maps are controlled by `INCLUDE_LABELS` (on by default)
and `INCLUDE_ANNOTATIONS`. For each key `k` the renderer emits `label_k`
(or `annotation_k`) with the value, plus a `labelpresent_k` (or
`annotationpresent_k`) companion — the latter supports relabel-style
keep/drop-on-presence rules downstream.

## Lifecycle

1. `config.Load` reads env vars and validates them.
2. `main` builds the in-cluster Kubernetes client.
3. A goroutine starts the health server on `METRICS_LISTEN_ADDR` (serves
   `/health`, returns `204`).
4. `Watcher.Run` starts the role informers, waits for cache sync, then blocks
   until the context is cancelled.
5. The main loop reads snapshots from `Watcher.Output`, renders, dedupes, and
   writes.
6. `SIGTERM`/`SIGINT` cancels the context; informers and the health server shut
   down gracefully.

## Generated config shape

```yaml
sources:
  k8s_metrics_http_metrics:
    type: prometheus_scrape
    endpoints:
      - http://10.0.1.5:9090/metrics
      - http://10.0.1.6:9090/metrics
    scrape_interval_secs: 30
    scrape_timeout_secs: 10

transforms:
  enrich_metrics:
    type: remap
    inputs: ["k8s_metrics_http_metrics"]
    source: |
      metadata = {
        "10.0.1.5:9090": {"namespace": "default", "pod": "app-0", "node": "node-a", "labels": {...}},
        "10.0.1.6:9090": {"namespace": "default", "pod": "app-1", "node": "node-b", "labels": {...}},
      }

      inst = .tags.instance
      m = metadata[inst]
      if m != null {
        .tags.namespace = m.namespace
        .tags.pod = m.pod
        # ...all metadata fields...
        .tags = merge(.tags, m.labels)
      }
      .tags.cluster = "dmz-prod-1"
```

## Graceful degradation

- **No targets** — a valid `no_targets` source is written; Vector never holds
  an invalid config.
- **ConfigMap missing** — created on the first write.
- **ConfigMap present** — patched via JSON merge patch with conflict retries.
- **Unchanged config** — write skipped via byte comparison.
- **API server churn** — debounce coalesces event bursts into one reconcile.
- **Role misconfiguration** — if no roles are enabled, `Watcher.Run` returns an
  error at startup.

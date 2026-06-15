# Architecture

The helper runs as a **sidecar** in the same pod as Vector. Each pod's helper
discovers only its own node's targets and writes a `prometheus_scrape` config
to a shared `emptyDir` volume. Vector reads it and scrapes.

```
DaemonSet pod
┌──────────────────────────────────────────┐
│  helper (sidecar)       vector (main)    │
│       │                       │          │
│  watches only            reads config    │
│  local-node pods         scrapes only    │
│  via field selector      local targets   │
│       │                       │          │
│  writes to ──── shared ──── reads from   │
│            emptyDir volume               │
└──────────────────────────────────────────┘
```

## Components

### Discovery (`internal/discovery`)

A `Watcher` manages per-role Kubernetes informers backed by client-go
`ListWatch`. Each informer streams add/update/delete events into an
in-memory store.

The pod informer is filtered by `spec.nodeName={self}` so each sidecar
only receives pods scheduled on its own node. Service, endpoint, and
node informers are cluster-wide.

The watcher exposes a single `Output() <-chan []Target` channel. Internal
events are coalesced through a debounce window and a lock-protected
snapshot before being emitted into the channel.

### Rendering (`internal/renderer`)

The renderer takes a target snapshot and produces a Vector YAML fragment
containing:

- **`prometheus_scrape` sources** — one per unique scheme + path + TLS + auth
  combination. Targets are grouped to minimise source count.
- **`enrich_metrics` transform** — a VRL `remap` transform that attaches
  Kubernetes metadata as tags. It embeds a static lookup table keyed on
  `instance = host:port` and uses `get!(metadata, [inst])` for O(1) lookup.
  A VRL guard `if .tags.node != get_env_var!(\"VECTOR_SELF_NODE_NAME\") { abort }`
  drops targets from other nodes as a safety net.

On startup, a seed config is written immediately so Vector has a valid
`enrich_metrics` before the first targets are discovered.

### Output

The rendered YAML is written to a local file (default
`/etc/vector/discovered/scrape_sources.yaml`) on a shared `emptyDir`
volume. Vector mounts the same volume and hot-reloads via `--watch-config`.

## Data flow

1. The Kubernetes API server streams pod/service/endpoint events.
2. Each informer store is walked on reconcile tick.
3. Targets are resolved: scrape annotations are read, ports are matched,
   metadata is extracted from the informer stores (node, namespace, pod).
4. The target list is deduplicated and grouped by source key.
5. The renderer builds the YAML fragment.
6. If the content differs from the previous write, the file is atomically
   replaced.
7. Vector detects the change via `--watch-config` and reloads.

## Resilience

- **Informer cache sync** — initial sync must complete before reconciliation
  starts, preventing empty writes.
- **Content-hash dedup** — the file is only rewritten when the config actually
  changes.
- **Pod field selector** — limits API server load to the pod's own node.
- **Seed config** — a valid config with `no_targets` + `enrich_metrics` is
  written before the watcher starts, so Vector never sees a broken config.

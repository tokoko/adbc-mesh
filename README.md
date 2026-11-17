# adbc-mesh

Lets untrusted code query a database through [ADBC](https://arrow.apache.org/adbc/) without
holding the database credentials or having its own network path to the data.

## Goal

A notebook, a Spark job or any other user workload should be able to run
`SELECT ... FROM orders` and get Arrow data back, while:

- it never sees the credentials: not in files, environment variables, the Kubernetes API or
  process memory;
- it can't go around the relay and talk to the backend with credentials of its own choosing;
- what it may read is decided by the backend's own access control (Postgres grants, the
  catalog's policies), not by code that runs inside the workload;
- the data costs as little as possible to deliver. Results are shared zero-copy instead of being
  serialised over a network protocol.

Kubernetes injects the relay as a sidecar, the way Istio injects its proxy: a workload opts in
with a pod label, and admission policies add the sidecar and lock the pod down.

## Architecture

```
 ┌──────────────────────────── pod (one per workload) ───────────────────────────┐
 │                                                                               │
 │  user container (untrusted)               relay-sidecar (trusted)             │
 │  ┌──────────────────────────┐             ┌──────────────────────────────┐    │
 │  │ app / Spark / pyarrow    │             │ adbc-mesh sidecar            │    │
 │  │   │                      │   SQL       │   │                          │    │
 │  │   ▼                      │ ──────────► │   ▼                          │    │     ┌──────────┐
 │  │ ADBC driver manager      │ SEQPACKET   │ real ADBC driver ────────────┼────┼───► │ backend  │
 │  │   │                      │ Unix socket │ (Postgres, Iceberg, ...)     │    │     │ Postgres │
 │  │   ▼                      │             │                              │    │     └──────────┘
 │  │ libadbc_driver_relay.so  │ ◄────────── │ copies each batch into a     │    │
 │  │ (shim, no credentials)   │ sealed memfd│ memfd, seals it, sends fd    │    │
 │  └──────────────────────────┘ per batch   └──────────────────────────────┘    │
 │        │                                     │             │                  │
 │   /run/adbc-mesh (emptyDir, memory)          │   /etc/adbc-mesh (Secret,      │
 │   shared by both, holds only the socket ─────┘   sidecar only)                │
 └───────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Crate | Runs in | Role |
|---|---|---|
| `crates/driver` | user container | ADBC driver shim, entrypoint `AdbcDriverRelayInit`. Its only option is `uri`, the socket path; any other option is refused, so the user can't point it at a backend or pass credentials. |
| `crates/sidecar` | relay-sidecar | Loads the real driver through the ADBC driver manager with options read from mounted files, listens on the socket, forwards SQL unchanged and streams results back. |
| `crates/proto` | both | Wire protocol: JSON control messages over `SOCK_SEQPACKET`, Arrow IPC in memfds passed with `SCM_RIGHTS`. |

### Data path

1. The workload loads the shim through any ADBC driver manager (Python, C, Java via JNI) with
   `uri=/run/adbc-mesh/<backend>.sock`.
2. The shim sends `Query { sql }`. The sidecar runs it with the real driver.
3. For each record batch, the sidecar writes Arrow IPC into a new memfd and seals it
   (`F_SEAL_WRITE | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL`), then passes the descriptor.
   Sealing, not read-only opening, is what makes the buffer immutable: a read-only descriptor
   could be reopened read-write through `/proc/self/fd`.
4. The shim maps the memfd and hands Arrow arrays that point into the mapping straight to the
   consumer. No copy is made on the consumer side.
5. When the consumer releases a batch, the shim sends `Release { id }`. The sidecar caps
   unreleased data per connection (`max_outstanding_bytes`) and the number of connections
   (`max_connections`).

The sidecar strips schema metadata (backends put file paths, field ids and table properties
there) and reports backend failures as a generic error, with the details in its own log.

### Access control

The relay does not parse or filter SQL. Whatever the workload may read is what the sidecar's
backend identity may read: for Postgres, the grants on the role in the credential secret
(`relay_reader` in the kind setup, which has no grant on the hidden columns). The relay's job is
to make sure the workload can only reach the backend as that identity, and never gets the
identity's credentials.

## Kubernetes deployment

Opting in takes two labels:

- the namespace: `adbc-mesh.io/enabled=true` (it must also enforce the `baseline` or
  `restricted` Pod Security Standard);
- the pod: `adbc-mesh.io/backend=postgres`.

The pod spec needs nothing else; see `deploy/kind/consumer-job.yaml`. Policies are
[Kyverno](https://kyverno.io/) `policies.kyverno.io/v1` resources. The validating ones are
compiled to native ValidatingAdmissionPolicies, so they keep being enforced if Kyverno is down.
Injection needs Kyverno: while it is down, relay pods are refused rather than created
without a sidecar.

**Injection** (`deploy/kyverno/inject-postgres.yaml`, a MutatingPolicy):

- prepends `relay-sidecar` as a native sidecar (init container with `restartPolicy: Always`)
  with a startup probe on the socket, so user containers start only once the relay is ready;
- mounts the credential Secret `adbc-mesh-postgres` into the sidecar only, and the shared
  socket directory into every container;
- removes the service account token from the pod, except for the Spark driver (see below);
- adds a projected token with audience `vault`, mounted only in the sidecar, for a future
  Vault integration;
- runs user containers as non-root, without privilege escalation, and disables service links.

**Validation** (`deploy/kyverno/validate.yaml`):

| Policy | Ensures |
|---|---|
| `adbc-mesh-credentials` | Only `relay-sidecar` in an injected relay pod mounts `adbc-mesh-*` secrets or the `vault` audience token; never through environment variables. |
| `adbc-mesh-pod-shape` | The sidecar runs first, with the approved image and exactly the injected settings and volumes; no shared process namespace, ephemeral containers, privileged or root user containers, or foreign token mounts. |
| `adbc-mesh-namespace-hardening` | No host namespaces, host paths, privileged pods or unapproved service accounts anywhere in an enabled namespace, so a pod without the label can't be used to reach the node and read the sidecar's files. |
| `adbc-mesh-namespace-pod-security` | Enabled namespaces enforce a Pod Security Standard. |

Tenant RBAC is part of the boundary: tenants must not be able to read Secrets, `exec` into or
attach to pods, create ephemeral containers, or patch pods in an enabled namespace. Cluster
admins and node access are out of scope.

To add a backend, copy the injection policy and change the label value, the Secret name and the
socket path. Run one sidecar per backend driver.

### Spark

`deploy/kind/spark/` runs classic `spark-submit` in cluster mode with
[spark-adbc](https://github.com/tokoko/spark-adbc) loading the shim through the ADBC JNI driver.
Driver and executors are each injected with their own sidecar, and each reads through it.

The driver has to create its executors, so pods using the `spark-driver` service account keep
their API token. Its Role (`deploy/kind/spark/tenant.yaml`) allows managing pods, services and
configmaps in its own namespace and nothing else, and the policies above still apply to any
pod it creates. User code in the driver holds that token, so give each tenant its own namespace.

Use `query` rather than `dbtable` when the table has columns the backend role can't read:
spark-adbc infers the schema with `SELECT *`, which the backend refuses.

## Repository layout

```
crates/            driver shim, sidecar, protocol
deploy/images/     sidecar, Python consumer and Spark images
deploy/kyverno/    policies and offline policy tests
deploy/kind/       kind cluster, Postgres backend, e2e, outage and Spark tests
test/podsim/       docker-only simulation of a relay pod (no Kubernetes)
test/bench/        relay vs direct driver benchmark (unfinished)
```

## Running

Everything runs from [pixi](https://pixi.sh); it provides Rust, kind, kubectl, helm and the
Kyverno CLI.

```sh
pixi run build          # cargo build --release
pixi run podsim         # relay pod simulated with docker: reads, zero-copy and attack checks
pixi run policy-test    # Kyverno policies against fixtures, no cluster

pixi run kind-up        # kind cluster, Kyverno, Postgres backend, images, policies
pixi run kind-e2e       # injection, admission refusals and attacks from inside a consumer
pixi run kind-outage    # policies still enforced with Kyverno scaled to zero
pixi run kind-spark-jars  # builds spark-adbc from the last commit of $SPARK_ADBC (default ~/spark-adbc)
pixi run kind-spark     # real spark-submit through the relay
pixi run kind-down
```

## Status and limitations

Prototype. Working: Postgres through the relay on kind, from Python and from Spark.

- The shim supports queries and table schemas only. `GetObjects`, `GetInfo`, partitioned
  results, cancellation, parameter binding and `ExecuteUpdate` are not implemented.
- Credentials are a static Secret read once at startup. There is no credential reload or Vault
  integration yet, although the sidecar already has a `vault` audience token for it.
- The shim is baked into consumer images rather than delivered by injection, and images are
  referenced by tag, not digest.
- Backend error messages are not forwarded to the workload.
- The relay's throughput overhead and Iceberg through the relay are not measured yet. Zero-copy
  is verified from Python, not inside the Spark JVM.

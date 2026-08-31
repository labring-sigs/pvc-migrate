# Controller Design

## Resource boundary

The local workflows are namespaced `migrate.sealos.io/v1alpha1` resources:
`Migration`, `PodMigration`, `Reservation`, `Copy`, `Backup`, `Restore`,
`Rename`, and `Move`. Their metadata namespace is the durable session namespace
and `spec.sessionNamespace` must match it. Source and destination PVC
references remain explicit in each operation's API spec, so a single-cluster Move
can span namespaces while the controller still has one clear ownership scope.

The API types live under `api/v1alpha1`. `controller-gen` owns the deepcopy and
CRD output, and controller-runtime owns the manager, cache, watch, leader
election, retries, and shutdown behavior. The session store uses the generated
typed operation APIs through `client.Client`; dynamic clients remain only for
third-party CRDs whose schemas are discovered at runtime by existing storage
and workload adapters.

Each Kind owns an independent API contract:

| Kind | Spec focus | Status focus |
| --- | --- | --- |
| `Migration` | multi-PVC offline transfer and staging | lifecycle plus per-volume sync/activation checkpoints |
| `PodMigration` | one workload, Pod/controller ownership, precopy passes | lifecycle, per-volume transfer/activation, warm-pass and OpenEBS checkpoints |
| `Reservation` | destination PVC reservation | lifecycle plus `reserved` per-volume checkpoint |
| `Copy` | finite or online data copy | lifecycle plus reservation/copy-attempt checkpoints |
| `Backup` | source PVC and object-store recovery point | lifecycle plus OpenEBS restoration checkpoints |
| `Restore` | recovery point and destination PVC creation | restore lifecycle only |
| `Rename` | PVC/PV identity replacement in one namespace | lifecycle plus active/rolled-back PVC identity |
| `Move` | PVC/PV identity transfer between namespaces | lifecycle plus active/rolled-back PVC identity |

Only deliberately small value types are shared for Kubernetes references,
planned volumes, conditions, and lifecycle history. Each operation Spec
declares its execution fields directly: for example, `Reservation` has no copy
strategy/checksum settings, `Backup` has no migration node/volume settings,
and `Restore` has only destination scheduling controls. Status detail types are
also capability-specific: reservation exposes only `reserved`, copy has no PVC
activation, offline migration has no warm-copy checkpoint, and Rename/Move
expose only PVC identity activation. The internal `domain.SessionSpec` and
`domain.SessionStatus` never appear in CRD fields; explicit adapters translate
between those execution checkpoints and the operation-specific API objects.

## Execution modes

| Mode | Session record | Execution | Intended use |
| --- | --- | --- | --- |
| `session` | ConfigMap | invoking CLI | compatibility and unsupported workflows |
| `controller` | operation-specific workflow CR | elected controller; CLI watches by default | explicit controller execution |
| `auto` | workflow CR when eligible, otherwise ConfigMap | controller or CLI | default local workflow path |

`auto` selects the matching CRD when that operation's CRD is discoverable and
the workflow is single-cluster. Missing operation CRDs fall back to ConfigMap
sessions, which makes staged CRD installation safe. Every local workflow carries explicit
source and destination namespace fields where it operates on PVCs; a resource
is stored in the configured session namespace and the controller ClusterRole
authorizes the referenced namespaces. `move` requires distinct source and
destination namespaces, while `backup`/`restore` carry explicit PVC and Secret
references. The controller watches only the configured session namespace.
ConfigMap and workflow records share the same domain state machine,
phase checkpoints, resource-version checks, session Lease fencing, rollback,
and cleanup semantics.

Explicit `controller` mode never falls back. It starts when at least one known
workflow CRD is served, watches the installed set, and rejects a command whose
operation CRD is missing.

ConfigMap sessions serialize the internal session envelope with
`apiVersion: migrate.sealos.io/v1alpha1`; CRD sessions use operation-specific
`Spec` and `Status` types and are adapted at the store boundary. The previous
API group is no longer accepted for newly read or updated sessions.

A declarative Migration may omit `status`; the first reconciliation derives a
`Planned` checkpoint and persists it. CLI-created resources add routing labels,
but those labels are optional for declarative users.

The submitting CLI performs a GET followed by a name-scoped watch of the exact
workflow resource. It reconnects from the latest resource version after watch
closure or expiration and fences the original object UID. Phase, message, and
history progress go to stderr; stdout receives the final result once, keeping
JSON and YAML machine-readable. `--wait=true` is the default, `--wait=false`
returns after submission, and the global `--timeout` bounds the watch. Failed,
deleted, or replaced resources return a nonzero exit code.

CR status is the durable user-facing progress channel. Tool Pod output remains
owned by the controller process because the submitting CLI has no direct
execution ownership; operators can read it from the controller Deployment
logs when `--stream-tool-logs` is enabled there.

## Deliberately unsupported controller workflows

Cross-cluster copy/reserve cannot be represented safely by a single in-cluster
controller without credentials for the destination API server. Those
operations remain their independent ConfigMap/session workflows. `controller`
mode rejects them with a precondition error, while `auto` and `session` execute
them through the two-cluster session service.

Backup and restore are supported by their own CRDs. Object-store credentials
are written to a per-session Secret and never to the CRD; the controller reads
that Secret when it resumes the transfer. Move is supported as a namespaced
single-cluster resource because its source and destination namespaces are
explicit in `spec` and the deployment uses a ClusterRole for PVC/PV access.

If a future API adds cross-cluster support, it should use a separate
cluster-scoped resource with explicit Secret references, namespace allowlists,
server identity checks, and dedicated RBAC. Local cross-namespace workflows
already use a namespaced control record with explicit namespace references.

## Deployment and upgrades

`PROJECT` and `config/` follow Kubebuilder layout. Run `make manifests` after
changing API markers; one CRD is generated for each local workflow and the
combined output is synchronized to `deploy/crd.yaml` for existing installation scripts. `kubectl apply -k
config/default` installs the CRD, controller namespace, RBAC, and controller
Deployment. The non-generated `config/` and `deploy/` RBAC/Deployment files are
kept as separate compatibility entrypoints so image and environment
customizations are never overwritten by `make manifests`.

Deploy at least one replica with leader election enabled. The per-session Lease
still fences each workflow, so an operator can safely run a CLI lifecycle
command while the controller is unavailable or while migrating an existing
session between backends.

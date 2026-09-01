# Controller Design

## Resource boundary

The local workflows are namespaced `migrate.sealos.io/v1alpha1` resources:
`Migration`, `PodMigration`, `Reservation`, `Copy`, `Backup`, `Restore`,
`Rename`, and `Move`. The controller-backed set excludes `Move`; its CRD stays
available for schema/status tooling, while execution remains on the session
backend. Their metadata namespace is the durable session namespace
and `spec.sessionNamespace` must match it. Controller workflows may reference
only PVCs, workloads, temporary resources, and other namespaced objects in that
namespace. Cross-namespace operations, including `Move`, remain on the
ConfigMap/session backend where the invoking identity explicitly supplies the
required cluster-wide authorization.

The controller rejects a namespaced object whose `spec.sessionNamespace` does
not equal `metadata.namespace` immediately after decoding it, before any PVC,
PV, Pod, or workload lookup. Kubernetes 1.28 CRD CEL does not expose the root
object's namespace, so this relationship is intentionally enforced by the
controller runtime; deployments that need admission-time rejection can add a
cluster admission policy for the same rule.

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
sessions, which makes staged CRD installation safe. Every local workflow carries
explicit source and destination namespace fields, but controller admission and
reconciliation require each one to equal the CR metadata namespace. The manager
watches all namespaces and applies this check before touching any referenced
object. `Move` and other cross-namespace flows are routed to ConfigMap sessions.
For `PodMigration`, the controller also validates every persisted workload GVK
against the adapter allowlist (core `Pod`, apps `Deployment`/`StatefulSet`,
the pinned VMCluster and Grafana versions, and supported KubeBlocks versions).
Unknown API versions or kinds remain available to session mode only; they are
rejected before a dynamic client lookup in controller mode.
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

Backup and restore are supported by their own CRDs. They require an
administrator-managed cluster-scoped `ObjectStoreProfile`. The profile owns
the HTTPS endpoint, provider, region, bucket, optional base prefix, and access
policy. Static profiles require an administrator-owned controller credentials
Secret; workload-identity profiles may omit that Secret when the controller
Deployment itself has an ambient cloud identity. The controller uses the
configured Secret or ambient identity for S3 locks, manifests, and inventory
checks. A profile may also bind transfer Pods to workload identity
ServiceAccounts; the controller and transfer credentials are intentionally
separate.
Static-credential profiles must set
`allowStaticCredentialsInTenantNamespace: true` and list exactly one
`allowedNamespaces` entry;
workload-identity profiles must list explicit `serviceAccountRefs` entries.
Each workload-identity entry contains the tenant namespace, ServiceAccount
name, administrator-observed ServiceAccount UID, and a SHA-256 identity
fingerprint covering IAM annotations, labels, and token automount settings.
There is no wildcard
namespace or same-name ServiceAccount fallback. The controller appends
`clusters/<sha256(kube-system-uid)[0:32]>/namespaces/<workflow-namespace>` to
every profile-backed object prefix, so a shared profile cannot be used to guess
another tenant's recovery point or collide with an identical profile in a
different cluster. When
configured, the credentials Secret must live in the controller installation
namespace and contain non-empty `accessKey` and `secretKey` entries.
`serviceAccountRefs` selects an administrator-preprovisioned ServiceAccount in
the workflow namespace. In workload identity mode the controller uses the
profile Secret when present or its ambient cloud identity, while the transfer
Pod uses `env_auth=true` with the bound ServiceAccount. Static profiles are the
exception: the transfer chart currently materializes a short-lived rclone
Secret in the tenant namespace, so they require explicit tenant Secret-read
restrictions. Workflow CRDs contain only the profile name and recovery-point
parameters; credentials and connection overrides are rejected and never written
to workflow spec. Backup/Restore status records the profile,
credential Secret, workload-identity ServiceAccount UIDs, and identity
fingerprints. In-place Secret data rotation is allowed; deleting/recreating an
identity or changing its IAM annotations, labels, or automount setting fails
the workflow closed and requires a new profile/workflow. Admission policy
should still deny tenant mutation of bound ServiceAccounts to avoid intentional
denial of service.

The transfer chart currently needs an rclone config Secret in the PVC's
namespace. Consequently, profile credentials are materialized there for the
duration of a backup or restore and are removed during normal Helm cleanup.
This is a deliberate trust boundary: tenants who can read arbitrary Secrets in
their namespace can read the profile credential while a transfer is running.
Static-credential profiles therefore require tenant Secret read access to be
restricted, or should be replaced with the workload identity form. Session-mode
CLI workflows may use a namespaced credentials Secret under the caller's own
authorization.

`Move` is deliberately unsupported in controller mode because a single
namespaced CR cannot safely express cross-namespace authorization. It remains
available through the ConfigMap/session path. Cross-cluster operations remain
session/CLI workflows because the controller has no second API server identity.
Controller startup reads the local `kube-system` namespace UID to establish the
cluster scope. If that identity cannot be read, the controller refuses to
start; this prevents silently falling back to a shared, collision-prone object
prefix. The profile prefix is limited to 639 characters, leaving room for the
controller-owned cluster and namespace segments, the maximum recovery-point
name, and the completion manifest suffix within S3's 1024-byte key limit.

If a future API adds cross-cluster support, it should use a separate
cluster-scoped resource with explicit destination-cluster Secret references,
namespace allowlists, server identity checks, and dedicated RBAC.

## Deployment and upgrades

`PROJECT` and `config/` follow Kubebuilder layout. Run `make manifests` after
changing API markers; one CRD is generated for each local workflow and the
combined output is synchronized to `deploy/crd.yaml` for existing installation scripts. `kubectl apply -k
config/default` installs the CRD, controller namespace, RBAC, and controller
Deployment. The non-generated `config/` and `deploy/` RBAC/Deployment files are
kept as separate compatibility entrypoints so image and environment
customizations are never overwritten by `make manifests`.

Deploy at least one replica with leader election enabled. The per-session Lease
still fences each workflow. A failed CR is quiescent and is polled at the
controller requeue interval; an explicit CLI `resume` reactivates its recorded
checkpoint, after which the elected controller continues execution.

The bundled `pvc-migrate` ClusterRole is the controller's execution identity,
not a tenant role. It can read and mutate PVCs, PVs, Pods, Secrets, workload
objects, and workflow CRs across namespaces because the transfer charts need
those permissions. Grant tenants only namespaced create/get/list/watch
permissions for the workflow kinds they are allowed to submit; grant
`/status` read access for observation and keep `/status` update/patch reserved
for the controller. Never bind the controller ClusterRole to an end user or
tenant workload. The controller reads
static object-store credentials only from its configured installation namespace,
and profile validation prevents a tenant CR from selecting an arbitrary Secret.
The bundled role intentionally omits Secret `list`; controller code reads named
Secrets only. For workload identity, prevent tenants from modifying the
administrator-bound ServiceAccount's IAM annotations, labels, or
`automountServiceAccountToken` with RBAC and admission policy. The current CLI
resume/abort/rollback/cleanup paths that mutate status or perform recovery
cleanup therefore require the operator/controller identity; tenant-facing
automation should submit a new CR and watch status. A future intent resource
can expose those lifecycle actions without granting tenants status writes.

`config/rbac/tenant-role.yaml` is the optional least-privilege tenant
ClusterRole. It intentionally excludes `Move`, because cross-namespace Move
continues to use the ConfigMap/session backend. Bind it with a `RoleBinding` in each approved tenant namespace;
the file is deliberately excluded from `config/default` and must never be
bound cluster-wide:

```bash
kubectl apply -f config/rbac/tenant-role.yaml
kubectl -n tenant-a create rolebinding pvc-migrate-tenant \
  --clusterrole pvc-migrate-tenant \
  --serviceaccount tenant-a:tenant-runner
```

A static transfer profile is intentionally explicit and single-tenant. Its
controller credential Secret is also used by the transfer Pod:

```yaml
apiVersion: migrate.sealos.io/v1alpha1
kind: ObjectStoreProfile
metadata:
  name: tenant-a-s3
spec:
  backend: s3
  bucket: pvc-backups
  endpoint: https://s3.example.com
  allowedNamespaces: [tenant-a]
  allowStaticCredentialsInTenantNamespace: true
  credentialsSecret:
    name: pvc-backups
```

For production, prefer workload identity for the transfer Pod. The administrator first creates the
ServiceAccount in the tenant namespace, obtains its UID, and records that exact
binding and fingerprint in the cluster-scoped profile:

```bash
kubectl -n tenant-a get serviceaccount pvc-migrate-s3 -o jsonpath='{.metadata.uid}'
```

```yaml
spec:
  backend: s3
  bucket: pvc-backups
  serviceAccountRefs:
    - namespace: tenant-a
      name: pvc-migrate-s3
      uid: "<service-account-uid>"
      identityFingerprint: "<64-lowercase-hex-sha256>"
```

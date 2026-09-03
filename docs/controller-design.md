# Controller Design

## Resource boundary

The API group is `migrate.sealos.io/v1alpha1`. Scope follows the operation's
actual authority boundary:

| Operation | Namespaced | Cluster-scoped |
| --- | --- | --- |
| migration | `Migration` | `ClusterMigration` |
| pod migration | `PodMigration` | `ClusterPodMigration` |
| reservation | `Reservation` | `ClusterReservation` |
| copy | `Copy` | `ClusterCopy` |
| backup | `Backup` | none |
| restore | `Restore` | none |
| rename | `Rename` | none |
| move | none | `ClusterMove` |

The operation-specific API types own their fields and statuses. The internal
`domain.SessionSpec` and `domain.SessionStatus` are translation details and do
not appear in CRD schemas. Kubebuilder/controller-runtime owns API generation,
deepcopy code, status subresources, watches, leader election, and retries.

Namespaced workflows have a strict tenant boundary. `metadata.namespace` is
the single namespace authority; namespaced specs and their local resource
references contain no namespace selectors. Source, temporary, destination,
session, workload, and repository namespaces are derived from metadata during
conversion to the internal execution model. A namespaced `BackupRepository`
can read only a Secret in its own namespace.

Cluster workflows have no metadata namespace. They require explicit namespace
roles at the top level of each spec; nested PVC and workload references are
relative to those roles. Their status types retain fully qualified references
for audit and restart recovery. PVs remain cluster-scoped and omit namespace.
Cluster workflow RBAC is restricted to the controller/operator identity.

The namespace roles are operation-specific:

- `ClusterMigration` declares source, temporary, final destination, and session namespaces.
- `ClusterPodMigration` declares source, temporary, and session namespaces; workload and
  final PVC identities stay in the source namespace.
- `ClusterReservation` and `ClusterCopy` declare source, actual destination, and session
  namespaces; they have no activation stage or separate temporary namespace in their API.
- `ClusterMove` declares source, destination, and session namespaces.

`volumes[].destinationPVC` is relative to the namespace that owns the actual
destination storage: `temporaryNamespace` for migration workflows and
`destinationNamespace` for reservation and copy workflows.

Backup and restore remain namespaced because the repository and credential
Secret are tenant-owned and local. Rename is inherently same-namespace, while
Move is inherently cross-namespace, so each exposes only the scope that can
represent its semantics safely.

Cross-cluster operations still use session mode. A controller connected to one
API server cannot safely act on a second cluster without a separately managed
destination identity and lifecycle protocol.

## Execution modes

| Mode | Persistence | Execution |
| --- | --- | --- |
| `session` | ConfigMap session | invoking CLI |
| `controller` | operation-specific CRD | elected controller |

The CLI defaults to `session`; `controller` is selected explicitly when the
controller and the required workflow CRDs are installed.

`controller` mode never silently falls back. It discovers each served workflow
kind independently. The CLI chooses a namespaced kind for tenant-local work and
a cluster kind when namespace roles differ. Every cluster-scoped workflow also
accepts equal namespace roles, so an administrator can use the cluster API for a
same-namespace operation when cluster-level authority or a uniform automation
interface is required. The CLI creates the CR, then watches that exact object by
resource version until a terminal status. It reconnects after watch closure or
expiration and reports CR status history and conditions while the controller owns
tool Pod logs. `--wait=false` returns after creation; the default waits for
completion.

`Reservation` is an optional two-phase workflow. It provisions and retains the
destination PVCs, allowing capacity, topology, quota, and scheduling checks to be
completed before a later `Copy` or migration cutover. `migrate` and `copy` still
perform reservation internally for the one-command path; the standalone command
and CRD are useful when an operator needs to hand off or inspect that checkpoint.

## Backup repositories

`BackupRepository` is a user-configured, namespaced backup location. It is a
configuration object, not an authorization lease or a tenant grant. Its
`spec.type` selects exactly one backend configuration:

```yaml
apiVersion: migrate.sealos.io/v1alpha1
kind: BackupRepository
metadata:
  name: team-backups
  namespace: tenant-a
spec:
  type: s3
  s3:
    bucket: pvc-backups
    prefix: tenant-a
    endpoint: https://s3.example.com
    credentialsSecret:
      name: s3-credentials
```

The current controller implements the `s3` backend. S3 endpoint, encryption,
bucket, prefix, and Secret data are validated before execution; the workflow
pins repository UID, generation, and credential Secret UID so replacement or
configuration changes fail closed and require a new workflow. Object keys are
scoped by a hashed cluster identity and the workload namespace.

The API also reserves a structured `pvc` backend:

```yaml
spec:
  type: pvc
  pvc:
    claimRef:
      name: backup-volume
    subPath: snapshots
```

PVC repositories contain no object-store credentials and remain namespaced.
The data-plane adapter is not enabled yet; a PVC repository is rejected with a
clear precondition status instead of being interpreted as S3. Adding future
backends is additive: each gets a new typed field and controller adapter while
the union validation prevents mixed configurations.

Namespaced `Backup` and `Restore` use a name-only `repositoryRef`, which is
resolved in the workflow namespace. No cluster-scoped backup workflow or
repository resource is part of the API; this prevents a cluster object from
becoming an indirect path to another namespace's credentials.

## Status and recovery

The controller initializes a missing status to `Planned`, persists every phase
checkpoint through the status subresource, and resumes idempotently after
restarts. A failed workflow remains quiescent until an explicit resume. Session
Leases fence concurrent execution, and controller-owned CR finalizers protect
resources during explicit cleanup. A namespaced workflow keeps that protection
when it alone is deleted, while the controller releases it when the containing
namespace is terminating so the workflow cannot block namespace deletion.
ConfigMap sessions intentionally have no finalizer: session mode has no
always-running reconciler, so a finalizer could otherwise block namespace
deletion indefinitely. UID and resource-version checks reject replacement or
stale updates.

## Deployment and RBAC

Run `make manifests` after API changes. It regenerates deepcopy code, one CRD
per resource, and the combined `deploy/crd.yaml` installation artifact.

Deploy the controller with leader election and at least one replica. Bind the
controller ClusterRole only to the controller ServiceAccount. Tenant roles
should grant create/get/list/watch on the namespaced workflow kinds they may
submit and status read access; status writes, Secret reads, PV access, and
cross-namespace workflow permissions remain controller/operator privileges.
The bundled controller role reads repository credentials by name and also
creates, updates, and deletes short-lived Helm release Secrets. Helm's
default Secret storage driver lists release history by label; this access is
required by the upstream
pv-migrate Helm driver and is limited to the controller/operator identity.

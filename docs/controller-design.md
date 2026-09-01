# Controller Design

## Resource boundary

The API group is `migrate.sealos.io/v1alpha1`. Each operation has its own
namespaced and cluster-scoped workflow resource:

| Operation | Namespaced | Cluster-scoped |
| --- | --- | --- |
| migration | `Migration` | `ClusterMigration` |
| pod migration | `PodMigration` | `ClusterPodMigration` |
| reservation | `Reservation` | `ClusterReservation` |
| copy | `Copy` | `ClusterCopy` |
| backup | `Backup` | `ClusterBackup` |
| restore | `Restore` | `ClusterRestore` |
| rename | `Rename` | `ClusterRename` |
| move | `Move` | `ClusterMove` |

The operation-specific API types own their fields and statuses. The internal
`domain.SessionSpec` and `domain.SessionStatus` are translation details and do
not appear in CRD schemas. Kubebuilder/controller-runtime owns API generation,
deepcopy code, status subresources, watches, leader election, and retries.

Namespaced workflows have a strict tenant boundary. Their metadata namespace,
`spec.sessionNamespace`, and every namespaced source, destination, temporary,
workload, and repository reference must identify the same namespace. A
namespaced `BackupRepository` can read only a Secret in its own namespace.

Cluster workflows have no metadata namespace. They require explicit namespace
fields on every namespaced reference and are the only workflow form allowed to
cross namespaces. PVs remain cluster-scoped and must omit namespace. Cluster
workflow RBAC is therefore restricted to the controller/operator identity.

Cross-cluster operations still use session mode. A controller connected to one
API server cannot safely act on a second cluster without a separately managed
destination identity and lifecycle protocol.

## Execution modes

| Mode | Persistence | Execution |
| --- | --- | --- |
| `session` | ConfigMap session | invoking CLI |
| `controller` | operation-specific CRD | elected controller |
| `auto` | CRD when the matching kind is served, otherwise ConfigMap | controller or CLI |

`controller` mode never silently falls back. `auto` discovers each served
workflow kind independently, chooses a namespaced kind for same-namespace work,
and chooses the matching cluster kind when namespaces differ. The CLI creates
the CR, then watches that exact object by resource version until a terminal
status. It reconnects after watch closure or expiration and reports CR status
history and conditions while the controller owns tool Pod logs. `--wait=false`
returns after creation; the default waits for completion.

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
resolved in the workflow namespace. `ClusterBackup` and `ClusterRestore` use a
name-plus-namespace `repositoryRef`, allowing an operator to select an
existing namespaced repository while keeping the credential Secret boundary
visible and auditable. A cluster-scoped repository resource is intentionally not
part of the API; it would make administrator credentials indirectly available
to tenant workflows.

## Status and recovery

The controller initializes a missing status to `Planned`, persists every phase
checkpoint through the status subresource, and resumes idempotently after
restarts. A failed workflow remains quiescent until an explicit resume. Session
Leases fence concurrent CLI/controller execution, and finalizers protect
resources during cleanup. UID and resource-version checks reject replacement or
stale updates.

## Deployment and RBAC

Run `make manifests` after API changes. It regenerates deepcopy code, one CRD
per resource, and the combined `deploy/crd.yaml` installation artifact.

Deploy the controller with leader election and at least one replica. Bind the
controller ClusterRole only to the controller ServiceAccount. Tenant roles
should grant create/get/list/watch on the namespaced workflow kinds they may
submit and status read access; status writes, Secret reads, PV access, and
cross-namespace workflow permissions remain controller/operator privileges.
The bundled controller role reads named Secrets and does not require Secret
list permission.

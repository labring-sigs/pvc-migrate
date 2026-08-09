# pvc-migrate

`pvc-migrate` is a resumable Kubernetes CLI for moving filesystem PVC data between storage classes, namespaces, and nodes. It supports warm copies before downtime, a final offline sync, workload-aware cutover, rollback, and S3-compatible backup and restore.

## Highlights

- Migrate every PVC attached to a supported Pod as one consistency unit.
- Reduce downtime with configurable warm-copy passes followed by a final offline sync.
- Validate topology, scheduling, quota, RBAC, dependencies, consumers, and controller ownership before execution.
- Resume interrupted sessions from their persisted phase.
- Preserve the source PV for an explicit rollback window.
- Move or rename offline PVC identities while retaining their PV.
- Back up and restore PVC files through S3-compatible object storage.

## Requirements

- A Kubernetes cluster with filesystem PVC support
- Kubernetes credentials with the permissions validated by the relevant `plan` command
- Network access for the temporary tool image on source and target nodes
- Go 1.26.5 or a compatible newer toolchain when building from source

## Installation

Build the CLI:

```bash
make build VERSION=0.1.0
./bin/pvc-migrate version
```

Install the session namespace and reference RBAC for an in-cluster service account:

```bash
kubectl apply -f deploy/session-namespace.yaml
kubectl apply -f deploy/rbac.yaml
```

A locally executed CLI uses the identity from its kubeconfig and requires equivalent permissions.

Build the tool image. It runs the CLI by default and also supplies PVC reservation, rsync, SSHD, and rclone roles inside the cluster:

```bash
docker build --build-arg VERSION=0.1.0 -t pvc-migrate:0.1.0 .
docker run --rm pvc-migrate:0.1.0 version
```

Use `--tool-image registry.example/pvc-migrate:0.1.0` when cluster nodes pull the image from an internal registry. New migration sessions persist this image reference and reuse it during resume.

## Quick Start

Every mutating command defaults to `--dry-run=true`. Execution requires an explicit `--dry-run=false`; workload pause and storage identity changes also require `--yes` or interactive approval.

### 1. Plan

Inspect the Pod, PVCs, target node, storage topology, permissions, quotas, consumers, and workload adapter:

```bash
pvc-migrate \
  --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --output yaml \
  migrate-pod plan \
  --session database-20260809 \
  --source-namespace application \
  --temporary-namespace pvc-migrate-system \
  --pod database-1 \
  --target-node worker-b \
  --destination-storage-class fast-local
```

### 2. Migrate

Run one warm-copy pass, pause the workload, perform the final sync, activate the destination PVs, and resume the workload:

```bash
pvc-migrate \
  --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --timeout 45m \
  --yes \
  migrate-pod \
  --session database-20260809 \
  --source-namespace application \
  --temporary-namespace pvc-migrate-system \
  --pod database-1 \
  --target-node worker-b \
  --destination-storage-class fast-local \
  --precopy-passes 1 \
  --verify-checksum \
  --dry-run=false
```

### 3. Inspect or Resume

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  session status database-20260809

pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes session resume database-20260809 --dry-run=false
```

### 4. Roll Back or Finalize

Restore the original PVs during the rollback window:

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes session rollback database-20260809 --dry-run=false
```

Close the rollback window after validating the application:

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes session cleanup database-20260809 --dry-run=false \
  --delete-rollback-pv --finalize --delete-session
```

`--delete-rollback-pv` deletes the retained PV object. Its `Retain` policy preserves the backend volume for storage-provider cleanup.

## Migration Workflow

```text
plan -> reserve -> warm copy -> pause -> final sync -> activate -> resume
```

The destination PVCs are provisioned before downtime. Warm-copy passes run while the workload remains available. The final sync begins after the selected workload adapter confirms that PVC consumers have stopped, and it establishes the cutover consistency point. The source PV remains retained until rollback or final cleanup.

## Supported Workloads

| Workload | Cutover behavior | Result |
| --- | --- | --- |
| Standalone Pod | Delete and recreate the recorded Pod on the target node | Supported with a Pod restart window |
| Native StatefulSet | Scale from `N` replicas to the selected ordinal `k`, then restore `N` | Supported when PVC retention and ordinal ownership checks pass |
| KubeBlocks component | Optionally switch the primary, stop every Cluster component, then restore each original state | Supported with cluster-wide downtime |
| VMCluster component | Pause the component and reduce its replica count to ordinal `k`, then restore it | Supported for managed VMCluster StatefulSets |
| Grafana | Pause the Grafana deployment and scale it to zero, then restore it | Supported when recreation scheduling checks pass |
| Victoria Logs `vlstorage` | Scale the complete `vlstorage` StatefulSet to zero under a session-owned pause lock | Supported with a shared `vlstorage` pause window |
| RWX or multiple consumers | Run a file-level pass after consumer validation | Application quiescence defines transactional consistency |
| MinIO Tenant | Use MinIO drive or pool maintenance | Rejected during planning |
| CockroachDB | Use drain, decommission, and CockroachDB recovery procedures | Rejected during planning |
| Backup archive-WAL helper | Use the owning backup controller workflow | Rejected during planning |

For a KubeBlocks primary, `--kubeblocks-candidate` requests an automated switchover. The plan also prints `kbcli cluster promote` guidance and a matching OpsRequest YAML when the installed KubeBlocks API accepts that operation. `--allow-leader-downtime` acknowledges a direct primary restart when the application can tolerate it.

Controller ownership outside the supported adapters causes the plan to fail. PVCs that are already offline can use `migrate`, `copy`, `rename`, or `move` directly.

## Safety and Recovery

- Session state is stored in `pvc-migrate-session-<id>` ConfigMaps.
- Every mutating session command uses a renewable Kubernetes Lease for exclusive ownership.
- PVC and PV mutations verify recorded UIDs, bindings, and session ownership.
- Source and destination PVs use `Retain` throughout cutover and rollback.
- Replacement PVCs receive API-server dry-run validation before activation.
- A failure after workload pause preserves the paused workload and its resumable session.
- `session abort` restores a paused workload before activation and retains staged storage for cleanup.
- `session rollback` restores application PVC identities to the retained source PVs.
- `session cleanup --finalize` restores the active PV reclaim policy and releases session ownership.

See [Architecture](docs/architecture.md) for consistency boundaries and [Runbook](docs/runbook.md) for recovery procedures.

## Command Reference

| Command | Purpose |
| --- | --- |
| `<operation> plan` | Validate an operation and print its resource inventory |
| `reserve` | Provision and retain staged destination PVCs |
| `copy` | Run a resumable offline copy or one online warm-copy pass without cutover |
| `final-sync` | Pause the recorded workload and run the final offline sync |
| `activate` | Bind staged PVs to application PVC identities |
| `migrate` | Run reserve, copy, pause, final sync, activation, and resume |
| `migrate-pod` | Migrate every PVC of one supported Pod as a unit |
| `rename` | Rename one offline PVC while retaining its PV |
| `move`, `mv` | Move one offline PVC identity to another namespace |
| `backup`, `live-backup` | Copy PVC files to S3-compatible object storage |
| `restore` | Restore a published recovery point into a PVC |
| `session status` | Show one session or list sessions |
| `session resume` | Continue from the persisted phase |
| `session abort` | Restore a paused workload before activation |
| `session rollback` | Restore retained source PVs and resume the workload |
| `session cleanup` | Delete staged resources or close the rollback window |
| `completion` | Generate shell completion |
| `version` | Print version information |

Read-only commands omit `--dry-run`. Mutating commands expose a local `--dry-run` flag that defaults to `true`. Table, JSON, and YAML output formats are available for automation.

Stable exit codes are validation `2`, precondition `3`, conflict `4`, Kubernetes `5`, copy `6`, timeout `7`, and internal `1`.

## Direct Copy

The default `copy` mode requires zero active Pod consumers. `copy --online` allows active consumers for one finite warm-copy pass. Both modes finish after data copy and leave application PVC identities unchanged. `migrate` and `migrate-pod` provide managed final sync and cutover.

The planner infers the source helper node from active consumers. `--source-node` supplies an explicit node when the storage backend requires one. RWOP consumers and consumers spread across multiple nodes require separate sessions or an application-specific workflow.

```bash
pvc-migrate copy --dry-run=false \
  --source-namespace application \
  --destination-namespace archive \
  --source-pvc database-data \
  --destination-storage-class fast \
  --target-node worker-b

pvc-migrate copy --dry-run=false --online \
  --source-namespace application \
  --destination-namespace archive \
  --source-pvc database-data \
  --source-node worker-a \
  --target-node worker-b
```

Cross-namespace copies reuse the source PVC name by default. Same-namespace copies generate a session-suffixed destination name unless `--destination-pvc` is supplied.

## Backup and Restore

`backup` requires an offline source by default. `backup --online` and `live-backup` perform one best-effort file-level pass while consumers remain active. Each completed backup publishes an immutable recovery point under a unique `--name`.

The completion manifest records the source PVC identity, capacity, VolumeMode, path, consistency boundary, object count, total bytes, and inventory digest. Restore validates the manifest and inventory before and after synchronization. The requested `--path` must match the published path.

S3-compatible credentials can come from the AWS default credential chain, explicit credential flags, or a Kubernetes Secret selected with `--credentials-secret`. Custom services such as MinIO use `--endpoint`, `--region`, and `--s3-provider`.

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  live-backup --dry-run=false \
  --namespace application \
  --source-pvc database-data \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260809

pvc-migrate --kubeconfig /path/to/kubeconfig \
  restore --dry-run=false \
  --namespace application \
  --destination-pvc database-restore \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260809
```

Online backup provides a best-effort crash-consistent file copy. Application-consistent database recovery requires quiescence, a filesystem snapshot, or a database-native backup. File contents and paths are preserved; POSIX ownership, permissions, ACLs, extended attributes, hard links, device files, and empty directories remain outside the backup contract.

Offline backup and restore require the workload owner to remain quiesced for the complete operation because controllers can recreate PVC consumers after preflight.

## Operational Boundaries

- Filesystem PVCs define the persistent-data boundary.
- Pod `emptyDir` volumes follow their Kubernetes ephemeral lifecycle.
- The workflow uses finite warm-copy passes followed by a final offline sync.
- Database-native CDC, WAL tailing, and continuous file watching remain application responsibilities.
- Storage backend cleanup follows the active reclaim policy and CSI implementation.

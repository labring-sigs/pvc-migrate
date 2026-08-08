# pvc-migrate

`pvc-migrate` is a resumable Kubernetes CLI for filesystem PVC migration. It provisions destination storage before downtime, performs warm rsync passes through the embedded [`pv-migrate`](https://github.com/utkuozdemir/pv-migrate) library, pauses a supported workload, performs a final offline sync, rebinds the application PVC identity, and preserves the source PV for an explicit rollback window.

## Safety Model

- Migration state lives in a ConfigMap named `pvc-migrate-session-<id>`.
- Every PVC and PV mutation verifies recorded UID, binding, and session ownership.
- ConfigMap `resourceVersion` provides optimistic concurrency between CLI processes.
- Every mutating session command acquires a renewable Kubernetes Lease in the session namespace; lease loss cancels the operation context and fences subsequent mutations.
- Source and destination PVs use `Retain` throughout cutover and rollback.
- Activation validates the replacement PVC with an API-server dry-run before changing the PV claim reservation.
- A final offline sync establishes the cutover consistency point.
- Failures after workload pause preserve the stopped workload and the resumable session.
- Finalize restores the active PV reclaim policy and releases its session ownership.

The detailed consistency and recovery boundaries are in [docs/architecture.md](docs/architecture.md) and [docs/runbook.md](docs/runbook.md).

Migration and S3 helper containers use zero CPU and memory requests/limits and a zero
ephemeral-storage request. The ephemeral-storage limit is omitted because a zero limit
causes kubelet eviction as soon as the container writes logs or its writable layer. A
namespace `ResourceQuota` still counts Pod, Job, Secret, Service, and ConfigMap objects;
the zero compute requests remove their requested CPU, memory, and ephemeral-storage
quantity from quota accounting.

## Build

Requirements:

- Go 1.26.5 or a compatible newer toolchain
- `golangci-lint` v2.12.2 or newer for `make check`
- Kubernetes credentials with the permissions checked by `plan`
- A Kubernetes cluster with filesystem PVC support
- Network reachability and image access for the Helm resources created by `pv-migrate`

```bash
make check
make build VERSION=0.1.0
./bin/pvc-migrate version
```

The optional in-cluster service account and its reference permissions are in `deploy/`:

```bash
kubectl apply -f deploy/session-namespace.yaml
kubectl apply -f deploy/rbac.yaml
```

A locally executed CLI uses the identity from its kubeconfig. That identity needs equivalent permissions.

## Typical Migration

Inspect a complete plan before creating resources:

```bash
./bin/pvc-migrate \
  --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --output yaml \
  migrate-pod plan \
  --session database-20260807 \
  --source-namespace application \
  --temporary-namespace pvc-migrate-system \
  --pod database-1 \
  --target-node worker-b \
  --destination-storage-class fast-local
```

Run a multi-PVC Pod migration as one consistency unit:

```bash
./bin/pvc-migrate \
  --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --timeout 45m \
  --yes \
  migrate-pod \
  --session database-20260807 \
  --source-namespace application \
  --temporary-namespace pvc-migrate-system \
  --pod database-1 \
  --target-node worker-b \
  --destination-storage-class fast-local \
  --precopy-passes 1 \
  --verify-checksum \
  --dry-run=false
```

Inspect and recover the persistent session:

```bash
./bin/pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  session status database-20260807

./bin/pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes session resume database-20260807 --dry-run=false
```

Close the rollback window after application validation:

```bash
./bin/pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes session cleanup database-20260807 --dry-run=false \
  --delete-rollback-pv --finalize --delete-session
```

`--delete-rollback-pv` deletes the retained PV object. A `Retain` policy preserves storage backend data. Backend deletion follows the StorageClass and CSI operator procedure.

## Commands

| Command | Purpose |
| --- | --- |
| `<operation> plan` | Inventory PVCs, workload ownership, topology, scheduling, quota, LimitRange, RBAC, dependencies, and network policy for that operation. |
| `reserve` | Create and bind staged PVCs, including a target-node consumer for WFFC classes. |
| `copy` | Reserve a direct destination and perform a resumable offline pass or one explicit online warm pass. |
| `final-sync` | Pause the recorded workload and perform the final offline sync. |
| `activate` | Rebind staged PVs to the application PVC names while the workload remains paused. |
| `migrate` | Compose all stages for offline PVCs or explicitly quiesced workloads. |
| `migrate-pod` | Compose all stages for every PVC of one supported Pod. |
| `rename` | Rebind one offline PVC name within the source namespace to the same retained PV. |
| `move`, `mv` | Rebind one offline PVC to another namespace while retaining the same PV; the destination name defaults to the source name. |
| `backup`, `live-backup`, `restore` | Copy PVC data to or from S3-compatible object storage. |
| `session status` | Print one session or list sessions. |
| `session resume` | Continue from the persisted stage or `resumeFrom` phase. |
| `session abort` | Resume a paused workload before activation and retain staged storage for cleanup. |
| `session rollback` | Restore application PVCs to retained source PVs and resume the workload. |
| `session cleanup` | Delete staged resources, close the rollback window, release ownership, and delete session state. |

Every mutating command exposes a local `--dry-run` flag that defaults to `true`. Plan subcommands and other read-only commands do not expose this flag. Use `--dry-run=false` on the mutating command for an approved execution. JSON and YAML output are designed for automation. Error categories map to stable exit codes: validation `2`, precondition `3`, conflict `4`, Kubernetes `5`, copy `6`, timeout `7`, and internal `1`.

### Copy Modes

`copy` accepts `--source-namespace`, `--destination-namespace`, `--source-pvc`, `--destination-pvc`, `--destination-storage-class`, and `--target-node`. `--source-node` pins the source helper when the storage backend or an active consumer requires a specific node.

The default copy mode is offline and requires zero active Pod consumers. `copy --online` performs one finite warm pass while writable PVC consumers continue running. The planner infers a unique source helper node from active consumers; `--source-node` provides an explicit value when needed. RWOP consumers and consumers spread across multiple nodes require a different workflow or separate sessions. The command copies data into the destination PVC and leaves cutover to the staged `final-sync` and `activate` commands.

Cross-namespace copy uses the source PVC name by default because the destination namespace provides a separate identity. Same-namespace copy generates a session-suffixed destination name unless `--destination-pvc` is supplied.

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

### Backup Modes

The default `backup` path expects the source PVC to have no Pod consumers and provides an offline file-consistency boundary. The `--online` flag, or the `live-backup` alias, keeps application Pods running and mounts the source read-only in a short-lived helper Job. A failed or incomplete pass can be retried with the same name while its completion manifest is absent. A published recovery point is immutable, so each completed backup uses a new `--name`.

Online backup provides best-effort crash-consistent file copies. Database transactions, open-file ordering, and application caches require an application quiesce, filesystem snapshot, or database-native backup for an application-consistent recovery point. Object backups copy files individually and use no archive compression; the dry-run output reports the destination prefix and this compression policy. File-level S3 copies preserve file contents and paths; owner/group/mode, ACL, xattr, hardlink, device-file, and empty-directory metadata require an application-specific export or a future archive format.

The immutable completion manifest records the source PVC identity, capacity, VolumeMode, subdirectory path, consistency boundary, compression policy, object count, total bytes, and an inventory SHA-256 over sorted object keys, sizes, and ETags. Backup publishes the manifest after collecting the inventory. Restore validates the manifest and inventory before the helper starts, then verifies the inventory again after synchronization. The inventory detects deleted, added, or changed object versions; file-level content checksums and POSIX metadata require an application-specific export or a future archive format. The requested `--path` must match the published path.

The manifest schema is version 2. Recovery points created by earlier builds need a fresh backup so the inventory fields are available.

Backup and restore dry-runs also check the helper Job, Pod, Secret, and ServiceAccount object quotas, including the Helm release Secret (or ConfigMap when `HELM_DRIVER=configmap`), and reject positive Container/Pod `LimitRange` minimums before Helm creates resources. A namespace quota on `limits.ephemeral-storage` is rejected because the helper deliberately omits that limit to avoid zero-limit eviction.

The embedded pv-migrate rsync, sshd, and rclone helper images use the pinned `v3.6.1` release tag. For a custom S3 endpoint, the generated rclone configuration defaults to the provider-neutral `Other` mode; AWS uses `AWS` when no endpoint is supplied, and `--s3-provider` selects a specific compatible-service dialect.

Consumer inspection happens immediately before the helper starts. Kubernetes has no atomic PVC lock that controllers honor, so a Deployment or StatefulSet can recreate a consumer after inspection; offline backup/restore therefore require the workload owner to remain quiesced for the entire helper lifetime. The CLI cannot guarantee controller quiescence from a PVC reference alone.

The backup and restore destination is S3-compatible object storage. The CLI exposes the S3 backend with a custom provider, endpoint, region, and credentials for AWS or compatible services such as MinIO. S3-compatible storage is the supported durable target.

An online backup is one finite synchronization pass:

```text
start helper Job/Pod
  -> mount source PVC read-only while the application keeps running
  -> run one rclone sync (an incomplete pass can be retried before publication)
  -> wait for Job completion
  -> uninstall the helper Helm release and clean up its Job/Pod
```

The helper does not remain active as a watcher and does not automatically run a second pass after the application stops. `migrate` and `migrate-pod` implement the cutover sequence separately: warm copy, workload pause, final offline sync, PV/PVC activation, workload resume, and Ready verification. The final sync is the consistency point for migration; `live-backup` remains a best-effort file-level backup operation.

S3-compatible backup and restore:

```bash
./bin/pvc-migrate --kubeconfig /path/to/kubeconfig \
  live-backup --dry-run=false \
  --namespace application \
  --source-pvc database-data \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260807

./bin/pvc-migrate --kubeconfig /path/to/kubeconfig \
  restore --dry-run=false \
  --namespace application \
  --destination-pvc database-restore \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260807
```

## Workload Adapters

- Standalone Pod: stores the full Pod object, deletes it for final sync, and recreates it on the destination node. Rollback recreates it on the source node.
- StatefulSet: supports ordinal `k` by scaling replicas from `N` to `k`, covering Pods `k..N-1`, then restoring `N`.
- KubeBlocks: discovers the served OpsRequest API for optional primary switchover, prints `kbcli cluster promote` and a matching `kubectl create` OpsRequest YAML when admission accepts the operation, then sets and restores every selected `componentSpecs[].stop` field so its PVCs remain available for final sync. Components whose webhook rejects Switchover require their native procedure or `--allow-leader-downtime`. Component-wide downtime is reported in the plan.
- VMCluster: pauses the VMCluster component and scales the selected StatefulSet ordinal out and back in.
- Grafana: sets the Grafana deployment pause state and scales its Deployment to zero and back.
- Victoria Logs `vlstorage`: uses a session-owned pause annotation, scales the entire StatefulSet to zero, and restores the original replica count.
- MinIO Tenant, CockroachDB, and Backup archive-WAL helpers: planning fails with guidance to use the controller's official maintenance or backup workflow.

Controller ownership outside these adapters causes a failed plan. `migrate` remains suitable for PVCs that are already offline.

Pod `emptyDir` volumes follow their Kubernetes ephemeral lifecycle and remain outside persistent-data scheduling blockers. PVC-backed volumes remain the migration data boundary.

### Approximate Real-Time Migration Matrix

Approximate real-time migration means that a warm file copy runs while the workload is serving traffic, followed by a short pause for final sync and PV/PVC activation. The final sync always requires the workload's supported pause mechanism and establishes the cutover consistency point.

| Workload | Warm copy | Pause during cutover | Final sync and result |
| --- | --- | --- | --- |
| Standalone Pod | Supported | Delete the source Pod and recreate it from the recorded spec | Supported; service has a pause window while the Pod is recreated |
| Native StatefulSet | Supported | Scale replicas from `N` to the selected ordinal `k` and restore `N` afterward | Supported; PVC retention and ordinal ownership are checked |
| KubeBlocks component | Supported when its PVC consumers pass preflight | Set every selected `componentSpecs[].stop` field and wait for consumers to disappear | Supported; component-wide downtime is required to keep PVCs intact |
| VMCluster component | Supported | Set component `paused` and scale the selected StatefulSet ordinal to zero | Supported; component-level downtime |
| Grafana | Supported | Set Grafana `spec.deployment.spec.paused` and scale its Deployment to zero | Supported; Deployment is restored and Ready is verified |
| Victoria Logs `vlstorage` | Supported | Set a session-owned pause lock and scale the entire StatefulSet to zero | Supported; all `vlstorage` replicas share the pause window and the original count is restored |
| RWX or multiple PVC consumers | Supported as a file-level pass | Application-specific quiesce remains required for transactional consistency | Supported; recovery point is best-effort until the final offline sync |
| MinIO Tenant | Rejected by this migrator | Use MinIO drive/pool maintenance | Migrator does not take ownership of the cutover |
| CockroachDB | Rejected by this migrator | Use drain/decommission and CockroachDB recovery procedures | Migrator does not take ownership of the cutover |
| Backup archive-WAL helper | Rejected by this migrator | Use the backup controller workflow | Migrator does not take ownership of the helper |

The matrix describes the implemented controller adapters and their consistency boundaries. It does not provide continuous file watching, WAL tailing, database-native CDC, or a permanently running backup Job.

### Container Image

The repository ships a multi-stage, non-root image based on distroless:

```bash
docker build --build-arg VERSION=$(git describe --tags --always --dirty) -t pvc-migrate:dev .
docker run --rm pvc-migrate:dev version
```

The image contains the CLI binary only. Kubernetes credentials and S3 credentials are supplied at runtime through the kubeconfig, AWS credential chain, or the documented Kubernetes Secret flags.

## Testing

```bash
make test
make test-race
make vet
```

The opt-in E2E creates a unique namespace, migrates a WFFC local volume between two nodes, verifies its digest, rolls it back, verifies it again, and removes its resources:

```bash
PVC_MIGRATE_E2E=1 \
PVC_MIGRATE_E2E_KUBECONFIG=/path/to/kubeconfig \
make e2e
```

Environment overrides include `PVC_MIGRATE_E2E_SOURCE_CLASS`, `PVC_MIGRATE_E2E_DESTINATION_CLASS`, `PVC_MIGRATE_E2E_TARGET_NODE`, `PVC_MIGRATE_E2E_HELPER_IMAGE`, and `PVC_MIGRATE_E2E_BINARY`.

The reference E2E path has been validated on Kubernetes `v1.28.15` with `openebs-hostpath` and `openebs-backup` WFFC classes across two worker nodes. The imported pv-migrate chart and `clusterip` strategy completed both warm and final copies in that environment.

The exhaustive isolated-cluster manual matrix, evidence, defects, and cleanup checks are in [docs/manual-validation.md](docs/manual-validation.md).

The embedded upstream dependency is pinned to commit `22a469151ecaf3e4c529437193380eba23949165` through pseudo-version `v1.8.1-0.20260802124747-22a469151eca`. All copy calls enter the public `pvmigrate` package through `internal/copyengine`.

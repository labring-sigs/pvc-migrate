# pvc-migrate

`pvc-migrate` is a resumable Kubernetes CLI for moving filesystem PVC data between storage classes, namespaces, and nodes. It supports warm copies before downtime, a final offline sync, workload-aware cutover, rollback, and S3-compatible backup and restore.

## Highlights

- Migrate every PVC attached to a supported Pod as one consistency unit.
- Reduce downtime with configurable warm-copy passes followed by a final offline sync.
- Validate topology, scheduling, quota, RBAC, dependencies, consumers, and controller ownership before execution.
- Resume interrupted sessions from their persisted phase.
- Follow generated reservation, rsync, SSHD, and rclone Pod logs in the active CLI.
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

The default ClusterRole excludes Pod exec. KubeBlocks MongoDB native switchover needs it for the source namespace only:

```bash
SOURCE_NAMESPACE=app
kubectl apply -f deploy/kubeblocks-mongodb-rbac.yaml
kubectl create rolebinding pvc-migrate-kubeblocks-mongodb \
  --namespace "$SOURCE_NAMESPACE" \
  --clusterrole pvc-migrate-kubeblocks-mongodb \
  --serviceaccount pvc-migrate-system:pvc-migrate
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

Inspect the Pod, automatically selected target node, CSI-reported storage capacity, topology, permissions, quotas, consumers, and workload adapter:

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

If a PVC or PV still has session ownership after its session ConfigMap was lost, validate the reconstructed resource relationship and then clear it:

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  session cleanup-orphan database-20260809 \
  --source-namespace application --source-pvc data-database-1

pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes session cleanup-orphan database-20260809 \
  --source-namespace application --source-pvc data-database-1 \
  --dry-run=false
```

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
| KubeBlocks InstanceSet | Optionally switch the primary, pause InstanceSet reconciliation, delete the selected Pod, then restore the original pause state | Supported with selected-instance downtime; sibling Pods remain running |
| KubeBlocks legacy StatefulSet | Optionally switch the primary, stop the selected Cluster component, then restore its original state | Supported with selected-component downtime |
| VMCluster component | Pause the component and reduce its replica count to ordinal `k`, then restore it | Supported for managed VMCluster StatefulSets |
| Grafana | Pause the Grafana deployment and scale it to zero, then restore it | Supported when recreation scheduling checks pass |
| Victoria Logs `vlstorage` | Scale the complete `vlstorage` StatefulSet to zero under a session-owned pause lock | Supported with a shared `vlstorage` pause window |
| RWX or multiple consumers | Run a file-level pass after consumer validation | Application quiescence defines transactional consistency |
| MinIO Tenant | Use MinIO drive or pool maintenance | Rejected during planning |
| CockroachDB | Use drain, decommission, and CockroachDB recovery procedures | Rejected during planning |
| Backup archive-WAL workload | Use the owning backup controller workflow | Rejected during planning |

For a KubeBlocks primary, `--kubeblocks-candidate` requests an automated switchover. The plan also prints `kbcli cluster promote` guidance and a matching OpsRequest YAML when the installed KubeBlocks API accepts that operation. For MongoDB components whose OpsRequest admission reports that switchover is unsupported, pvc-migrate validates and runs the KubeBlocks MongoDB native candidate script; choose a Ready, caught-up secondary as the candidate. Failure guidance includes the fully resolved equivalent command. The native command has this form:

```sh
kubectl --namespace <namespace> exec <current-primary-pod> -c mongodb -- env \
  KB_CONSENSUS_LEADER_POD_FQDN=<current-primary-pod>.<cluster>-<component>-headless \
  KB_SWITCHOVER_CANDIDATE_FQDN=<ready-secondary-pod>.<cluster>-<component>-headless \
  /scripts/switchover-with-candidate.sh
```

`--allow-leader-downtime` acknowledges a direct primary restart when the application can tolerate it.

InstanceSet-backed components use the served `spec.paused` field to suspend InstanceSet reconciliation while the selected Pod is migrated. The adapter deletes that Pod with a UID precondition and verifies the InstanceSet pause owner before final sync. Legacy StatefulSet-backed components use the selected Cluster component's `spec.componentSpecs[].stop` field. The `kubeblocks.io/reconcile` annotation triggers reconciliation and has no pause semantics.

Controller ownership outside the supported adapters causes the plan to fail. PVCs that are already offline can use `migrate`, `copy`, `rename`, or `move` directly.

## Safety and Recovery

- Session commands print phase-aware next steps, verification commands, and validated dry-run/execute pairs on stderr. Suggested `pvc-migrate` and `kubectl` commands preserve the active kubeconfig, context, session namespace, and explicitly changed execution settings they need. Argument quoting follows POSIX shell syntax by default, PowerShell syntax in detected PowerShell environments and native Windows, and POSIX syntax in Windows MSYS shells. JSON and YAML results remain a single structured document on stdout. With `--log-format=json`, stderr is JSON Lines for progress events, guidance, and failures.
- Text logs support `--color=auto|always|never`. `auto` colors interactive terminal output, `always` forces ANSI colors for terminal multiplexers, and `never` keeps stderr plain for text collectors. Levels use severity colors, component and tool prefixes use stable per-value colors, and guidance uses semantic colors for phases and actions while keeping command bodies plain. JSON logs remain ANSI-free.
- `migrate-pod` stops with an already-satisfied check when the Pod uses the requested target node and every PVC keeps its current StorageClass. Use `--force-reprovision` for an intentional backing-PV replacement on the same node and StorageClass.
- Tool Pod logs stream to stderr by default and remain available in the command output after short-lived Pods are removed. Use `--stream-tool-logs=false` for quiet automation.
- Each tool-backed stage runs a short-lived probe Pod on every selected source and target node before starting the stage. Image pull, scheduling, security-context, shell, rsync, SSHD, and rclone failures retain the session record and surface before data transfer or workload mutation; backup and restore probes run while their operation lock is held.
- For an active OpenEBS LVM LocalPV, a warm-copy plan reads the existing source PV's `LVMVolume.spec.shared` value instead of relying on the StorageClass default. If the current volume is not shared, the plan offers `--precopy-passes 0` for an offline final sync. `--openebs-lvm-enable-shared` is an explicit alternative: it temporarily patches that LVMVolume to `shared=yes`, then verifies a same-node second-Pod read-write mount without writing data. The StorageClass is never modified, and the original LVMVolume field value is restored after every warm-copy pass, including probe and copy failures. This does not provide cross-node RWX storage.
- Session state is stored in `pvc-migrate-session-<id>` ConfigMaps.
- Session ConfigMaps carry a protection finalizer and are deleted through validated session cleanup.
- Every mutating session command uses a renewable Kubernetes Lease for exclusive ownership.
- PVC and PV mutations verify recorded UIDs, bindings, and session ownership.
- Source and destination PVs use `Retain` throughout cutover and rollback.
- Replacement PVCs receive API-server dry-run validation before activation.
- Recreated PVC workflows reject unknown custom finalizers during planning; Kubernetes PVC protection remains controller-managed, stale CSI binding metadata is omitted, and the external resizer writes its annotation when a later expansion begins.
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
| `session cleanup-orphan` | Validate and clear ownership after a session ConfigMap was lost |
| `completion` | Generate shell completion |
| `version` | Print version information |

Read-only commands omit `--dry-run`. Mutating commands expose a local `--dry-run` flag that defaults to `true`. Table, JSON, and YAML output formats are available for automation.

Stable exit codes are validation `2`, precondition `3`, conflict `4`, Kubernetes `5`, copy `6`, timeout `7`, and internal `1`.

## Direct Copy

The default `copy` mode requires zero active Pod consumers. `copy --online` allows active consumers for one finite warm-copy pass. Both modes finish after data copy and leave application PVC identities unchanged. `migrate` and `migrate-pod` provide managed final sync and cutover.

The planner infers the source tool node from active consumers and selects a Ready, schedulable target node that satisfies Pod scheduling, StorageClass topology, and available CSI capacity signals. `--target-node` accepts a node name or `auto`; `auto` is the default and prefers nodes with sufficient reported capacity and then a node different from the source. `--source-node` supplies an explicit node when the storage backend requires one. RWOP consumers and consumers spread across multiple nodes require separate sessions or an application-specific workflow.

`--capacity-awareness=auto` is the default. Matching `CSIStorageCapacity` objects enforce reported capacity and `maximumVolumeSize`; missing capacity information produces a warning and reservation performs the final provisioning check. `require` makes missing capacity information a failed plan, and `off` disables the API lookup.

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
For multiple explicit destination names, pass `--destination-pvc source-pvc-name=destination-pvc-name` once per source PVC; mappings must be complete and unique.

### Destination Capacity

`reserve`, `copy`, `migrate`, and `migrate-pod` accept `--destination-capacity` because they create destination PVCs. Omit it to keep each source PV capacity. Pass one value to apply it to every source PVC, or use explicit `source-pvc-name=capacity` entries for multiple PVCs. Plans and session status show both source and destination capacities.

The planner rejects a destination smaller than its source PV by default. Add `--allow-volume-shrink` only after confirming that the copied data fits in every smaller PVC. pvc-migrate never mounts a source volume to measure usage during planning or execution. It accepts usage only from a trusted adapter for a known storage-backend CRD; provisioned capacity is not treated as used bytes. When no trusted adapter is available, the plan is blocked unless `--skip-source-usage-check` explicitly accepts the risk. The current release has no trusted usage adapter for OpenEBS LVM, OpenEBS HostPath, or S3 CSI because their CRDs do not expose per-volume filesystem usage. These flags apply only to a new session and cannot change an existing session. `rename` and `move` preserve PVC identity and do not expose capacity flags.

For an orchestrated migration, the recreated application PVC uses the requested destination capacity after activation. Rollback recreates it with the original source PVC request.

```bash
pvc-migrate copy --dry-run=false \
  --source-namespace application \
  --source-pvc database-data \
  --destination-capacity 200Gi

# For a Pod with two PVCs, map each source PVC by name.
pvc-migrate migrate-pod plan \
  --source-namespace application \
  --pod database-1 \
  --destination-capacity data=200Gi \
  --destination-capacity logs=256Gi

pvc-migrate copy --dry-run=false \
  --source-namespace application \
  --source-pvc database-data \
  --destination-capacity 32Gi \
  --allow-volume-shrink \
  --skip-source-usage-check
```

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

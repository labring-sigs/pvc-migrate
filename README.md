# pvc-migrate

`pvc-migrate` is a resumable Kubernetes CLI for moving filesystem PVC data between storage classes, namespaces, and nodes. It provides separate offline PVC migration and real-time Pod migration workflows, plus copy/backup/restore operations.

## Highlights

- `migrate` performs an offline PVC migration and accepts explicit PVC and destination identities.
- `migrate-pod` performs a real-time migration of every PVC attached to one supported Pod.
- Reduce downtime with configurable warm-copy passes followed by a final offline sync in `migrate-pod`.
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
- Go 1.27.0 or a compatible newer toolchain when building from source

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

The bundled ClusterRole is a high-privilege controller identity. Bind it only
to the controller ServiceAccount; tenant users should receive narrowly scoped
namespaced permissions to submit and observe the workflow CRs they own.
The role reads repository credentials by exact name and grants the controller
the create/update/delete Secret verbs required by Helm's default release
storage driver. It intentionally omits Secret `list`.

Build the tool image. It runs the CLI by default and also supplies PVC reservation, rsync, SSHD, and rclone roles inside the cluster:

```bash
docker build --build-arg VERSION=0.1.0 -t pvc-migrate:0.1.0 .
docker run --rm pvc-migrate:0.1.0 version
```

Use `--tool-image registry.example/pvc-migrate:0.1.0` when cluster nodes pull the image from an internal registry. New migration sessions persist this image reference and reuse it during resume.

## Execution Modes

The CLI supports two durable execution backends:

The default mode is `session`. Use `--mode=controller` when the controller and
the required workflow CRDs are installed.

- `--mode=session` always stores sessions in ConfigMaps and executes the workflow in the invoking process.
- `--mode=controller` stores local sessions as operation-specific `migrate.sealos.io/v1alpha1` CRs. Same-namespace work uses `Migration`, `PodMigration`, `Reservation`, `Copy`, `Backup`, `Restore`, or `Rename`. Cross-namespace migration, reservation, and copy use their `Cluster*` kind. Pod migration with administrator-selected temporary or session namespaces uses `ClusterPodMigration` while keeping the workload and PVC identities in the source namespace. PVC identity moves use `ClusterMove`. Backup, restore, and rename intentionally have no cluster-scoped form. Cross-cluster workflows remain on the ConfigMap/session backend. The controller uses leader election, watches every installed workflow kind, and reuses the same resumable app.Service state machine. The CLI watches that CR and waits for completion by default; use `--wait=false` for detached submission. A command fails clearly when its matching CRD is absent.

Install the controller backend with:

```bash
kubectl apply -f deploy/crd.yaml
kubectl apply -f deploy/session-namespace.yaml
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/controller.yaml
```

The repository also exposes the standard Kubebuilder/kustomize entrypoint:

```bash
kubectl apply -k config/default
```

Run `make manifests` after changing API markers. It regenerates the typed
deep-copy code and the CRD under `config/crd/bases`, then synchronizes the
single-installation `deploy/crd.yaml` file.

The resource boundary and unsupported-workflow decisions are documented in
[`docs/controller-design.md`](docs/controller-design.md).

Namespaced workflow CRDs use `metadata.namespace` as their tenant boundary.
Their specs and local object references expose no namespace fields; conversion
to the execution model derives source, temporary, destination, session, and
repository namespaces from metadata. Cluster workflow specs declare each
operational namespace once at the top level and keep nested references local
to the relevant source or destination namespace.
Backup and restore use a namespaced `BackupRepository` for a user-selected
location. `spec.type` selects a structured backend configuration. `s3` is
currently executable and reads its credentials from a Secret in the repository
namespace. The API also defines a `pvc` backend for a future data-plane
adapter; the current controller rejects it explicitly instead of treating it
as S3. Creating a repository configures a location and does not grant access
to other namespaces. Namespaced workflows use a name-only `repositoryRef`;
the repository and its Secret must be in the workflow namespace. Backup and
restore have no cluster-scoped workflow resource, preventing an operator API
from becoming an indirect cross-namespace credential path. The controller
scopes object keys by cluster and workload namespace and pins repository
UID/generation in workflow status, so replacing a repository requires a new
workflow while in-place Secret rotation remains possible. Cross-cluster
commands remain ConfigMap/session workflows because they require a second API
server identity.

The bundled ClusterRole is controller-only. Tenant bindings should grant
namespaced workflow create/get/list/watch permissions and `/status` read only;
`/status` update/patch, repository reads, and Secret access stay with the
controller/operator identity. The current lifecycle commands that
perform abort, rollback, cleanup, or failed-workflow reactivation therefore
require that operator identity in controller mode.

Submit a supported migration and wait for its CR status to reach completion:

```bash
pvc-migrate --mode=session --yes migrate \
  --source-namespace application --source-pvc data \
  --destination-pvc data --dry-run=false
kubectl -n application get migrations
```

Controller progress is read from the CR's durable status and written to
stderr; the final table, JSON, or YAML document is written once to stdout.
Lifecycle commands use `--workflow-namespace application` to address a
tenant-scoped CR; the default remains the global `--session-namespace` for
ConfigMap/session workflows.
`--timeout` bounds planning, submission, and waiting. A failed or deleted CR
returns a nonzero exit code. Use `--wait=false` when another process owns
observation. Tool Pod logs are emitted by the controller process, so inspect
the controller Deployment logs when transfer-level output is needed.

The controller image defaults to `ghcr.io/labring-sigs/pvc-migrate:dev`; set the image explicitly in `deploy/controller.yaml` for a released build.

### Real-cluster E2E

The E2E suite defaults to the synchronous ConfigMap backend. It creates an
isolated namespace per test, writes real PVC data, verifies migration and
rollback digests, and removes its workflow records, Leases, PVCs, and PVs:

```bash
PVC_MIGRATE_E2E_KUBECONFIG=/path/to/kubeconfig \
PVC_MIGRATE_E2E_TOOL_IMAGE=registry.example/pvc-migrate:0.1.0 \
make e2e
```

Run the same suite through operation-specific CRDs with:

```bash
PVC_MIGRATE_E2E_KUBECONFIG=/path/to/kubeconfig \
PVC_MIGRATE_E2E_TOOL_IMAGE=registry.example/pvc-migrate:0.1.0 \
make e2e PVC_MIGRATE_E2E_MODE=controller
```

Controller-mode tests start a real controller-runtime manager in the test
namespace, wait for its leader-election Lease, submit the workflow CR, and
verify the operation-specific terminal status. Both source and
destination StorageClasses must be available; override them with
`PVC_MIGRATE_E2E_SOURCE_CLASS` and `PVC_MIGRATE_E2E_DESTINATION_CLASS`.

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

For an offline PVC migration, use `migrate plan` with `--source-pvc`. It does not
accept `--pod` and does not inspect workload ownership or KubeBlocks metadata:

```bash
pvc-migrate migrate plan \
  --source-namespace application \
  --source-pvc database-data \
  --destination-namespace archive \
  --destination-pvc database-data
```

### 2. Migrate

Run the complete real-time migration. Warm copy, pause, final sync, activation, and workload resume are one idempotent workflow:

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
  migrate-pod status database-20260809

pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes migrate-pod resume database-20260809 --dry-run=false
```

### 4. Roll Back or Finalize

Restore the original PVs during the rollback window:

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes migrate-pod rollback database-20260809 --dry-run=false
```

Close the rollback window after validating the application:

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes migrate-pod cleanup database-20260809 --dry-run=false \
  --delete-rollback-pv --finalize --delete-session
```

`--delete-rollback-pv` restores the rollback PV's recorded reclaim policy before deleting the PV. A recorded `Delete` policy lets Kubernetes and the CSI driver delete the backend volume. A recorded `Retain` policy preserves the backend volume.

If a PVC or PV still has session ownership after its session ConfigMap was lost, validate the reconstructed resource relationship and then clear it:

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  recovery cleanup-orphan database-20260809 \
  --source-namespace application --source-pvc data-database-1

pvc-migrate --kubeconfig /path/to/kubeconfig \
  --session-namespace pvc-migrate-system \
  --yes recovery cleanup-orphan database-20260809 \
  --source-namespace application --source-pvc data-database-1 \
  --dry-run=false
```

## Migration Workflow

```text
Offline migrate:
plan -> reserve -> final sync -> activate -> completed (one command)

Real-time Pod migrate:
plan -> reserve -> warm copy -> pause -> final sync -> activate -> resume (one command)
```

`migrate` is caller-quiesced: it accepts PVC identities and namespace/PVC
destination overrides, never discovers or pauses a workload, and never runs a
warm-copy pass. `migrate-pod` is the real-time Pod workflow: it derives the
complete PVC set from `--pod`, keeps the application PVC identities in the
source namespace, and owns workload pause/resume and cutover. Its workload
controls are limited to supported same-zone Pod migration; use `copy --online`
for cross-zone replication. It coordinates one selected workload. Consumers
from another workload make the plan fail; stop them or use offline `migrate`
after quiescing every consumer instead of implicitly grouping independent
workloads.

The destination PVCs are provisioned before downtime. Warm-copy passes run while the workload remains available. The final sync begins after the selected workload adapter confirms that PVC consumers have stopped, and it establishes the cutover consistency point. The source PV remains retained until rollback or final cleanup.

## Supported Workloads

| Workload | Cutover behavior | Result |
| --- | --- | --- |
| Standalone Pod | Delete and recreate the recorded Pod on the target node | Supported with a Pod restart window |
| Ordinary Deployment | Scale the complete Deployment to zero, switch all selected PVCs, then restore the original replica count | Supported when fully Ready, without an operator owner or HorizontalPodAutoscaler |
| Native StatefulSet | Scale from `N` replicas to the selected ordinal `k`, then restore `N` | Supported when PVC retention and ordinal ownership checks pass, without a HorizontalPodAutoscaler |
| KubeBlocks InstanceSet | Optionally switch the primary, pause InstanceSet reconciliation, delete the selected Pod, then restore the original pause state | Supported with selected-instance downtime; sibling Pods remain running |
| KubeBlocks legacy StatefulSet | Stop the affected Cluster or component through a Stop/Start OpsRequest, then restore it | Supported with Cluster- or component-wide downtime |
| VMCluster component | Pause the component and reduce its replica count to ordinal `k`, then restore it | Supported for managed VMCluster StatefulSets without a HorizontalPodAutoscaler |
| Grafana | Pause the Grafana deployment and scale it to zero, then restore it | Supported without a HorizontalPodAutoscaler when recreation scheduling checks pass |
| Victoria Logs `vlstorage` | Scale the complete `vlstorage` StatefulSet to zero under a session-owned pause lock | Supported without a HorizontalPodAutoscaler and with a shared `vlstorage` pause window |
| RWX or multiple consumers | Run a file-level pass after consumer validation | Application quiescence defines transactional consistency |
| MinIO Tenant | Use MinIO drive or pool maintenance | Rejected during planning |
| CockroachDB | Use drain, decommission, and CockroachDB recovery procedures | Rejected during planning |
| Backup archive-WAL workload | Use the owning backup controller workflow | Rejected during planning |

For a KubeBlocks primary, `--kubeblocks-candidate` requests an automated switchover. Non-MongoDB components use the served KubeBlocks Switchover action and receive matching `kbcli` and OpsRequest guidance. MongoDB InstanceSet migrations validate and call the native candidate script directly without probing an OpsRequest API; choose a Ready, caught-up secondary as the candidate. Failure guidance includes the fully resolved equivalent command. The native command has this form:

```sh
kubectl --namespace <namespace> exec <current-primary-pod> -c mongodb -- env \
  KB_CONSENSUS_LEADER_POD_FQDN=<current-primary-pod>.<cluster>-<component>-headless \
  KB_SWITCHOVER_CANDIDATE_FQDN=<ready-secondary-pod>.<cluster>-<component>-headless \
  /scripts/switchover-with-candidate.sh
```

`--allow-leader-downtime` acknowledges a direct primary restart when the application can tolerate it.

InstanceSet-backed components use the served `spec.paused` field to suspend InstanceSet reconciliation while the selected Pod is migrated. The adapter deletes that Pod with a UID precondition and verifies the InstanceSet pause owner before final sync. Legacy StatefulSet-backed components use Stop/Start OpsRequests: the apps API pauses the complete Cluster, while the operations API can target the selected component. Legacy workloads reject `--kubeblocks-candidate` because the pause operation affects every instance in its scope. The `kubeblocks.io/reconcile` annotation triggers reconciliation and has no pause semantics.

Controller ownership outside the supported adapters causes the plan to fail. PVCs that are already offline can use `migrate`, `copy`, `rename`, or `move` directly.

## Safety and Recovery

- Workflow commands print phase-aware next steps, verification commands, and validated dry-run/execute pairs on stderr. Suggested commands use the owning workflow (`migrate`, `migrate-pod`, `copy`, `backup`, `rename`, or `move`) and preserve the active kubeconfig, context, session namespace, and explicitly changed execution settings they need. JSON and YAML results remain a single structured document on stdout. With `--log-format=json`, stderr is JSON Lines for progress events, guidance, and failures.
- Text logs support `--color=auto|always|never`. `auto` colors interactive terminal output, `always` forces ANSI colors for terminal multiplexers, and `never` keeps stderr plain for text collectors. Levels use severity colors, component and tool prefixes use stable per-value colors, and guidance uses semantic colors for phases and actions while keeping command bodies plain. JSON logs remain ANSI-free.
- `migrate-pod` stops with an already-satisfied check when the Pod uses the requested target node and every PVC keeps its current StorageClass. Use `--force-reprovision` for an intentional backing-PV replacement on the same node and StorageClass.
- Tool Pod logs stream to stderr by default and remain available in the command output after short-lived Pods are removed. Use `--stream-tool-logs=false` for quiet automation.
- Each tool-backed stage runs a short-lived probe Pod on every selected source and target node before starting the stage. Image pull, scheduling, security-context, shell, rsync, SSHD, and rclone failures retain the session record and surface before data transfer or workload mutation; backup and restore probes run while their operation lock is held.
- For any active OpenEBS LVM LocalPV, warm copy reads the actual source PV and its `LVMVolume.spec.shared`; the workload adapter and StorageClass defaults do not determine this state. If the volume is unshared, use `migrate-pod --precopy-passes 0` to skip warm copy and proceed directly to controlled cutover and final sync, or explicitly pass `--openebs-lvm-enable-shared` to temporarily set `shared=yes` and verify a same-node second-Pod read-write mount. The original source setting is restored after every warm-copy attempt, including failures.
- The planner separately counts each destination PVC's consumers inside the selected migration unit. When multiple Pods will mount one RWO PVC and the destination StorageClass predicts OpenEBS LVM, `--openebs-lvm-enable-shared` authorizes the operation. Execution verifies the provisioned destination PV and matching LVMVolume, then keeps `spec.shared=yes` for the resumed application. This behavior is independent of the workload adapter and provides same-node multi-mount capability, not cross-node RWX storage. The plan rejects matching required Pod anti-affinity and `DoNotSchedule` topology-spread rules when they prevent every consumer from running on that node.
- Session state is stored in `pvc-migrate-session-<id>` ConfigMaps.
- Session ConfigMaps carry a protection finalizer and are deleted through validated session cleanup.
- Every mutating session command uses a renewable Kubernetes Lease for exclusive ownership.
- PVC and PV mutations verify recorded UIDs, bindings, and session ownership.
- Source and destination PVs use `Retain` throughout cutover and rollback.
- Replacement PVCs receive API-server dry-run validation before activation.
- Recreated PVC workflows reject unknown custom finalizers during planning; Kubernetes PVC protection remains controller-managed, stale CSI binding metadata is omitted, and the external resizer writes its annotation when a later expansion begins.
- A failure after workload pause preserves the paused workload and its resumable session.
- `<workflow> abort` restores a paused workload before activation and retains staged storage for cleanup.
- `<workflow> rollback` restores application PVC identities to the retained source PVs when that workflow changes PVC identity.
- `<workflow> cleanup --finalize` restores the active PV reclaim policy and releases session ownership.

## Command Reference

| Command | Purpose |
| --- | --- |
| `<operation> plan` | Validate an operation and print its resource inventory |
| `reserve` | Provision and retain staged destination PVCs |
| `copy` | Run a resumable offline copy or one online warm-copy pass without cutover |
| `copy cross-cluster` | Copy PVC data between two Kubernetes clusters with separate source and destination connections |
| `reserve cross-cluster` | Provision destination PVCs in another cluster and persist a cross-cluster session |
| `migrate` | Run an offline reserve, final sync, activation, and completion |
| `migrate-pod` | Run real-time warm copy, workload pause, cutover, and resume for one Pod |
| `rename` | Rename one offline PVC while retaining its PV |
| `move` | Move one offline PVC identity to another namespace |
| `backup` | Copy PVC files to S3-compatible object storage (`--online` keeps active consumers running) |
| `restore` | Restore a published recovery point into a PVC |
| `migrate status/resume/abort/rollback/cleanup` | Manage an offline migration session |
| `migrate-pod status/resume/abort/rollback/cleanup` | Manage a real-time Pod migration session |
| `reserve/copy/backup/rename/move status/resume/abort/cleanup` | Manage the lifecycle actions supported by each workflow |
| `copy cross-cluster` / `reserve cross-cluster` lifecycle commands | Inspect, continue, or clean up a cross-cluster session |
| `recovery cleanup-orphan` | Validate and clear ownership after a session record was lost |
| `controller` | Run the controller-runtime reconciliation loop for the installed local workflow CRDs |
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

### Partial Directory Transfers

`reserve`, `copy`, `migrate`, and `migrate-pod` accept optional `--source-path` and `--destination-path` directory scopes. Omit both flags to copy the full PVC. Paths are relative to the PVC root, and `.` selects the root. A single-PVC operation accepts a bare path; multi-PVC operations use explicit `source-pvc-name=relative-path` mappings. Unmapped PVCs keep the full-volume scope.

```bash
pvc-migrate copy --dry-run=false \
  --source-namespace application \
  --source-pvc database-data \
  --source-path mysql/current \
  --destination-path restored/mysql

pvc-migrate migrate-pod plan \
  --source-namespace application \
  --pod database-1 \
  --source-path data=mysql/current \
  --destination-path data=. \
  --source-path logs=archive/current \
  --destination-path logs=restored/logs
```

Each scope is persisted in the session and reused by warm copy, final sync, checksum verification, and resume. Execution verifies that source directories exist, creates nested destination directories, and rejects a selected path containing a symbolic-link component. `--delete-extraneous` remains confined to the selected destination directory. An orchestrated partial transfer still replaces the whole application PVC at cutover, so files outside each selected source directory are absent from the destination by explicit request.

### Cross-Cluster Copy

Cross-cluster workflows use explicit subcommands and keep their session state on the source cluster. The destination kubeconfig is required, and the source and destination cluster identities must differ. StorageClass objects are read-only inputs; their parameters are never changed.

```bash
pvc-migrate copy cross-cluster plan \
  --source-kubeconfig ~/.kube/source \
  --destination-kubeconfig ~/.kube/destination \
  --source-namespace application \
  --source-pvc database-data \
  --destination-namespace archive \
  --destination-pvc database-data \
  --destination-storage-class fast

pvc-migrate copy cross-cluster \
  --source-kubeconfig ~/.kube/source \
  --destination-kubeconfig ~/.kube/destination \
  --source-namespace application \
  --source-pvc database-data \
  --destination-namespace archive \
  --destination-pvc database-data \
  --destination-storage-class fast \
  --dry-run=false
```

Use `reserve cross-cluster` to provision and inspect destination PVCs before copying. `reserve cross-cluster status/resume/cleanup` and `copy cross-cluster status/resume/cleanup` require both connections so resource identities can be verified on each cluster. Multiple PVCs use explicit `source=destination`, `source=capacity`, and `source=path` mappings. Cross-cluster shrink keeps the same safety defaults as local copy: `--allow-volume-shrink` and an explicit `--skip-source-usage-check` are required when no trusted usage reader exists.

### Destination Capacity

`reserve`, `copy`, `migrate`, and `migrate-pod` accept `--destination-capacity` because they create destination PVCs. Omit it to keep each source PV capacity. Pass one value to apply it to every source PVC, or use explicit `source-pvc-name=capacity` entries for multiple PVCs. Plans and workflow-specific `status` commands show both source and destination capacities.

The planner rejects a destination smaller than its source PV by default. Add `--allow-volume-shrink` only after confirming that the copied data fits in every smaller PVC. pvc-migrate never mounts a source volume to measure usage during planning or execution. It accepts usage only from a trusted adapter for a known storage-backend CRD; provisioned capacity is not treated as used bytes. When no trusted adapter is available, the plan is blocked unless `--skip-source-usage-check` explicitly accepts the risk. The current release has no trusted usage adapter for OpenEBS LVM, OpenEBS HostPath, or S3 CSI because their CRDs do not expose per-volume filesystem usage. These flags apply only to a new session and cannot change an existing session. `rename` and `move` preserve PVC identity and do not expose capacity flags.

Destination PVCs retain the source access modes. Plans enforce the known
OpenEBS LocalPV contracts: `openebs.io/local` accepts `ReadWriteOnce`, while
`local.csi.openebs.io` accepts `ReadWriteOnce` and `ReadWriteOncePod`. Choose a
StorageClass whose driver supports the source modes. Capabilities of unknown
CSI provisioners are validated by their own admission path.

`copy`, `reserve`, and offline `migrate` can use a different destination capacity. Real-time
`migrate-pod` keeps the Pod PVC identities in the source namespace and rejects destination
namespace/PVC overrides. For a KubeBlocks `migrate-pod`, capacity is controlled by the Cluster
component template; update that template and create a new session after cleanup when the
destination is too small. Other real-time workloads use the requested destination capacity.

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

`backup` performs an offline file-consistent copy by default. Add `--online` for a best-effort crash-consistent file copy while source consumers remain active. Each completed backup publishes an immutable recovery point under a unique `--name`.

The completion manifest records the source PVC identity, capacity, VolumeMode, path, consistency boundary, object count, total bytes, and inventory digest. Restore validates the manifest and inventory before and after synchronization. The requested `--path` must match the published path.

Session-mode S3-compatible credentials can come from the AWS default credential
chain, explicit credential flags, or a Kubernetes Secret selected with
`--credentials-secret`. Controller-mode Backup and Restore require a
namespaced `BackupRepository`; the selected backend configuration, endpoint,
provider, region, bucket, prefix, and credentials are taken from that repository
and cannot be overridden by the workflow. The current controller executes only
the `s3` backend; `pvc` is reserved for a future data-plane adapter.

```bash
pvc-migrate --kubeconfig /path/to/kubeconfig \
  backup --online --dry-run=false \
  --namespace application \
  --source-pvc database-data \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260809 \
  --path mysql/current

pvc-migrate --kubeconfig /path/to/kubeconfig \
  restore --dry-run=false \
  --namespace application \
  --destination-pvc database-restore \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260809 \
  --path mysql/current
```

By default, restore requires an existing destination PVC. To create the PVC, use `--create-pvc`,
`--destination-storage-class`, and `--destination-access-mode`. Automatic creation uses the backup
capacity by default. `--destination-capacity` can increase the capacity and cannot decrease it. Use
`--target-node` when a local or `WaitForFirstConsumer` volume must bind on a selected node. A failed
restore keeps its automatically created PVC for a retry with the same recovery-point parameters.
Restore rejects a same-named PVC from another restore.

`backup` and `restore` use `--path` for one PVC subdirectory. The restore path must match the path recorded in the immutable completion manifest. Omitting `--path` selects the full PVC.

Online backup provides a best-effort crash-consistent file copy. Application-consistent database recovery requires quiescence, a filesystem snapshot, or a database-native backup. File contents and paths are preserved; POSIX ownership, permissions, ACLs, extended attributes, hard links, device files, and empty directories remain outside the backup contract.

Offline backup and restore require the workload owner to remain quiesced for the complete operation because controllers can recreate PVC consumers after preflight.

## Operational Boundaries

- Filesystem PVCs define the persistent-data boundary.
- Pod `emptyDir` volumes follow their Kubernetes ephemeral lifecycle.
- Offline `migrate` uses one terminal sync after the caller stops PVC consumers.
- `migrate-pod` uses finite warm-copy passes followed by a workload-controlled final sync.
- Database-native CDC, WAL tailing, and continuous file watching remain application responsibilities.
- Storage backend cleanup follows the active reclaim policy and CSI implementation.

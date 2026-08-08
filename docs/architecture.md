# Architecture

## Components

| Package | Responsibility |
| --- | --- |
| `cmd/pvc-migrate` | Process signals, execute Cobra, and map typed errors to exit codes. |
| `internal/cli` | Parse flags, request approval, create runtimes, and print results. |
| `internal/app` | Compose idempotent stages, retries, compensation, resume, abort, rollback, and cleanup. |
| `internal/domain` | Define plans, session schema, workload state, resource identities, phases, and errors. |
| `internal/planner` | Read the cluster and produce checks and resource estimates. |
| `internal/kube` | Persist ConfigMap sessions, reserve WFFC storage, own resources, and switch PV/PVC identities. |
| `internal/controller` | Discover, pause, verify, and resume supported workloads. |
| `internal/copyengine` | Isolate the imported `pv-migrate/pvmigrate` implementation. |
| `internal/output` | Render table, JSON, and YAML output. |

The CLI builds one typed clientset, dynamic client, and discovery client from the selected kubeconfig and context. Kubernetes objects remain typed through the planner and switcher. KubeBlocks OpsRequests use unstructured objects because their served API version varies by installation.

Each mutating command owns its `--dry-run` flag, with a default of `true`, and exposes a read-only `plan` subcommand. Resource plans inventory the exact operation mode and produce checks and estimates. Session-stage plans validate the persisted phase and resource identities. Read-only commands have no dry-run state, and the root command has no default operation-specific plan.

## Session State Machine

```text
Planned
  -> Reserving -> Reserved
  -> WarmCopying -> WarmCopied
  -> Pausing -> Paused
  -> FinalSyncing -> FinalSynced
  -> Activating -> Activated
  -> Resuming -> Completed
```

Every active phase can transition to `Failed`. `status.resumeFrom` records the interrupted phase. `session resume` verifies current resource identity and re-enters that idempotent phase. Terminal recovery paths are `Aborting -> Aborted` and `RollingBack -> RolledBack`.

The ConfigMap contains:

- source and destination PVC/PV names, UIDs, and resource versions;
- original PVC specification and preserved user metadata;
- original PV reclaim policies;
- workload adapter, Pod identity, controller identity, replica state, and affected Pods;
- warm and final sync completion times, retry count, checksum state, and last error;
- per-volume activation checkpoints and the active PVC UID;
- phase history and `resumeFrom`.

Session specifications use a discriminator with exactly one concrete payload. `reserve`, `migrate`, `migratePod`, `copy`, `rename`, and `move` each have their own typed payload; workflow options such as helper nodes, copy strategies, checksum verification, and extraneous-file policy live inside the applicable payload. `SessionCommon` contains only identities shared by every workflow: namespaces, creator, and volume records. `Session.Validate` rejects missing, mixed, or discriminator-mismatched payloads, so a persisted session cannot silently combine fields from different workflows.

ConfigMap updates carry the current Kubernetes `resourceVersion`. A competing process receives a conflict and reloads the session before continuing. Mutating session commands also claim `pvc-migrate-lock-<sha256(session-id)>` in the session namespace. The holder renews the Lease while the operation runs; a renewal, ownership, or resource-version failure cancels the operation context, and the service refuses further mutations under the fenced holder. The Lease is released only by its current holder, with expiration recovering abandoned sessions.

## Migration Transaction

Kubernetes has no transaction across PVCs, PVs, Pods, StatefulSets, and custom resources. The application service implements a durable saga:

1. Plan records immutable source identities and controller state.
2. Reserve acquires each source PVC, changes both PV policies to `Retain`, and persists each bound destination PV.
3. Warm copy reruns rsync with stable session-derived operation IDs and incremental semantics.
4. Pause records one workload boundary for all Pod PVCs.
5. Final sync verifies every source and destination claim has no Pod consumer, then completes all volumes while the workload stays paused.
6. Activate checkpoints temporary PVC deletion, source PVC deletion, destination PV reservation, and recreated active PVC binding.
7. Resume verifies active bindings and workload readiness.

Each activation mutation has UID and resource-version preconditions where the API supports them. The recreated PVC manifest receives a server-side dry-run after the old PVC disappears and before the PV claim reservation changes. A process failure at any checkpoint leaves `Retain` volumes and enough session state for `session resume` or `session rollback`.

Multi-PVC Pod migration reserves and warm-copies every volume, pauses once, final-syncs every volume, activates every volume, and resumes once. A partial activation keeps the workload paused while resume completes the remaining volumes.

## Consistency Boundary

Warm copy provides file-level convergence while the application can still write. Open files, application caches, and write ordering remain application concerns during this phase.

The consistency point begins after the controller adapter confirms the workload is paused and all PVC consumers have disappeared. The final rsync pass runs with delete-extraneous semantics and optional checksum comparison. Activation requires a recorded final-sync completion for every volume.

This model gives crash consistency at the final filesystem state. Database-level guarantees require the workload's own shutdown or KubeBlocks offline operation to flush durable state.

## Storage Reservation And Topology

The planner checks target node readiness, Pod node selectors, required affinity, taints, StorageClass topology, and CSINode registration signals. Reservation creates a real destination PVC before downtime, which consumes namespace quota and tests backend provisioning.

For `WaitForFirstConsumer`, a short-lived consumer Pod uses the target hostname and generated tolerations for target-node `NoSchedule` and `NoExecute` taints. The reserver waits for Pod readiness and a Bound PVC, records the PV UID and selected node, changes the PV to `Retain`, and removes the consumer. Generated tolerations remain limited to helper resources.

Quota evaluation uses Kubernetes `resource.Quantity` and evaluates every ResourceQuota and PVC LimitRange in the staging namespace. The plan estimates temporary storage, retained rollback storage, PVCs, Pods, Jobs, Services, Secrets, and ConfigMaps. Actual destination binding remains the authoritative storage-capacity test.

Helper containers set CPU and memory requests/limits to `0` and ephemeral-storage requests to `0`. The ephemeral-storage limit remains unset so kubelet can account for helper logs and writable-layer usage without immediately evicting a container whose limit is `0`. Object-count quota for Pods, Jobs, Secrets, Services, and ConfigMaps still applies and remains part of the plan estimate.

## Workload Adapters

### Standalone Pod

Discovery serializes the complete Pod. Pause deletes the Pod with a UID precondition. Resume removes server-assigned metadata, preserves its functional specification, adds session ownership, and selects the target node. Rollback selects the recorded source node.

### StatefulSet

For ordinal `k` and original replicas `N`, pause changes replicas to `k`. Pods `k..N-1` form the affected set and must all be Ready during planning. Resume restores `N` and waits for every affected Pod. Replica values outside the recorded pair cause a conflict.

### KubeBlocks

Discovery supports served `apps.kubeblocks.io/v1alpha1` and `operations.kubeblocks.io/v1alpha1` OpsRequest resources. Leader, primary, and master roles require a candidate or explicit downtime acknowledgement. The adapter can issue a switchover request, then sets and restores every Cluster component through `componentSpecs[].stop` so instance PVCs remain available for final sync. Component-wide downtime is part of the migration plan.

## Namespace And RBAC Model

The source namespace owns the application PVC identity. The temporary namespace holds staged PVCs and copy helpers. The destination namespace defaults to the source namespace for activation. The session namespace holds ConfigMaps and defaults to `pvc-migrate-system`. `rename` rebinds a name within one namespace; `move` rebinds the same retained PV across namespaces.

`plan` issues SelfSubjectAccessReviews for source, staging, session, cluster-scoped PV/node/storage resources, Helm helper objects, controller operations, and KubeBlocks OpsRequests. It also verifies Pod-referenced Secrets, ConfigMaps, and ServiceAccounts. Existing NetworkPolicies generate a warning because the real copy Job provides the definitive connectivity result.

## Finalize And Rollback

Completion leaves the destination PV active with `Retain` and the source PV released with `Retain`. Rollback reverses those roles and resumes the workload against the source PV.

Finalize closes the rollback window. It restores the active PV's recorded reclaim policy, removes migration ownership from the active PV and PVC, and permits a later migration. Session deletion requires finalize and deletion of every recorded rollback PV, preserving the recovery record until its resources are resolved.

Abort applies before activation. The source remains active, the staged destination becomes the cleanup target, and finalize releases the source PVC/PV ownership.

## Upstream Boundary

`internal/copyengine.PVMigrate` imports `github.com/utkuozdemir/pv-migrate/pvmigrate` directly. The adapter converts strategies, builds stable operation IDs, supplies scheduling Helm values, disables interactive progress, and converts upstream failures into the `copy` error category.

The module is pinned to upstream commit `22a469151ecaf3e4c529437193380eba23949165` as `v1.8.1-0.20260802124747-22a469151eca`. The pseudo-version resolves the upstream tag and module-path history while retaining a reproducible commit.

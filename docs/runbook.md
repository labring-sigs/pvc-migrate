# Operations Runbook

## Before A Migration

1. Confirm recent application-level backups and a tested restore procedure.
2. Confirm source and destination StorageClasses, available backend capacity, and image registry access. The planner selects a compatible target node automatically; pass `--target-node` to pin one.
3. Confirm the session and temporary namespaces exist or allow the CLI to create them.
4. Run the operation-specific plan, such as `migrate-pod plan -o yaml`, and resolve every error check. Review warnings for active writers, roles, CSINode registration, and NetworkPolicies.
5. Run the write command with its local `--dry-run=true` default to exercise API reads and server-side validation without persistent resources. Add `--dry-run=false` to the approved execution.
6. Record the session ID in the change ticket and use it for every follow-up command.

For a Pod with multiple PVCs, use `migrate-pod`. It creates one workload consistency boundary for the full set.

## Observe Progress

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  -o yaml session status SESSION
```

Read these fields first:

- `status.phase`: current durable phase;
- `status.resumeFrom`: phase that failed;
- `status.message`: latest state transition or failure;
- `status.volumes[].sync`: attempts and final-sync completion;
- `status.volumes[].activation`: cutover checkpoints and active PVC UID;
- `status.history`: ordered phase changes.

Structured logs go to stderr. JSON or YAML command results go to stdout.

## Failure Response

| Session state | Workload state | Operator action |
| --- | --- | --- |
| `Failed`, `resumeFrom` at reserving or warm copy | Running | Correct provisioning, quota, image, or network issue; run `session resume`. |
| `Failed`, `resumeFrom=Pausing` | Inspect adapter state | Preserve application writes as stopped until adapter state is clear; run `session resume` or `session abort`. |
| `Failed`, `resumeFrom=FinalSyncing` | Paused | Correct copy connectivity or capacity; run `session resume`. |
| `Failed`, `resumeFrom=Activating` | Paused | Keep all claims offline; inspect UIDs and PV claimRefs; run `session resume` or `session rollback`. |
| `Failed`, `resumeFrom=Resuming` | Activation completed | Correct controller scheduling or readiness; run `session resume`, with rollback available during the retained window. |
| `Completed` | Running on destination | Validate the application, then finalize or roll back. |

Every retry reloads the session through the CLI command. A ConfigMap conflict indicates another process updated the session; inspect current state before retrying.

## Resume

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --yes session resume SESSION --dry-run=false
```

Resume re-enters the persisted idempotent stage. Warm and final rsync operations are incremental. Stable session and attempt identifiers make Helm resource ownership traceable. High-risk resume phases require approval.

## Abort Before Activation

Use abort when the destination has not become the application identity:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --yes session abort SESSION --dry-run=false
```

Abort resumes a paused workload and records `Aborted`. Staged PVCs and PVs remain retained. Resolve them with:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --yes session cleanup SESSION --dry-run=false \
  --delete-temporary --delete-rollback-pv --finalize --delete-session
```

This sequence deletes the staged PVC, deletes its Released destination PV object, restores the source PV reclaim policy, releases source ownership, and deletes the ConfigMap. A retained PV object preserves backend data according to CSI behavior.

## Rollback After Activation

Rollback pauses a running workload when needed, deletes the active destination-backed PVC identity, reserves the retained source PV for the original identity, recreates the PVC, and resumes the workload:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --timeout 30m \
  --yes session rollback SESSION --dry-run=false
```

For standalone Pods, rollback recreates the Pod on the recorded source node. StatefulSets restore scheduling through their controller. Validate application data and readiness after rollback.

## Close The Rollback Window

Cleanup flags have independent resource effects:

| Flag | Effect |
| --- | --- |
| `--delete-temporary` | Delete a staged PVC whose UID and session ownership still match. |
| `--delete-rollback-pv` | Delete the session-owned Released or Available rollback PV object. |
| `--finalize` | Restore the active PV reclaim policy and release active PV/PVC ownership. |
| `--delete-session` | Delete the ConfigMap after finalize and rollback-PV deletion are requested. |

Recommended successful closeout:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --yes session cleanup SESSION --dry-run=false \
  --delete-rollback-pv --finalize --delete-session
```

For a `Retain` rollback PV, deleting the Kubernetes PV object preserves backend data. Use the CSI driver's documented backend deletion procedure when storage reclamation is intended. A backend deletion policy can be applied to the rollback PV before cleanup after the application validation window.

## Inspect A Stuck Cutover

Use the session's recorded names and UIDs:

```bash
kubectl --kubeconfig /path/to/config -n pvc-migrate-system \
  get configmap pvc-migrate-session-SESSION -o yaml

kubectl --kubeconfig /path/to/config get pv \
  -l pvc-migrate.io/session=SESSION -o wide

kubectl --kubeconfig /path/to/config get pvc,pod -A \
  -l pvc-migrate.io/session=SESSION -o wide
```

During activation, expected PV roles are `source` and `destination`; completed cutover uses `active` and `rollback`. Compare PV `claimRef.uid` with the current PVC UID. A name match alone is insufficient evidence.

Keep PV reclaim policies at `Retain` while investigating. Preserve the session ConfigMap until the active PVC is Bound and the workload recovery decision is complete.

## Quota And Provisioning Failures

`plan` evaluates every ResourceQuota and PVC LimitRange in the staging namespace. Reservation remains the authoritative backend test because CSI capacity, topology, and external provisioner health can change after planning.

For a WFFC destination stuck in Pending:

1. Inspect the reservation consumer Pod and its scheduling events.
2. Inspect PVC events and `volume.kubernetes.io/selected-node`.
3. Confirm the StorageClass provisioner is present on the target node.
4. Confirm tool image pull, taints, quota, and backend capacity.
5. Correct the cluster condition and run `session resume`.

## Copy Failures

The copy engine tries configured strategies in order through upstream pv-migrate. `clusterip` requires policy and network reachability between source and staging namespaces. Node-local RWO volumes generally need source and destination helpers on their respective nodes.

`copy` defaults to an offline pass and requires zero active Pod consumers. Use `copy --online` for one finite warm pass with file-level consistency while consumers keep running. `--destination-storage-class`, `--target-node`, and `--source-node` control the destination class and helper placement; `--target-node` defaults to `auto`, which selects a compatible Ready node and prefers a node different from the source. The source node is inferred from a unique active consumer when possible. Cross-namespace copy defaults the destination PVC name to the source name.

Inspect Helm-owned Jobs, Deployments, Services, and Pods carrying the operation ID from structured logs. The application service increments and persists copy attempts before each call. A later resume performs another incremental pass.

## KubeBlocks

Review the planned role and served KubeBlocks API version. A selected leader, primary, or master needs `--kubeblocks-candidate` or `--allow-leader-downtime`. `--kubeblocks-candidate` lets the adapter submit and wait for the KubeBlocks switchover OpsRequest. The plan also prints a `kbcli cluster promote` command and a server-version-matched `kubectl create` YAML command for a native pre-switch. Components whose admission webhook rejects Switchover require their database-native procedure or an explicit downtime acknowledgement. The adapter sets each selected `componentSpecs[].stop` field, waits for consumers to disappear, and restores the recorded stop values after activation.

Pod `emptyDir` volumes are recreated empty with the Pod and remain outside persistent-data scheduling blockers. Durable migration scope is defined by the PVC volumes recorded in the plan.

Database consistency depends on a successful KubeBlocks offline operation and Ready verification. Keep the session paused while investigating any final-sync or activation failure.

## Object-Storage Backup

The default `backup` command uses an offline source-PVC check. It creates a short-lived read-only helper Job after the source has no Pod consumers and syncs individual files to the object prefix.

For an online warm pass, use `live-backup` or `backup --online` with an explicit mutation flag:

```bash
pvc-migrate --kubeconfig /path/to/config \
  live-backup --dry-run=false \
  --namespace application \
  --source-pvc database-data \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260807
```

The helper mounts the source read-only while application Pods continue to run. An incomplete synchronization can be retried with the same name while its completion manifest is absent. A published recovery point is immutable and requires a new name for a later backup. The result has a best-effort crash-consistent file boundary; database transactions need application quiesce, a filesystem snapshot, or a database-native backup.

The offline consumer check is a point-in-time guard. A controller can recreate a Pod after the check because Kubernetes has no PVC lock understood by workload controllers. Keep the workload owner quiesced for the whole offline backup or restore helper lifetime when file consistency matters.

Restore an S3 prefix into a PVC with the destination kept quiesced until validation completes:

```bash
pvc-migrate --kubeconfig /path/to/config \
  restore --dry-run=false \
  --namespace application \
  --destination-pvc database-restore \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260807
```

S3 object-storage mode copies files individually and uses no archive compression. The resulting recovery point preserves file contents and paths; owner/group/mode, ACL, xattr, hardlink, device-file, and empty-directory metadata require an application-specific export or a future archive format. Restore keeps extra destination files by default; pass `--delete-extraneous` after reviewing the destructive deletion set. The immutable manifest binds the source PVC identity, capacity, VolumeMode, and `--path`, plus an object inventory containing count, total bytes, and a SHA-256 fingerprint over sorted object keys, sizes, and ETags. Restore checks the manifest and inventory before creating the helper and verifies the inventory after synchronization. The dry-run result displays the destination prefix, mode, consistency boundary, and compression policy without exposing credentials.

The manifest schema is version 2. Recovery points created by earlier builds need a fresh backup before restore.

## Real-Cluster E2E

Run the isolated test with credentials that can create namespaces, PV/PVC resources, Pods, Helm helper resources, and SelfSubjectAccessReviews:

```bash
PVC_MIGRATE_E2E=1 \
PVC_MIGRATE_E2E_KUBECONFIG=/path/to/config \
PVC_MIGRATE_E2E_SOURCE_CLASS=openebs-hostpath \
PVC_MIGRATE_E2E_DESTINATION_CLASS=openebs-backup \
go test -tags=e2e -v ./test/e2e -timeout 30m
```

The test namespace begins with `pvc-migrate-e2e-`. Teardown changes only test-owned PVs to `Delete`, removes the namespace, and deletes any remaining PV object selected by the test namespace or session label.

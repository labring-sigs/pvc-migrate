# Operations Runbook

## Before A Migration

1. Confirm recent application-level backups and a tested restore procedure.
2. Confirm source and destination StorageClasses, available backend capacity, and image registry access. The planner selects a compatible target node automatically; pass `--target-node` to pin one.
3. Confirm the session and temporary namespaces exist or allow the CLI to create them.
4. Run the operation-specific plan, such as `migrate-pod plan -o yaml`, and resolve every error check. Review warnings for unknown CSI capacity, active writers, roles, CSINode registration, and NetworkPolicies. Use `--capacity-awareness=require` where every destination driver publishes `CSIStorageCapacity`.
5. Run the write command with its local `--dry-run=true` default to exercise API reads and server-side validation without persistent resources. Add `--dry-run=false` to the approved execution.
6. Record the session ID in the change ticket and use it for every follow-up command.

For a Pod with multiple PVCs, use `migrate-pod`. It creates one workload consistency boundary for the full set.

For an ordinary Deployment, `migrate-pod` pauses the complete Deployment by setting its replica count to zero. All replicas stop for the final sync, PVC switch, and initial recovery. Start from a fully rolled-out Deployment and plan a downtime window for every replica. Delete any HorizontalPodAutoscaler that targets the Deployment before planning. A Deployment controlled by another operator is rejected; use that operator's native maintenance procedure before migration. Resume restores the recorded replica count and waits for all replacement Pods to become Ready.

For a StatefulSet-backed workload, `migrate-pod` scales to the selected Pod ordinal or zero and later restores the recorded replica count. Delete any HorizontalPodAutoscaler that targets a native StatefulSet, VMCluster StatefulSet, or Victoria Logs StatefulSet before planning. If another autoscaling controller creates the HPA, suspend that controller and wait for the HPA to be deleted. The same restriction applies to the Deployment managed by the Grafana adapter. Discovery and execution reject every matching HPA so that Pods cannot reappear during final sync or PVC switching.

OpenEBS LVM shared-mount checks are volume-level checks for every workload adapter. During warm copy, an active source PVC gains a second writer from the tool Pod. The planner reads the actual source PV and LVMVolume. Pass `--openebs-lvm-enable-shared` to temporarily set an unshared source LVMVolume to `shared=yes`; pvc-migrate restores its original value after the probe or copy attempt. Use `--precopy-passes 0` when temporary sharing is not acceptable.

The planner also counts how many Pods in the selected migration unit use each PVC. If multiple Pods will resume against one RWO PVC and the destination is predicted to use OpenEBS LVM, pass `--openebs-lvm-enable-shared` to authorize the destination setting. After provisioning, execution verifies the actual destination PV UID, CSI driver, and matching LVMVolume before it persists `spec.shared=yes`. It repeats this check before workload resume. The destination setting remains because the application requires it. All consumers must run on one node; this setting does not provide RWX storage across nodes. The plan rejects required Pod anti-affinity that matches another consumer and `DoNotSchedule` topology-spread rules whose `maxSkew` and eligible domains cannot permit this co-location.

When creating destination PVCs, use `--destination-capacity` on `reserve`, `copy`, `migrate`, or `migrate-pod`. One value applies to every source PVC; for multiple PVCs use explicit `source-pvc-name=capacity` entries. The planner compares every requested value with its source PV capacity and rejects shrink by default. Use `--allow-volume-shrink` only after checking that the data fits. pvc-migrate reads usage only through a trusted storage-backend CRD adapter and never creates a Pod or mounts a source volume for this check. If the backend does not expose reliable per-volume usage, the operation is blocked unless `--skip-source-usage-check` explicitly accepts the risk. Capacity flags cannot modify an existing session and are not available on `rename` or `move`.

When naming multiple destination PVCs, use `--destination-pvc source-pvc-name=destination-pvc-name` for each claim. The planner rejects unknown, duplicate, or missing source-name mappings.

Use `--source-path` and `--destination-path` to copy directory contents instead of the full PVC. Paths are relative to the PVC root; `.` means the root. Use bare values only with one source PVC. For multiple PVCs, repeat explicit mappings such as `--source-path data=mysql/current --destination-path data=.`. Unmapped PVCs remain full-volume transfers. Paths are immutable after session creation and appear in plan and session output.

Execution validates source directories after mounting the source PVC and creates destination directories after reservation. Missing directories, non-directory objects, parent traversal, and symbolic-link path components stop before rsync. For a migration without warm-copy passes, source validation occurs after the workload is paused; abort the session to restore the workload if validation cannot be corrected. A partial `migrate` or `migrate-pod` still activates a replacement PVC, which contains only the selected source contents at the selected destination path.

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

Generated reservation, rsync, SSHD, and rclone Pod logs follow the active command on stderr by default, so short-lived tool cleanup does not remove the operator's copy of their output. Prefixes identify the Pod and container when several PVCs run concurrently. Redirect stderr to retain a file, for example `2>migration.log`, or use `--stream-tool-logs=false` for quiet automation. `--log-format=json` emits JSON Lines for tool output, progress events, guidance, and failures. JSON or YAML command results stay on stdout.

Interactive text output uses `--color=auto` by default. Use `--color=always` when stderr is rendered by a terminal multiplexer, and use `--color=never` for plain-text collection. Severity, component, and tool prefixes receive separate colors; JSON output stays parseable.

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

This sequence deletes the staged PVC, restores the Released destination PV's recorded reclaim policy before deleting the PV, restores the source PV reclaim policy, releases source ownership, and deletes the ConfigMap. A recorded `Delete` policy lets Kubernetes and the CSI driver delete the destination backend volume. A recorded `Retain` policy preserves it.

## Rollback After Activation

Rollback pauses a running workload when needed, deletes the active destination-backed PVC identity, reserves the retained source PV for the original identity, recreates the PVC, and resumes the workload:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --timeout 30m \
  --yes session rollback SESSION --dry-run=false
```

For standalone Pods, rollback recreates the Pod on the recorded source node. Deployments and StatefulSets restore scheduling through their controllers. Validate application data and readiness after rollback.

## Close The Rollback Window

Cleanup flags have independent resource effects:

| Flag | Effect |
| --- | --- |
| `--delete-temporary` | Delete a staged PVC whose UID and session ownership still match. |
| `--delete-rollback-pv` | Restore each session-owned Released or Available rollback PV's recorded reclaim policy, then delete the PV. |
| `--finalize` | Restore the active PV reclaim policy and release active PV/PVC ownership. |
| `--delete-session` | Delete the ConfigMap after finalize and rollback-PV deletion are requested. |

Recommended successful closeout:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --yes session cleanup SESSION --dry-run=false \
  --delete-rollback-pv --finalize --delete-session
```

Cleanup verifies the recorded rollback reclaim policy before making changes. A recorded `Delete` policy lets the PV controller call the CSI driver and remove the backend volume. A recorded `Retain` policy preserves backend data after the Kubernetes PV object is deleted.

## Recover A Missing Session Record

Session ConfigMaps use `pvc-migrate.io/session-protection`. A direct ConfigMap deletion remains pending until validated session cleanup removes this finalizer.

For ownership left by an already missing ConfigMap, identify the active PVC reported by migration preflight and validate the reconstructed PVC/PV pair:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  session cleanup-orphan SESSION \
  --source-namespace application --source-pvc data-database-1
```

Execute the same validated plan:

```bash
pvc-migrate --kubeconfig /path/to/config \
  --session-namespace pvc-migrate-system \
  --yes session cleanup-orphan SESSION \
  --source-namespace application --source-pvc data-database-1 \
  --dry-run=false
```

The command requires matching session ownership, active PVC/PV claimRef UIDs, reciprocal `paired-pv` metadata, recorded active and rollback reclaim policies, and a Released or Available rollback PV. The rollback PV policy must still be `Retain` or already equal its recorded original value from a prior cleanup attempt. The command restores that policy before deleting the rollback PV, restores the active reclaim policy, clears session metadata, and removes the orphan Lease. A recorded `Delete` policy triggers normal Kubernetes and CSI backend deletion.

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

`plan` evaluates ResourceQuota and LimitRange policies in every namespace where the workflow creates resources, including separate source, staging, destination, and session namespaces. It checks the maximum concurrent Pod count for unscoped quota and separate probe and transfer peaks for `Terminating` and `NotTerminating` quota. ResourceQuota limits existing and requested usage; it does not add a missing container resource value. pvc-migrate leaves the tool `ephemeral-storage` limit unset. If a container LimitRange supplies `default.ephemeral-storage`, Kubernetes adds that limit and the planner includes it in `limits.ephemeral-storage` quota demand. A `defaultRequest` does not create a limit, and the tool's explicit zero request prevents it from changing the request. Reservation and actual PVC binding remain the authoritative backend tests because CSI capacity, topology, quota usage, and external provisioner health can change after planning.

For a WFFC destination stuck in Pending:

1. Inspect the reservation consumer Pod and its scheduling events.
2. Inspect PVC events and `volume.kubernetes.io/selected-node`.
3. Confirm the StorageClass provisioner is present on the target node.
4. Confirm tool image pull, taints, quota, and backend capacity.
5. Correct the cluster condition and run `session resume`.

## Copy Failures

The copy engine tries configured strategies in order through upstream pv-migrate. `clusterip` requires policy and network reachability between source and staging namespaces. Node-local RWO volumes generally need source and destination tools on their respective nodes.

`copy` defaults to an offline pass and requires zero active Pod consumers. Use `copy --online` for one finite warm pass with file-level consistency while consumers keep running. `--destination-storage-class`, `--target-node`, and `--source-node` control the destination class and tool placement; `--target-node` defaults to `auto`, which selects a compatible Ready node and prefers a node different from the source. The source node is inferred from a unique active consumer when possible. Cross-namespace copy defaults the destination PVC name to the source name.

The destination PVC request is recorded separately from the source PV capacity. For orchestrated migration, activation recreates the application PVC with the requested destination capacity; rollback restores the original source PVC request.

The CLI follows matching Helm-owned tool Pod logs through each attempt and prints the operation ID in progress records. Inspect Jobs, Deployments, Services, and Pods carrying that ID when cluster events require more context. The application service increments and persists copy attempts before each call. A later resume performs another incremental pass.

## KubeBlocks

Review the planned role and served KubeBlocks API version. A selected leader, primary, or master needs `--kubeblocks-candidate` or `--allow-leader-downtime`. `--kubeblocks-candidate` lets the adapter submit and wait for the KubeBlocks switchover OpsRequest. The plan also prints a `kbcli cluster promote` command and a server-version-matched `kubectl create` YAML command for a native pre-switch. Components whose admission webhook rejects Switchover require their database-native procedure or an explicit downtime acknowledgement. InstanceSet-backed components use `spec.paused=true` on the selected InstanceSet, delete only the selected Pod with a UID precondition, and restore the original pause representation after activation. Legacy StatefulSet-backed components set `componentSpecs[].stop=true` only for the selected Cluster component, wait for consumers to disappear, and restore its recorded stop value. The `kubeblocks.io/reconcile` annotation triggers reconciliation and does not pause it.

Pod `emptyDir` volumes are recreated empty with the Pod and remain outside persistent-data scheduling blockers. Durable migration scope is defined by the PVC volumes recorded in the plan.

Database consistency depends on a successful KubeBlocks offline operation and Ready verification. Keep the session paused while investigating any final-sync or activation failure.

## Object-Storage Backup

The default `backup` command uses an offline source-PVC check. It creates a short-lived read-only tool Job after the source has no Pod consumers and syncs individual files to the object prefix.

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

The tool mounts the source read-only while application Pods continue to run. An incomplete synchronization can be retried with the same name while its completion manifest is absent. A published recovery point is immutable and requires a new name for a later backup. The result has a best-effort crash-consistent file boundary; database transactions need application quiesce, a filesystem snapshot, or a database-native backup.

The offline consumer check is a point-in-time guard. A controller can recreate a Pod after the check because Kubernetes has no PVC lock understood by workload controllers. Keep the workload owner quiesced for the whole offline backup or restore tool lifetime when file consistency matters.

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

Restore requires the destination PVC to exist by default. To create a Filesystem PVC from the
published backup manifest, pass `--create-pvc` with its StorageClass and access mode:

```bash
pvc-migrate --kubeconfig /path/to/config \
  restore --dry-run=false \
  --namespace application \
  --destination-pvc restored-data \
  --create-pvc \
  --destination-storage-class openebs-lvm \
  --destination-access-mode ReadWriteOnce \
  --destination-capacity 20Gi \
  --target-node worker-1 \
  --backend s3 \
  --bucket pvc-backups \
  --endpoint https://s3.example.com \
  --name database-20260807
```

Omit `--destination-capacity` to use the capacity recorded in the manifest. An explicit capacity
can be larger and cannot be smaller. Supported access modes are `ReadWriteOnce`,
`ReadWriteOncePod`, and `ReadWriteMany`. `--target-node` pins the binding probe and restore tool.
Use it for local volumes, OpenEBS LVM, `WaitForFirstConsumer` StorageClasses, and restricted
`allowedTopologies`.

An automatically created PVC remains after a probe, binding, preflight, or restore failure. Use the
same values for the bucket, prefix, name, PVC settings, and target node when you retry. The restore
annotations identify the recovery point that owns the PVC. Restore reports a conflict for a
same-named PVC without matching annotations. Restore does not adopt or overwrite the PVC.

S3 object-storage mode copies files individually and uses no archive compression. The resulting recovery point preserves file contents and paths; owner/group/mode, ACL, xattr, hardlink, device-file, and empty-directory metadata require an application-specific export or a future archive format. Restore keeps extra destination files by default; pass `--delete-extraneous` after reviewing the destructive deletion set. The immutable manifest binds the source PVC identity, capacity, VolumeMode, and `--path`, plus an object inventory containing count, total bytes, and a SHA-256 fingerprint over sorted object keys, sizes, and ETags. Restore checks the manifest and inventory before creating the tool and verifies the inventory after synchronization. The dry-run result displays the destination prefix, mode, consistency boundary, and compression policy without exposing credentials.

For a partial object-storage recovery point, pass the same relative `--path` to backup and restore. Omitting it selects the PVC root. The plan and result output show the effective path.

Keep the destination application stopped or otherwise quiesced for the complete restore. The
`--allow-mounted` override can cause application writes during the restore. These writes can make
the result inconsistent.

The manifest schema is version 2. Recovery points created by earlier builds need a fresh backup before restore.

## Real-Cluster E2E

Run the isolated test with credentials that can create namespaces, PV/PVC resources, Pods, Helm tool resources, and SelfSubjectAccessReviews:

```bash
PVC_MIGRATE_E2E=1 \
PVC_MIGRATE_E2E_KUBECONFIG=/path/to/config \
PVC_MIGRATE_E2E_SOURCE_CLASS=openebs-hostpath \
PVC_MIGRATE_E2E_DESTINATION_CLASS=openebs-backup \
go test -tags=e2e -v ./test/e2e -timeout 30m
```

The test namespace begins with `pvc-migrate-e2e-`. Teardown changes only test-owned PVs to `Delete`, removes the namespace, and deletes any remaining PV object selected by the test namespace or session label.

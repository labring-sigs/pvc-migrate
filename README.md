# pvc-migrate

`pvc-migrate` is a sealos/Kubernetes tool for moving filesystem PVC data and PVC identities between storage classes, namespaces, and clusters. It has two execution backends: CLI session commands run workflows in-process with resumable records, and controller CRDs (driven through `cr` commands) are reconciled by a deployed controller. It covers offline PVC migration, warm-copy Pod migration with cutover, copy and reservation, PVC rename/move, backup/restore through S3-compatible object storage, and cross-cluster copy.

## Key features

- Warm-copy Pod migration: configurable warm-copy passes while the workload stays available, followed by workload pause, final sync, cutover, and resume — one idempotent `migrate-pod` workflow.
- Safety-first execution: every mutating command defaults to `--dry-run=true`, execution requires `--dry-run=false`, and workload pause plus storage identity changes also require `--yes` or interactive approval.
- Storage-class and capacity-aware replatforming: destination PVCs can change StorageClass and capacity, with shrink guarded by explicit `--allow-volume-shrink`.
- Same-namespace work (`migrate`, `copy`, `reserve` with a single `-n`) and cross-namespace work (`cluster-migrate`, `cluster-copy`, `cluster-reserve` with role flags) are separate command families.
- Two execution modes: session commands execute in-process; `cr` commands submit workflow CRs that the controller reconciles. The two backends never share records.
- Cross-cluster copy and reservation (`cluster-copy cross`, `cluster-reserve cross`) with separate source and destination kubeconfig connections.
- Backup and restore of PVC files to S3-compatible object storage; controller mode reads its configuration from a namespaced `BackupRepository`.
- Rename or move offline PVC identities while retaining their PV; the source PV stays retained for a rollback window after cutover.

## Supported workloads

`migrate-pod` coordinates one workload and pauses/resumes it safely around the final sync and cutover. The workload adapters are:

| Workload | Pause/cutover behavior |
| --- | --- |
| Standalone Pod | Delete and recreate the recorded Pod on the target node |
| Ordinary Deployment | Scale the Deployment to zero, switch PVCs, restore replicas |
| Native StatefulSet | Scale from `N` to the selected ordinal, then restore `N` |
| KubeBlocks InstanceSet | Optional primary switchover, pause reconciliation, delete the selected Pod |
| KubeBlocks legacy StatefulSet | Stop/Start OpsRequest on the Cluster or component |
| VMCluster component | Pause the component and reduce replicas to the selected ordinal |
| Grafana | Pause the Grafana deployment and scale it to zero |
| Victoria Logs `vlstorage` | Scale the `vlstorage` StatefulSet to zero under a session pause lock |

MinIO tenants, CockroachDB, and backup archive-WAL workloads are rejected during planning; they require their own application-native procedures. Controller ownership outside the adapters above also fails the plan.

Plain offline operations (`migrate`, `copy`, `rename`, `move`) never touch a workload and require zero active PVC consumers; `copy --online` allows one finite warm-copy pass with consumers running.

## Installation

The Helm chart is the only supported installation path, and the workflow CRDs ship with the chart. Required namespaces (session, temporary, destination) must already exist; nothing is created automatically.

```bash
CHART_VERSION=X.Y.Z
kubectl get namespace pvc-migrate-system
helm upgrade --install pvc-migrate \
  oci://ghcr.io/labring-sigs/pvc-migrate/charts/pvc-migrate \
  --version "$CHART_VERSION" --namespace pvc-migrate-system \
  --rollback-on-failure --wait --timeout 10m --history-max 10
helm test pvc-migrate --namespace pvc-migrate-system --logs --timeout 10m
```

Session-mode CLI runs use the kubeconfig identity and require equivalent permissions. `cr` commands require the installed controller and fail clearly when a workflow CRD is absent.

## Command families

| Command | Mode | Purpose |
| --- | --- | --- |
| `migrate` | session | Offline PVC migration in one namespace (`-n`) |
| `cluster-migrate` | session | Cross-namespace offline migration (role flags) |
| `copy` / `cluster-copy` | session | Resumable copy without cutover, single- or cross-namespace |
| `cluster-copy cross` | session | Copy PVC data between two clusters |
| `reserve` / `cluster-reserve` | session | Pre-provision and retain destination PVCs |
| `cluster-reserve cross` | session | Provision destination PVCs in another cluster |
| `migrate-pod` | session | Warm copy, pause, cutover, resume for one Pod (`-n`) |
| `rename` | session | Rename one offline PVC, retaining its PV |
| `move` | session | Move one offline PVC identity across namespaces (cluster-scoped kind) |
| `backup` / `restore` | session | PVC backup/restore via S3-compatible object storage |
| `recovery cleanup-orphan` | session | Clear stale session ownership after a session record was lost |
| `cr <family>` | controller | Create and drive workflow CRs |

`cr` families mirror the workflow kinds: `cr migrate-pod`, `cr migrate`, `cr cluster-migrate`, `cr copy`, `cr cluster-copy`, `cr reserve`, `cr cluster-reserve`, `cr rename`, `cr move`, `cr backup`, `cr restore`. Each offers `create`, `status`, `watch`, and the lifecycle verbs (`resume`, `abort`, `rollback` where the kind supports it, `cleanup`).

Session families expose the lifecycle verbs each workflow supports (`status`, `resume`, `abort`, `cleanup`, plus `rollback` on migration, Pod migration, rename, and move, and `plan` on the migrate and cross families). `controller` runs the reconciliation loop standalone; `version` and `completion` are utility commands.

## Execution and scoping model

- Session commands execute in the invoking process and persist records as ConfigMaps in `--session-namespace` (default `pvc-migrate-system`).
- `cr` commands submit workflow CRs the controller reconciles; the two backends never read or write each other's records.
- Namespaced commands take one `-n/--namespace` — the tenant namespace of the workflow and every PVC it addresses.
- Cluster families take role flags: `-n/--source-namespace`, `--destination-namespace`, plus `--temporary-namespace` on `cluster-migrate`/`cr cluster-migrate`.
- The command boundary decides scope: same-namespace work uses the namespaced command and cannot express a cross-namespace spec, and vice versa.
- Every command's `--help` documents its exact flags; it is the reference — this README does not repeat them.

## Quick start

Offline migration in one namespace (every mutating command defaults to dry-run):

```bash
pvc-migrate --yes migrate --dry-run=false -n application \
  --source-pvc database-data --destination-pvc database-data \
  --destination-storage-class fast-local
```

Real-time Pod migration with one warm-copy pass:

```bash
pvc-migrate --yes migrate-pod \
  --session database-20260809 -n application \
  --pod database-1 \
  --destination-storage-class fast-local \
  --precopy-passes 1 --verify-checksum \
  --dry-run=false
```

Controller workflow: create, observe, then finalize cleanup:

```bash
pvc-migrate --yes cr migrate create -n application \
  --source-pvc data --destination-pvc data
pvc-migrate cr migrate watch data-migration -n application
pvc-migrate --yes cr migrate cleanup data-migration -n application --dry-run=false
```

Cross-cluster copy with two explicit connections:

```bash
pvc-migrate cluster-copy cross --dry-run=false \
  --source-kubeconfig ~/.kube/source \
  --destination-kubeconfig ~/.kube/destination \
  --source-namespace application --source-pvc database-data \
  --destination-namespace archive --destination-pvc database-data \
  --destination-storage-class fast
```

## Safety

- Dry-run is the default everywhere: execution requires an explicit `--dry-run=false`, and workload pause and storage identity changes additionally require `--yes` or interactive approval.
- Ownership fences: renewable Kubernetes Leases give one session exclusive ownership, and PVC/PV mutations verify recorded UIDs, bindings, and session ownership, preventing two workflows from touching the same PVC.
- The source PV is retained after cutover for a rollback window; `<workflow> rollback` restores the original identities, and `<workflow> cleanup --finalize` applies the recorded reclaim policies and releases ownership.
- Session commands resume from their persisted phase after interruption, and behavior against real clusters is covered by the unit test suite (`make test`).

## Development

```bash
make test        # run the Go test suite
make manifests   # regenerate deepcopy code and CRDs, sync them into the chart
make chart-lint  # helm lint plus chart deployment-contract tests
```

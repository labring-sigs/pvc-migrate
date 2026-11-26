# pvc-migrate

`pvc-migrate` migrates, copies, and backs up filesystem PVCs on Kubernetes and Sealos. Run a resumable session from the CLI or submit a workflow to the Kubernetes controller.

## Supported workloads

`migrate-pod` copies data while the workload is running, then pauses it for the final sync and cutover.

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

Install the controller and workflow CRDs with Helm. Create the session, temporary, and destination namespaces your workflow will use before running it.

```bash
CHART_VERSION=X.Y.Z
kubectl get namespace pvc-migrate-system
helm upgrade --install pvc-migrate \
  oci://ghcr.io/labring-sigs/pvc-migrate/charts/pvc-migrate \
  --version "$CHART_VERSION" --namespace pvc-migrate-system \
  --rollback-on-failure --wait --timeout 10m --history-max 10
helm test pvc-migrate --namespace pvc-migrate-system --logs --timeout 10m
```

## Command families

| Command | Purpose |
| --- | --- |
| `migrate` | Offline PVC migration in one namespace (`-n`) |
| `cluster-migrate` | Cross-namespace offline migration (role flags) |
| `copy` / `cluster-copy` | Resumable copy without cutover, single- or cross-namespace |
| `cluster-copy cross` | Copy PVC data between two clusters |
| `reserve` / `cluster-reserve` | Pre-provision and retain destination PVCs |
| `cluster-reserve cross` | Provision destination PVCs in another cluster |
| `migrate-pod` | Warm copy, pause, cutover, resume for one Pod (`-n`) |
| `rename` | Rename one offline PVC, retaining its PV |
| `move` | Move one offline PVC identity across namespaces (cluster-scoped kind) |
| `backup` / `restore` | PVC backup/restore via S3-compatible object storage |
| `recovery cleanup-orphan` | Clear stale session ownership after a session record was lost |
| `cr <family>` | Create and drive workflow CRs |

Use `pvc-migrate <command> --help` for the lifecycle actions a workflow supports.

## Execution and scope

- CLI sessions run with your kubeconfig identity and save checkpoints in ConfigMaps in `--session-namespace` (default `pvc-migrate-system`). Use the same session to resume an interrupted run.
- `cr <family> create` submits a workflow CR to the controller. Session and CR records are separate. Controller transfers use the controller's service account. For copy, backup, and restore, `cr <family> resume` runs the transfer locally and requires runtime permissions in the workflow namespace.
- Namespaced commands take `-n/--namespace`. Cross-namespace commands take `--source-namespace` and `--destination-namespace`. `cluster-migrate` also accepts `--temporary-namespace`.
- Backups preserve permission modes, numeric ownership, symlinks, directory modification times, and empty directories. Controller-mode backup and restore use a namespaced `BackupRepository`. Leave `HELM_DRIVER` unset or set it to `secret` so the rclone credentials are stored in Helm release Secrets.

## Quick start

Migrate an offline PVC to another StorageClass. Use a distinct temporary PVC name for the destination.

```bash
pvc-migrate --yes migrate --dry-run=false -n application \
  --source-pvc database-data --destination-pvc database-data-fast \
  --destination-storage-class fast-local
```

Migrate a Pod with one warm-copy pass.

```bash
pvc-migrate --yes migrate-pod \
  --session database-20260809 -n application \
  --pod database-1 \
  --destination-storage-class fast-local \
  --precopy-passes 1 --verify-checksum \
  --dry-run=false
```

Submit a migration to the controller and watch it by name.

```bash
pvc-migrate --yes cr migrate create -n application --session data-migration \
  --source-pvc data --destination-pvc data-fast --destination-storage-class fast-local \
  --dry-run=false --wait=false
pvc-migrate cr migrate watch data-migration -n application
```

Copy a PVC between clusters.

```bash
pvc-migrate cluster-copy cross --dry-run=false \
  --source-kubeconfig ~/.kube/source \
  --destination-kubeconfig ~/.kube/destination \
  --source-namespace application --source-pvc database-data \
  --destination-namespace archive --destination-pvc database-data \
  --destination-storage-class fast
```

## Safety

- Mutating commands default to dry-run. Pass `--dry-run=false` to execute. Pausing a workload or changing storage identity also requires `--yes` or interactive approval.
- To choose when `migrate-pod` pauses the workload, set `--manual-cutover` (or `spec.manualCutover` on the CR). After warm copy, use `resume` or clear the CR field to approve the pause.
- A smaller destination PVC requires `--allow-volume-shrink`. Kubernetes Leases and PVC/PV identity checks protect storage from concurrent operations.
- After migration cutover, the source PV is retained for rollback. Use `rollback` to restore the original identities or `cleanup --finalize` to close the rollback window and apply the recorded reclaim policies.

## Development

```bash
make test        # run the Go test suite
make manifests   # regenerate deepcopy code and CRDs, sync them into the chart
make chart-lint  # helm lint plus chart deployment-contract tests
```

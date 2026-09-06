# pvc-migrate Helm Chart

Initialized with `helm create charts/pvc-migrate` and adapted to the project's
controller and permission contracts. Requires Kubernetes 1.25+ and Helm
3.17+ or 4. Use one controller release per cluster: all replicas watch all
workflow namespaces, and leader election uses a fixed Lease name in the
release namespace. Installing another controller in a different namespace
does not provide independent tenancy.

## Install

The release namespace and every workflow namespace must already exist.
There is no Namespace template, namespace hook, or namespace create permission.
The installer needs privileges to install CRDs and controller RBAC.

```bash
kubectl get namespace pvc-migrate-system
helm lint ./charts/pvc-migrate --strict
helm template pvc-migrate ./charts/pvc-migrate \
  --namespace pvc-migrate-system --include-crds
helm upgrade --install pvc-migrate ./charts/pvc-migrate \
  --namespace pvc-migrate-system \
  --rollback-on-failure --wait --timeout 10m --history-max 10
helm test pvc-migrate --namespace pvc-migrate-system --logs
```

For Helm 3, replace `--rollback-on-failure` with `--atomic`. Do not pass
`--create-namespace`. A missing namespace fails installation. The readiness
test has no API token and deletes its Pod after success; failed test Pods
remain for diagnostics and are replaced by the next `helm test`.

## Configuration

Keep environment settings in a reviewed values file and pass it with `-f` on
every upgrade. Unknown values and unsafe common mistakes fail schema validation.

| Value | Default | Purpose |
| --- | --- | --- |
| `replicaCount` | `2` | One leader plus a warm standby; only the leader executes workflows |
| `image.repository` | `ghcr.io/labring-sigs/pvc-migrate` | Controller image repository |
| `image.tag` | Chart appVersion | Released or commit-tagged controller image |
| `image.digest` | empty | Optional controller digest, taking precedence over tag |
| `toolImage.repository/tag` | Controller repository/tag | Trusted tagged image for transfer Pods; digest references are not supported by the transfer integration |
| `imagePullSecrets` | `[]` | Existing pull secrets in the release namespace |
| `serviceAccount.create/name` | `true` / generated | Dedicated operator identity; an explicit name is required for an external account |
| `rbac.create` | `true` | Install the existing controller permission contract |
| `rbac.kubeBlocksMongoDBNamespaces` | `[]` | Namespace-local Pod exec Roles and bindings for MongoDB native switchover |
| `controller.logLevel/logFormat` | `info` / `json` | Structured controller logs |
| `controller.operationTimeout` | `30m` | Application operation timeout; independent of Helm's rollout timeout |
| `resources` | 100m CPU/128Mi requests; 512Mi memory limit | Baseline for controller memory/cache use; tune to workflow count and object volume |
| `podDisruptionBudget` | enabled, minAvailable 1 | Applied with multiple replicas; omitted with one replica |
| `affinity` | preferred Pod anti-affinity | Prefer separate nodes without blocking installation on a single eligible node |
| `nodeSelector/tolerations/topologySpreadConstraints` | empty | Placement overrides; existing node taints are never modified |
| `priorityClassName` | empty | Optional existing PriorityClass |

The controller runs as UID/GID 10000 with a read-only root filesystem,
RuntimeDefault seccomp, no added capabilities, and no privilege escalation.
Its Pod explicitly mounts an API token; the ServiceAccount disables implicit
token mounting for other Pods. No temporary writable volume is required.
CPU has a request but no default limit to avoid throttling reconciliation.
The health Service is ClusterIP only; the application currently disables
metrics, so the chart does not advertise a metrics endpoint or ServiceMonitor.

Two replicas improve controller availability, not migration throughput or
instant recovery: workflow Lease expiration and controller queueing still
bound takeover time. `maxUnavailable=0` protects rolling updates, while the
PDB covers voluntary eviction. Review cluster-specific NetworkPolicies for
API server, DNS, tool traffic and readiness probes before applying them;
the chart does not impose a generic policy that would break data transfer.

Chart pull secrets only configure controller and test Pods. Tool Pods run in
workflow namespaces and inherit the application's ServiceAccount-based pull
secret handling. Provision the required secrets in those namespaces first.
Never bind the controller ClusterRole to tenants. It manages storage and
reads Secret-backed Helm release history; tenant permissions should be
bound separately within approved namespaces.

## Existing Installation

Pause new submissions and wait for active workflows to finish, or abort and
clean them up. Back up the existing Deployment, ServiceAccount, ClusterRole,
ClusterRoleBinding, and workflow/CRD state using your normal cluster backup.
Confirm those objects are not owned by another release.

The default `pvc-migrate` release preserves the existing Deployment name and
immutable selector, ServiceAccount name, ClusterRole, and binding. Review the
rendered difference, then adopt only the resources this chart defines:

With Helm 3, client-side apply is the default:

```bash
helm upgrade --install pvc-migrate ./charts/pvc-migrate \
  --namespace pvc-migrate-system --take-ownership --skip-crds \
  --dry-run=server --hide-secret
helm upgrade --install pvc-migrate ./charts/pvc-migrate \
  --namespace pvc-migrate-system --take-ownership --skip-crds \
  --wait --timeout 10m --history-max 10
helm test pvc-migrate --namespace pvc-migrate-system --logs
```

With Helm 4, add `--server-side=false` to both adoption commands. This avoids
field-manager conflicts with the existing kubectl-managed Deployment and
ClusterRole while Helm records ownership. After the first successful adoption,
ordinary upgrades may use Helm 4's default server-side apply.

Initial adoption intentionally omits automatic uninstall on failure: deleting
an unsuccessful first release can delete adopted controller resources. If
rollout fails, inspect the Pods and correct values with `helm upgrade`; there
is no earlier Helm revision to roll back to. Once adoption succeeds, omit
`--take-ownership` on subsequent upgrades and enable rollback-on-failure.
Never use `--force-replace` to bypass an immutable selector mismatch.

## Upgrade And Rollback

Helm installs missing files in `crds/` only at installation. It does not
upgrade or delete them. For a new chart version, review schema compatibility
and back up workflow CRs, then apply that version's CRDs before the controller:

```bash
kubectl diff --server-side --field-manager=pvc-migrate-crds \
  -f charts/pvc-migrate/crds/
kubectl apply --server-side --field-manager=pvc-migrate-crds \
  -f charts/pvc-migrate/crds/
helm upgrade pvc-migrate ./charts/pvc-migrate \
  --namespace pvc-migrate-system -f production-values.yaml \
  --rollback-on-failure --wait --timeout 10m --history-max 10
helm test pvc-migrate --namespace pvc-migrate-system --logs
helm history pvc-migrate --namespace pvc-migrate-system
```

Do not force field ownership conflicts without reviewing the current CRD
manager. CRDs remain outside the Helm release so an uninstall cannot remove
workflow state. `make manifests` regenerates and synchronizes them; tests
compare all packaged schemas with `config/crd/bases`.

```bash
helm rollback pvc-migrate REVISION --namespace pvc-migrate-system \
  --wait --timeout 10m
```

Rollback restores controller resources and values; it does not revert CRD
schemas, workflow status, PVC/PV changes or copied data. Verify the previous
controller supports stored workflow schemas before rolling back. Use the
application's rollback/abort commands for migration lifecycle recovery.

## Uninstall

Stop new submissions. Complete or abort workflows, then use their cleanup
commands to release finalizers, temporary storage and Leases before removing
the controller:

```bash
helm uninstall pvc-migrate --namespace pvc-migrate-system --wait --timeout 10m
```

CRDs, workflow CRs, existing namespaces and application storage are retained.
Uninstall does not run migration cleanup; unfinished workflows can retain
finalizers until a compatible controller is reinstalled. Do not delete CRDs
as a substitute for cleanup. The controller leader-election Lease can remain;
remove only that Lease after confirming all controller replicas are stopped.

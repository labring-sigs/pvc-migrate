# Kubernetes Permissions

The controller role in `deploy/rbac.yaml` and `config/rbac/role.yaml` covers
execution, recovery, cleanup, and failure diagnostics. Bind it only to the
controller or an operator identity. Workload CRs and their status subresources
are separate authorization boundaries.

## Workflow Submission

Creating a controller workflow submits its spec. The controller initializes
status, creates execution namespaces, and acquires session Leases. The submitting
CLI does not create Leases, write status, or reserve PVCs. Kubernetes makes each
CR create atomic; the controller checks cross-kind name collisions before
execution and uses the session Lease to fence execution.

The CLI's controller-mode RBAC preflight checks `create`, `get`, and `watch` on
the selected namespaced or cluster workflow. Planning still needs read access
to the source workload, volumes, storage topology, namespace policies, and
referenced Pod dependencies. A tenant with only the bundled workflow role can
submit a declarative CR directly; interactive planning also needs these inventory
reads. Explicit lifecycle operations such as cleanup and rollback remain
operator actions and need their corresponding execution permissions.

## Execution Permissions

- Temporary Pods use a managed, token-free ServiceAccount. Its `get` and `update`
  permissions are restricted to `pvc-migrate-transfer`; Kubernetes cannot restrict
  `create` by resource name.
- Reservation does not execute the Helm data mover, so its preflight does not
  demand transfer Secret, Service, ServiceAccount, Job, or Deployment access.
- Reservation and copy still update source PVC ownership and PV retention
  metadata. Removing those writes would break fencing and recovery.
- The data mover uses Helm 4 server-side apply for chart resources, requiring
  `patch`. Native workload adapters use optimistic-concurrency `update` calls;
  they do not require `patch` for KubeBlocks, VMCluster, or Grafana resources.
- `list` on Jobs and Deployments is needed by upstream failure diagnostics.
  Losing these reads hides evidence when scheduling or transfers fail.
- Pod logs and port forwarding are checked as Kubernetes subresources.
  Port forwarding is needed only when the selected transfer strategies include
  `local`. MongoDB native switchover uses the separate optional `pods/exec` role.
- Session cleanup deletes its Lease; planning includes this permission.
- Access reviews are deduplicated and preserve `resourceNames` restrictions.

## Helm Release Storage

The default Helm driver persists release state in Secrets and lists release
history by label. Transfer charts also contain SSH credential Secrets; backup
repositories require named credential reads. These uses are distinct from
workflow state, which is persisted in CRDs for controller mode and ConfigMaps
for session mode.

The upstream data mover independently initializes Helm action configurations
for installation and uninstallation. Switching it to the memory driver loses
the installed release metadata needed for cleanup. Retaining durable release
storage preserves retry and cleanup behavior. Session-mode preflight also
recognizes the upstream ConfigMap release driver when explicitly configured;
the bundled controller role targets the default Secret driver.

package planner

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func (p *Planner) checkReservationPermissions(
	ctx context.Context,
	result checkRecorder,
	name, source, destination, session string,
) {
	checks := reservationResourceAccess(source, destination, session)
	resource, namespace := scopedWorkflowResource("reservations", session, source, destination)
	p.checkWorkflowPermissions(
		ctx,
		result,
		name,
		[]string{session, destination},
		namespace,
		resource,
		checks,
	)
}

func (p *Planner) checkCopyPermissions(
	ctx context.Context,
	result checkRecorder,
	name, source, destination, session string,
	strategies []string,
	inspectShared bool,
) {
	checks := reservationResourceAccess(source, destination, session)

	checks = append(checks, transferToolAccess([]string{source, destination}, strategies)...)
	if inspectShared {
		checks.add("", "local.openebs.io", "lvmvolumes", "list")
	}

	p.checkNetworkPolicyLifecycle(ctx, result, []string{source, destination})

	resource, namespace := scopedWorkflowResource("copies", session, source, destination)
	p.checkWorkflowPermissions(
		ctx,
		result,
		name,
		[]string{session, destination},
		namespace,
		resource,
		checks,
	)
}

func (p *Planner) checkMigrationPermissions(
	ctx context.Context,
	result checkRecorder,
	name, source, destination, staging, session string,
	strategies []string,
) {
	checks := reservationResourceAccess(source, staging, session)
	checks = append(checks, transferToolAccess([]string{source, staging}, strategies)...)
	checks = append(checks, activationResourceAccess(source, destination)...)
	p.checkNetworkPolicyLifecycle(ctx, result, []string{source, staging})
	resource, namespace := scopedWorkflowResource(
		"migrations",
		session,
		source,
		destination,
		staging,
		source,
	)
	p.checkWorkflowPermissions(
		ctx,
		result,
		name,
		[]string{session, source, destination, staging},
		namespace,
		resource,
		checks,
	)
}

func (p *Planner) checkPodMigrationPermissions(
	ctx context.Context,
	result checkRecorder,
	name, source, staging, session string,
	strategies []string,
	workload v1alpha1.WorkloadSpec,
	inspectShared, patchShared bool,
) {
	checks := reservationResourceAccess(source, staging, session)
	checks = append(checks, transferToolAccess([]string{source, staging}, strategies)...)
	checks = append(checks, activationResourceAccess(source, source)...)
	p.checkNetworkPolicyLifecycle(ctx, result, []string{source, staging})
	checks.add(source, "", "pods", "update")

	checks = append(checks, workloadRBAC(source, workload)...)
	if inspectShared {
		checks.add("", "local.openebs.io", "lvmvolumes", "list")
	}

	if patchShared {
		checks.add("", "local.openebs.io", "lvmvolumes", "patch")
	}

	resource, namespace := scopedWorkflowResource("podmigrations", session, source, staging)
	p.checkWorkflowPermissions(
		ctx,
		result,
		name,
		[]string{session, source, staging},
		namespace,
		resource,
		checks,
	)
}

func scopedWorkflowResource(
	resource, sessionNamespace string,
	namespaces ...string,
) (string, string) {
	for _, namespace := range namespaces {
		if namespace != "" && namespace != sessionNamespace {
			return "cluster" + resource, ""
		}
	}

	return resource, sessionNamespace
}

func (p *Planner) checkWorkflowPermissions(
	ctx context.Context,
	result checkRecorder,
	name string,
	namespaces []string,
	sessionNamespace, resource string,
	checks rbacChecks,
) {
	if p.controllerSubmission {
		p.checkSubmissionNamespaces(ctx, result, namespaces)

		if !p.planningWorkflow {
			p.checkWorkflowSubmissionAccess(ctx, result, name, sessionNamespace, resource)
		}

		return
	}

	// Log the namespaces actually reviewed (checkNamespaceAccess reviews the
	// deduplicated set): a same-namespace workflow passes the same name in
	// several roles, and the raw slice would read as duplicate work.
	p.logInfo(
		"checking workflow RBAC permissions",
		"resource",
		resource,
		"namespaces",
		uniqueSorted(namespaces),
	)
	checks = append(p.checkNamespaceAccess(ctx, result, namespaces), checks...)
	p.checkAccessReviews(ctx, result, checks)
}

package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type rbacAccess struct {
	namespace string
	verb      string
	group     string
	resource  string
}

func (p *Planner) checkRBAC(
	ctx context.Context,
	plan *domain.MigrationPlan,
	sourceNamespace, stagingNamespace, sessionNamespace string,
	workload domain.WorkloadSpec,
	inspectOpenEBSLVMShared, enableOpenEBSLVMShared bool,
) {
	p.logInfo(
		"checking migration RBAC permissions",
		"sourceNamespace",
		sourceNamespace,
		"stagingNamespace",
		stagingNamespace,
		"sessionNamespace",
		sessionNamespace,
	)

	checks := make([]rbacAccess, 0)

	add := func(namespace, group, resource string, verbs ...string) {
		for _, verb := range verbs {
			checks = append(
				checks,
				rbacAccess{namespace: namespace, verb: verb, group: group, resource: resource},
			)
		}
	}
	for _, namespace := range uniqueSorted([]string{sourceNamespace, stagingNamespace}) {
		add(namespace, "", "pods", "get", "list", "watch", "create", "delete")
		add(namespace, "", "pods/log", "get")
		add(namespace, "", "pods/portforward", "create")
		add(
			namespace,
			"",
			"services",
			"get",
			"list",
			"watch",
			"create",
			"update",
			"patch",
			"delete",
		)
		add(namespace, "", "secrets", "get", "list", "watch", "create", "update", "patch", "delete")
		add(
			namespace,
			"",
			"configmaps",
			"get",
			"list",
			"watch",
			"create",
			"update",
			"patch",
			"delete",
		)
		add(namespace, "", "serviceaccounts", "get", "list", "create", "update", "patch", "delete")
		add(namespace, "", "events", "get", "list")
		add(
			namespace,
			"batch",
			"jobs",
			"get",
			"list",
			"watch",
			"create",
			"update",
			"patch",
			"delete",
		)
		add(
			namespace,
			"apps",
			"deployments",
			"get",
			"list",
			"watch",
			"create",
			"update",
			"patch",
			"delete",
		)
		add(namespace, "apps", "replicasets", "get", "list", "watch")
		add(namespace, "networking.k8s.io", "networkpolicies", "get", "list", "watch")
	}

	add(
		sessionNamespace,
		"",
		"configmaps",
		"get",
		"list",
		"watch",
		"create",
		"update",
		"patch",
		"delete",
	)
	add(sessionNamespace, "coordination.k8s.io", "leases", "get", "create", "update")

	for _, namespace := range uniqueSorted([]string{sourceNamespace, stagingNamespace}) {
		add(
			namespace,
			"",
			"persistentvolumeclaims",
			"get",
			"list",
			"watch",
			"create",
			"update",
			"patch",
			"delete",
		)
		add(namespace, "", "resourcequotas", "get", "list")
		add(namespace, "", "limitranges", "get", "list")
	}

	add("", "", "namespaces", "get", "create")
	add("", "", "nodes", "get", "list")
	add("", "", "persistentvolumes", "get", "list", "watch", "update", "patch", "delete")
	add("", "storage.k8s.io", "storageclasses", "get", "list")
	add("", "storage.k8s.io", "csinodes", "get")
	add("", "storage.k8s.io", "volumeattachments", "get", "list", "watch")

	if inspectOpenEBSLVMShared {
		add("", "local.openebs.io", "lvmvolumes", "list")
	}

	if enableOpenEBSLVMShared {
		add("", "local.openebs.io", "lvmvolumes", "patch")
	}

	switch workload.Adapter {
	case domain.WorkloadStatefulSet, domain.WorkloadVictoriaLogs, domain.WorkloadVMCluster:
		add(workload.Controller.Namespace, "apps", "statefulsets", "get", "update")
	case domain.WorkloadDeployment, domain.WorkloadGrafana:
		add(workload.Controller.Namespace, "apps", "deployments", "get", "update")
	}

	switch workload.Adapter {
	case domain.WorkloadDeployment,
		domain.WorkloadGrafana,
		domain.WorkloadStatefulSet,
		domain.WorkloadVictoriaLogs,
		domain.WorkloadVMCluster:
		add(workload.Controller.Namespace, "autoscaling", "horizontalpodautoscalers", "list")
	}

	if workload.KubeBlocks != nil {
		instanceSetSwitchover := workload.Controller.Kind == domain.KindInstanceSet &&
			workload.KubeBlocks.SwitchoverCandidate != ""
		if instanceSetSwitchover &&
			workload.KubeBlocks.SwitchoverStrategy == domain.KubeBlocksSwitchoverMongoDBNative {
			add(sourceNamespace, "", "pods/exec", "create")
		}

		if workload.Controller.Kind != domain.KindInstanceSet ||
			(instanceSetSwitchover &&
				workload.KubeBlocks.SwitchoverStrategy == domain.KubeBlocksSwitchoverOpsRequest) {
			group, _, _ := strings.Cut(workload.KubeBlocks.OpsAPIVersion, "/")
			add(sourceNamespace, group, "opsrequests", "get", "create", "delete")
		}

		clusterGroup := domain.KubeBlocksAppsGroup
		add(sourceNamespace, clusterGroup, "clusters", "get")

		if workload.Controller.Kind == domain.KindInstanceSet {
			instanceSetGroup, _, _ := strings.Cut(workload.Controller.APIVersion, "/")
			add(sourceNamespace, instanceSetGroup, "instancesets", "get", "update", "patch")
		} else {
			add(sourceNamespace, clusterGroup, "clusters", "update", "patch")
		}
	}

	if workload.VMCluster != nil {
		group, _, _ := strings.Cut(workload.VMCluster.APIVersion, "/")
		add(sourceNamespace, group, "vmclusters", "get", "update", "patch")
	}

	if workload.Grafana != nil {
		group, _, _ := strings.Cut(workload.Grafana.APIVersion, "/")
		add(sourceNamespace, group, "grafanas", "get", "update", "patch")
	}

	p.checkAccessReviews(ctx, plan, checks)
}

func (p *Planner) checkRenameRBAC(
	ctx context.Context,
	plan *domain.MigrationPlan,
	sourceNamespace, destinationNamespace, sessionNamespace string,
) {
	checks := make([]rbacAccess, 0)
	add := func(namespace, group, resource string, verbs ...string) {
		for _, verb := range verbs {
			checks = append(
				checks,
				rbacAccess{namespace: namespace, verb: verb, group: group, resource: resource},
			)
		}
	}
	add(sourceNamespace, "", "pods", "list")
	add(destinationNamespace, "", "pods", "list")
	add(sourceNamespace, "", "persistentvolumeclaims", "get", "delete")
	add(destinationNamespace, "", "persistentvolumeclaims", "get", "create")
	add(sessionNamespace, "", "configmaps", "create", "update")
	add(sessionNamespace, "coordination.k8s.io", "leases", "get", "create", "update")
	add("", "", "namespaces", "get", "create")
	add("", "", "persistentvolumes", "get", "update")
	add("", "storage.k8s.io", "storageclasses", "get")
	add("", "storage.k8s.io", "volumeattachments", "list")
	p.checkAccessReviews(ctx, plan, checks)
}

func (p *Planner) checkAccessReviews(
	ctx context.Context,
	plan *domain.MigrationPlan,
	checks []rbacAccess,
) {
	type result struct {
		review *authorizationv1.SelfSubjectAccessReview
		err    error
	}

	results := make([]result, len(checks))
	run := func(index int) {
		check := checks[index]
		review, err := p.client.AuthorizationV1().
			SelfSubjectAccessReviews().
			Create(ctx, &authorizationv1.SelfSubjectAccessReview{
				Spec: authorizationv1.SelfSubjectAccessReviewSpec{
					ResourceAttributes: &authorizationv1.ResourceAttributes{
						Namespace: check.namespace,
						Verb:      check.verb,
						Group:     check.group,
						Resource:  check.resource,
					},
				},
			}, metav1.CreateOptions{})
		results[index] = result{review: review, err: err}
	}

	if len(checks) == 0 {
		plan.AddCheck(passed("rbac", "no Kubernetes permissions are required"))
		return
	}
	// Keep the existing fast-fail behavior for an unavailable authorization
	// endpoint, then fan out the remaining independent reviews.
	run(0)

	if results[0].err != nil {
		plan.AddCheck(
			failed("rbac", fmt.Sprintf("SelfSubjectAccessReview failed: %v", results[0].err)),
		)
		return
	}

	parallel.For(len(checks)-1, func(index int) {
		run(index + 1)
	})

	denied := make([]string, 0)
	for index, check := range checks {
		review := results[index].review
		if err := results[index].err; err != nil {
			plan.AddCheck(failed("rbac", fmt.Sprintf("SelfSubjectAccessReview failed: %v", err)))
			return
		}

		if review == nil {
			plan.AddCheck(failed("rbac", "SelfSubjectAccessReview returned an empty object"))
			return
		}

		if !review.Status.Allowed {
			identity := check.resource
			if check.namespace != "" {
				identity = check.namespace + "/" + identity
			}

			reason := review.Status.Reason
			if reason == "" {
				reason = review.Status.EvaluationError
			}

			denied = append(denied, fmt.Sprintf("%s %s (%s)", check.verb, identity, reason))
		}
	}

	if len(denied) > 0 {
		plan.AddCheck(failed("rbac", strings.Join(denied, "; ")))
		return
	}

	plan.AddCheck(
		passed("rbac", fmt.Sprintf("%d required Kubernetes permissions are allowed", len(checks))),
	)
}

package planner

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type rbacAccess struct {
	namespace string
	verb      string
	group     string
	resource  string
	name      string
}

type rbacChecks []rbacAccess

func (checks *rbacChecks) add(namespace, group, resource string, verbs ...string) {
	for _, verb := range verbs {
		*checks = append(
			*checks,
			rbacAccess{namespace: namespace, verb: verb, group: group, resource: resource},
		)
	}
}

func (p *Planner) checkRBAC(
	ctx context.Context,
	plan *domain.MigrationPlan,
	spec domain.SessionSpec,
	inspectOpenEBSLVMShared, enableOpenEBSLVMShared bool,
) {
	if p.controllerSubmission {
		p.checkControllerSubmissionRBAC(ctx, plan, spec)
		return
	}

	sourceNamespace, stagingNamespace, sessionNamespace := spec.SourceNamespace, spec.TemporaryNamespace, spec.SessionNamespace
	workload := spec.Workload()
	transfer := spec.Operation() != domain.OperationReserve

	p.logInfo(
		"checking migration RBAC permissions",
		"sourceNamespace",
		sourceNamespace,
		"stagingNamespace",
		stagingNamespace,
		"sessionNamespace",
		sessionNamespace,
	)

	checks := p.namespaceRBAC(ctx, plan, spec)

	add := checks.add
	for _, namespace := range uniqueSorted([]string{sourceNamespace, stagingNamespace}) {
		add(namespace, "", "pods", "get", "list", "create", "delete")
		add(namespace, "", "pods/log", "get")
		add(namespace, "", "events", "list")

		if !transfer {
			continue
		}

		add(namespace, "", "pods", "watch")

		if slices.Contains(spec.WorkflowOptions().Strategies, domain.StrategyLocal) {
			add(namespace, "", "pods/portforward", "create")
		}

		add(
			namespace,
			"",
			"services",
			"get",
			"list",
			"watch",
			"create",
			"patch",
			"delete",
		)
		// Helm's default Secret storage driver lists release history by label.
		add(namespace, "", "secrets", "get", "create", "patch", "delete")

		releaseResource := "secrets"
		if driver := os.Getenv("HELM_DRIVER"); driver == "configmap" || driver == "configmaps" {
			releaseResource = "configmaps"
		}

		add(namespace, "", releaseResource, "get", "list", "create", "update", "delete")
		add(namespace, "", "serviceaccounts", "create")

		for _, verb := range []string{"get", "update"} {
			checks = append(checks, rbacAccess{
				namespace: namespace, verb: verb,
				resource: "serviceaccounts", name: kube.TransferServiceAccountName,
			})
		}

		add(
			namespace,
			"batch",
			"jobs",
			"get",
			"list",
			"create",
			"patch",
			"delete",
		)
		add(
			namespace,
			"apps",
			"deployments",
			"get",
			"list",
			"create",
			"update",
			"patch",
			"delete",
		)
		add(namespace, "apps", "replicasets", "get", "list")
		add(namespace, "networking.k8s.io", "networkpolicies", "list")
	}

	add(
		sessionNamespace,
		"",
		"configmaps",
		"get",
		"list",
		"create",
		"update",
		"delete",
	)
	add(sessionNamespace, "coordination.k8s.io", "leases", "get", "create", "update", "delete")

	for _, namespace := range uniqueSorted([]string{sourceNamespace, stagingNamespace}) {
		add(namespace, "", "persistentvolumeclaims", "get", "update")

		if namespace == stagingNamespace || spec.Operation() == domain.OperationMigrate ||
			spec.Operation() == domain.OperationMigratePod {
			add(namespace, "", "persistentvolumeclaims", "create", "update", "delete")
		}

		add(namespace, "", "resourcequotas", "list")
		add(namespace, "", "limitranges", "list")
	}

	add("", "", "nodes", "get", "list")
	add("", "", "persistentvolumes", "get", "update", "delete")
	add("", "storage.k8s.io", "storageclasses", "get")
	add("", "storage.k8s.io", "csinodes", "get")
	add("", "storage.k8s.io", "volumeattachments", "list")

	if spec.Operation() == domain.OperationMigrate ||
		spec.Operation() == domain.OperationMigratePod {
		for _, namespace := range uniqueSorted([]string{sourceNamespace, spec.DestinationNamespace}) {
			add(namespace, "", "persistentvolumeclaims", "get", "create", "update", "delete")
			add(namespace, "", "pods", "list")
		}
	}

	if spec.Operation() == domain.OperationMigratePod {
		add(sourceNamespace, "", "pods", "update")
	}

	if inspectOpenEBSLVMShared {
		add("", "local.openebs.io", "lvmvolumes", "list")
	}

	if enableOpenEBSLVMShared {
		add("", "local.openebs.io", "lvmvolumes", "patch")
	}

	checks = append(checks, workloadRBAC(sourceNamespace, workload)...)
	p.checkAccessReviews(ctx, plan, checks)
}

func workloadRBAC(sourceNamespace string, workload domain.WorkloadSpec) rbacChecks {
	checks := rbacChecks{}
	add := checks.add

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
			add(sourceNamespace, instanceSetGroup, "instancesets", "get", "update")
		} else {
			add(sourceNamespace, clusterGroup, "clusters", "update")
		}
	}

	if workload.VMCluster != nil {
		group, _, _ := strings.Cut(workload.VMCluster.APIVersion, "/")
		add(sourceNamespace, group, "vmclusters", "get", "update")
	}

	if workload.Grafana != nil {
		group, _, _ := strings.Cut(workload.Grafana.APIVersion, "/")
		add(sourceNamespace, group, "grafanas", "get", "update")
	}

	return checks
}

func (p *Planner) checkRenameRBAC(
	ctx context.Context,
	plan *domain.MigrationPlan,
	spec domain.SessionSpec,
) {
	if p.controllerSubmission {
		p.checkControllerSubmissionRBAC(ctx, plan, spec)
		return
	}

	sourceNamespace, destinationNamespace, sessionNamespace := spec.SourceNamespace, spec.DestinationNamespace, spec.SessionNamespace
	checks := p.namespaceRBAC(ctx, plan, spec)
	add := checks.add
	add(sourceNamespace, "", "pods", "list")
	add(destinationNamespace, "", "pods", "list")
	add(sourceNamespace, "", "persistentvolumeclaims", "get", "delete")
	add(destinationNamespace, "", "persistentvolumeclaims", "get", "create")
	add(sessionNamespace, "", "configmaps", "get", "create", "update", "delete")
	add(sessionNamespace, "coordination.k8s.io", "leases", "get", "create", "update", "delete")
	add("", "", "persistentvolumes", "get", "update")
	add("", "storage.k8s.io", "storageclasses", "get")
	add("", "storage.k8s.io", "volumeattachments", "list")
	p.checkAccessReviews(ctx, plan, checks)
}

func (p *Planner) namespaceRBAC(
	ctx context.Context,
	plan *domain.MigrationPlan,
	spec domain.SessionSpec,
) rbacChecks {
	namespaces := make([]string, 0, 3+len(spec.Volumes))

	namespaces = append(namespaces,
		spec.SessionNamespace,
		spec.TemporaryNamespace,
		spec.DestinationNamespace,
	)
	for _, volume := range spec.Volumes {
		namespaces = append(namespaces, volume.DestinationPVC.Namespace)
	}

	checks := rbacChecks{}
	for _, namespace := range uniqueSorted(namespaces) {
		if namespace == "" {
			continue
		}

		checks = append(checks, rbacAccess{resource: "namespaces", verb: "get", name: namespace})

		_, err := p.client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			checks.add("", "", "namespaces", "create")
		case err != nil:
			plan.AddCheck(
				failed(domain.CheckNameRBAC, fmt.Sprintf("read namespace %s: %v", namespace, err)),
			)
		}
	}

	return checks
}

func (p *Planner) checkControllerSubmissionRBAC(
	ctx context.Context,
	plan *domain.MigrationPlan,
	spec domain.SessionSpec,
) {
	resource, ok := domain.ControllerResourceForSpec(spec)
	if !ok {
		plan.AddCheck(failed(domain.CheckNameRBAC, "workflow has no controller resource"))
		return
	}

	namespace := spec.SessionNamespace
	if resource.Cluster {
		namespace = ""
	}

	checks := []rbacAccess{
		{
			namespace: namespace,
			group:     "migrate.sealos.io",
			resource:  resource.Resource,
			verb:      "create",
		},
		{
			namespace: namespace,
			group:     "migrate.sealos.io",
			resource:  resource.Resource,
			name:      plan.SessionID,
			verb:      "get",
		},
		{
			namespace: namespace,
			group:     "migrate.sealos.io",
			resource:  resource.Resource,
			name:      plan.SessionID,
			verb:      "watch",
		},
	}
	p.checkAccessReviews(ctx, plan, checks)
}

func (p *Planner) checkAccessReviews(
	ctx context.Context,
	plan *domain.MigrationPlan,
	checks []rbacAccess,
) {
	seen := make(map[rbacAccess]struct{}, len(checks))

	unique := checks[:0]
	for _, check := range checks {
		if _, exists := seen[check]; !exists {
			seen[check] = struct{}{}
			unique = append(unique, check)
		}
	}

	checks = unique

	type result struct {
		review *authorizationv1.SelfSubjectAccessReview
		err    error
	}

	results := make([]result, len(checks))
	run := func(index int) {
		check := checks[index]
		resource, subresource, _ := strings.Cut(check.resource, "/")
		review, err := p.client.AuthorizationV1().
			SelfSubjectAccessReviews().
			Create(ctx, &authorizationv1.SelfSubjectAccessReview{
				Spec: authorizationv1.SelfSubjectAccessReviewSpec{
					ResourceAttributes: &authorizationv1.ResourceAttributes{
						Namespace:   check.namespace,
						Verb:        check.verb,
						Group:       check.group,
						Resource:    resource,
						Subresource: subresource,
						Name:        check.name,
					},
				},
			}, metav1.CreateOptions{})
		results[index] = result{review: review, err: err}
	}

	if len(checks) == 0 {
		plan.AddCheck(passed(domain.CheckNameRBAC, "no Kubernetes permissions are required"))
		return
	}
	// Keep the existing fast-fail behavior for an unavailable authorization
	// endpoint, then fan out the remaining independent reviews.
	run(0)

	if results[0].err != nil {
		plan.AddCheck(
			failed(
				domain.CheckNameRBAC,
				fmt.Sprintf("SelfSubjectAccessReview failed: %v", results[0].err),
			),
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
			plan.AddCheck(
				failed(
					domain.CheckNameRBAC,
					fmt.Sprintf("SelfSubjectAccessReview failed: %v", err),
				),
			)

			return
		}

		if review == nil {
			plan.AddCheck(
				failed(domain.CheckNameRBAC, "SelfSubjectAccessReview returned an empty object"),
			)
			return
		}

		if !review.Status.Allowed {
			identity := check.resource
			if check.group != "" {
				identity += "." + check.group
			}

			if check.name != "" {
				identity += "/" + check.name
			}

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
		plan.AddCheck(failed(domain.CheckNameRBAC, strings.Join(denied, "; ")))
		return
	}

	plan.AddCheck(
		passed(
			domain.CheckNameRBAC,
			fmt.Sprintf("%d required Kubernetes permissions are allowed", len(checks)),
		),
	)
}

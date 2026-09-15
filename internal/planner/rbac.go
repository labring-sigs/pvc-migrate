package planner

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
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

func reservationResourceAccess(
	sourceNamespace, stagingNamespace, sessionNamespace string,
) rbacChecks {
	checks := rbacChecks{}
	add := checks.add

	for _, namespace := range uniqueSorted([]string{sourceNamespace, stagingNamespace}) {
		add(namespace, "", "pods", "get", "list", "create", "delete")
		add(namespace, "", "pods/log", "get")
		add(namespace, "", "events", "list")
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

		if namespace == stagingNamespace {
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

	return checks
}

func transferToolAccess(namespaces, strategies []string) rbacChecks {
	checks := make(rbacChecks, 0, 40*len(namespaces))
	add := checks.add

	for _, namespace := range uniqueSorted(namespaces) {
		add(namespace, "", "pods", "watch")

		if slices.Contains(strategies, domain.StrategyLocal) {
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

	return checks
}

func activationResourceAccess(sourceNamespace, destinationNamespace string) rbacChecks {
	checks := rbacChecks{}
	for _, namespace := range uniqueSorted([]string{sourceNamespace, destinationNamespace}) {
		checks.add(namespace, "", "persistentvolumeclaims", "get", "create", "update", "delete")
		checks.add(namespace, "", "pods", "list")
	}

	return checks
}

func workloadRBAC(sourceNamespace string, workload v1alpha1.WorkloadSpec) rbacChecks {
	checks := rbacChecks{}
	add := checks.add

	controller := v1alpha1.LocalResourceReference{}
	if workload.Controller != nil {
		controller = *workload.Controller
	}

	switch workload.Adapter {
	case v1alpha1.WorkloadStatefulSet, v1alpha1.WorkloadVictoriaLogs, v1alpha1.WorkloadVMCluster:
		add(sourceNamespace, "apps", "statefulsets", "get", "update")
	case v1alpha1.WorkloadDeployment, v1alpha1.WorkloadGrafana:
		add(sourceNamespace, "apps", "deployments", "get", "update")
	}

	switch workload.Adapter {
	case v1alpha1.WorkloadDeployment,
		v1alpha1.WorkloadGrafana,
		v1alpha1.WorkloadStatefulSet,
		v1alpha1.WorkloadVictoriaLogs,
		v1alpha1.WorkloadVMCluster:
		add(sourceNamespace, "autoscaling", "horizontalpodautoscalers", "list")
	}

	if workload.KubeBlocks != nil {
		instanceSetSwitchover := controller.Kind == domain.KindInstanceSet &&
			workload.KubeBlocks.SwitchoverCandidate != ""
		if instanceSetSwitchover &&
			workload.KubeBlocks.SwitchoverStrategy == v1alpha1.KubeBlocksSwitchoverMongoDBNative {
			add(sourceNamespace, "", "pods/exec", "create")
		}

		if controller.Kind != domain.KindInstanceSet ||
			(instanceSetSwitchover &&
				workload.KubeBlocks.SwitchoverStrategy == v1alpha1.KubeBlocksSwitchoverOpsRequest) {
			group, _, _ := strings.Cut(workload.KubeBlocks.OpsAPIVersion, "/")
			add(sourceNamespace, group, "opsrequests", "get", "create", "delete")
		}

		clusterGroup := domain.KubeBlocksAppsGroup
		add(sourceNamespace, clusterGroup, "clusters", "get")

		if controller.Kind == domain.KindInstanceSet {
			instanceSetGroup, _, _ := strings.Cut(controller.APIVersion, "/")
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
	plan checkRecorder,
	sourceNamespace, destinationNamespace, sessionNamespace string,
) {
	if p.controllerSubmission {
		p.checkSubmissionNamespaces(ctx, plan, []string{destinationNamespace, sessionNamespace})
		return
	}

	checks := p.checkNamespaceAccess(ctx, plan, []string{destinationNamespace, sessionNamespace})
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

func (p *Planner) checkNamespaceAccess(
	ctx context.Context,
	plan checkRecorder,
	namespaces []string,
) rbacChecks {
	checks := rbacChecks{}
	for _, namespace := range uniqueSorted(namespaces) {
		if namespace == "" {
			continue
		}

		checks = append(checks, rbacAccess{resource: "namespaces", verb: "get", name: namespace})

		if err := kube.RequireNamespace(ctx, p.client, namespace); err != nil {
			plan.AddCheck(
				failed(domain.CheckNameNamespace, err.Error()),
			)
		}
	}

	return checks
}

func (p *Planner) checkSubmissionNamespaces(
	ctx context.Context,
	plan checkRecorder,
	namespaces []string,
) {
	for _, namespace := range uniqueSorted(namespaces) {
		if namespace == "" {
			continue
		}

		if err := kube.RequireNamespace(ctx, p.client, namespace); err != nil {
			if apierrors.IsForbidden(err) {
				plan.AddCheck(
					warned(
						domain.CheckNameNamespace,
						"namespace "+namespace+" existence must be verified by the controller: caller lacks get permission",
					),
				)
			} else {
				plan.AddCheck(failed(domain.CheckNameNamespace, err.Error()))
			}
		}
	}
}

func (p *Planner) checkWorkflowSubmissionAccess(
	ctx context.Context,
	plan checkRecorder,
	name, namespace string,
	resource string,
) {
	checks := []rbacAccess{
		{
			namespace: namespace,
			group:     "migrate.sealos.io",
			resource:  resource,
			verb:      "create",
		},
		{
			namespace: namespace,
			group:     "migrate.sealos.io",
			resource:  resource,
			name:      name,
			verb:      "get",
		},
		{
			namespace: namespace,
			group:     "migrate.sealos.io",
			resource:  resource,
			name:      name,
			verb:      "watch",
		},
	}
	p.checkAccessReviews(ctx, plan, checks)
}

func (p *Planner) checkAccessReviews(
	ctx context.Context,
	plan checkRecorder,
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

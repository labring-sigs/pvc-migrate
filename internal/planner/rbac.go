package planner

import (
	"context"
	"fmt"
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
		if kube.HelmReleaseStorageFromEnv() == kube.HelmReleaseStorageConfigMap {
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
	}

	return checks
}

// networkPolicyLifecycleVerbs are the permissions the transfer tool's helm
// release needs to manage its own allow-all NetworkPolicies: upstream enables
// the policies whenever create is permitted, and the release then reads,
// updates, and removes them on every install and uninstall.
var networkPolicyLifecycleVerbs = []string{"get", "list", "create", "update", "delete"}

// networkPolicyReview records the lifecycle-verb grants of one namespace.
type networkPolicyReview struct {
	namespace string
	allowed   map[string]bool
	err       error
}

// checkNetworkPolicyLifecycle verifies the identity that runs the transfer
// tools can manage the NetworkPolicies its helm releases install. A partial
// grant is the one broken state: upstream enables its policies on create and
// then fails mid-transfer without get, update, and delete, so planning fails
// fast instead. Without create the policies are skipped entirely and transfers
// only need namespaces that are not default-deny.
func (p *Planner) checkNetworkPolicyLifecycle(
	ctx context.Context,
	plan checkRecorder,
	namespaces []string,
) {
	if p.controllerSubmission {
		return
	}

	unique := make([]string, 0, len(namespaces))
	for _, namespace := range uniqueSorted(namespaces) {
		if namespace != "" {
			unique = append(unique, namespace)
		}
	}

	if len(unique) == 0 {
		return
	}

	reviews := make([]networkPolicyReview, len(unique))
	for index, namespace := range unique {
		// Fast-fail on an unavailable authorization endpoint, then continue
		// with the remaining namespaces only when reviews succeed.
		if index > 0 && reviews[index-1].err != nil {
			break
		}

		allowed := make(map[string]bool, len(networkPolicyLifecycleVerbs))
		for _, verb := range networkPolicyLifecycleVerbs {
			review, err := p.client.AuthorizationV1().
				SelfSubjectAccessReviews().
				Create(ctx, &authorizationv1.SelfSubjectAccessReview{
					Spec: authorizationv1.SelfSubjectAccessReviewSpec{
						ResourceAttributes: &authorizationv1.ResourceAttributes{
							Namespace: namespace,
							Verb:      verb,
							Group:     "networking.k8s.io",
							Resource:  "networkpolicies",
						},
					},
				}, metav1.CreateOptions{})
			if err != nil {
				reviews[index] = networkPolicyReview{namespace: namespace, err: err}
				break
			}

			allowed[verb] = review.Status.Allowed
		}

		if reviews[index].err == nil {
			reviews[index] = networkPolicyReview{namespace: namespace, allowed: allowed}
		}
	}

	for index := range unique {
		if reviews[index].err != nil {
			plan.AddCheck(failed(domain.CheckNameRBAC, fmt.Sprintf(
				"network policy permission review in %s failed: %v",
				reviews[index].namespace, reviews[index].err,
			)))

			return
		}
	}

	plan.AddCheck(classifyNetworkPolicyAccess(reviews))
}

// classifyNetworkPolicyAccess reduces the per-namespace grants to one check:
// the weakest namespace decides. Partial grants (create without get, update,
// or delete) fail because the tools would install policies they cannot
// manage; namespaces without create keep working without the policies.
func classifyNetworkPolicyAccess(reviews []networkPolicyReview) domain.Check {
	managed, skipped := make([]string, 0, len(reviews)), make([]string, 0, len(reviews))

	for _, result := range reviews {
		if result.err != nil || result.allowed == nil {
			break
		}

		missing := make([]string, 0, len(networkPolicyLifecycleVerbs))
		for _, verb := range networkPolicyLifecycleVerbs {
			if !result.allowed[verb] {
				missing = append(missing, verb)
			}
		}

		switch {
		case !result.allowed["list"]:
			return failed(domain.CheckNameRBAC, fmt.Sprintf(
				"networkpolicies list in %s is required to verify namespace isolation before a transfer",
				result.namespace,
			))
		case result.allowed["create"] && len(missing) > 0:
			return failed(domain.CheckNameRBAC, fmt.Sprintf(
				"partial network policy permissions in %s: create is allowed, so the transfer tools install their own NetworkPolicies, but %s is denied and their helm release cannot manage them; grant get, list, create, update, and delete together or remove them all",
				result.namespace,
				strings.Join(missing, ", "),
			))
		case result.allowed["create"]:
			managed = append(managed, result.namespace)
		default:
			skipped = append(skipped, result.namespace)
		}
	}

	if len(skipped) > 0 {
		return warned(domain.CheckNameRBAC, fmt.Sprintf(
			"creating NetworkPolicies in %s is not permitted, so transfers there run without the tools' allow-all policies; those namespaces must not isolate Pods with a default-deny policy",
			strings.Join(skipped, ", "),
		))
	}

	return passed(
		domain.CheckNameRBAC,
		"transfer NetworkPolicy lifecycle (get, list, create, update, delete) is allowed in "+strings.Join(
			managed,
			", ",
		),
	)
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

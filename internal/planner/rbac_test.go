package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestCheckRBACIncludesToolAndVolumePermissions(t *testing.T) {
	seen := collectAllowedAccessReviews(t, domain.WorkloadSpec{}, false, false)
	for _, verb := range []string{"get", "list", "create", "update", "patch", "delete"} {
		if !hasAccessReview(seen, authorizationv1.ResourceAttributes{
			Namespace: "app", Verb: verb, Resource: "secrets",
		}) {
			t.Fatalf("planner should require %s access to Secrets", verb)
		}
	}

	want := []authorizationv1.ResourceAttributes{
		{Namespace: "app", Verb: "create", Resource: "pods/portforward"},
		{
			Namespace: "app",
			Verb:      "get",
			Resource:  "serviceaccounts",
			Name:      kube.TransferServiceAccountName,
		},
		{Namespace: "app", Verb: "create", Resource: "serviceaccounts"},
		{
			Namespace: "app",
			Verb:      "update",
			Resource:  "serviceaccounts",
			Name:      kube.TransferServiceAccountName,
		},
		{Namespace: "system", Verb: "get", Group: "coordination.k8s.io", Resource: "leases"},
		{Namespace: "system", Verb: "create", Group: "coordination.k8s.io", Resource: "leases"},
		{Namespace: "system", Verb: "update", Group: "coordination.k8s.io", Resource: "leases"},
		{Verb: "list", Group: "storage.k8s.io", Resource: "volumeattachments"},
	}
	for _, attributes := range want {
		if !hasAccessReview(seen, attributes) {
			t.Fatalf("missing access review %#v", attributes)
		}
	}

	for _, verb := range []string{"patch", "delete"} {
		if hasAccessReview(seen, authorizationv1.ResourceAttributes{
			Namespace: "app", Verb: verb, Resource: "serviceaccounts",
		}) {
			t.Fatalf("planner should not require %s access to ServiceAccounts", verb)
		}
	}
}

func TestCheckRBACIncludesOpenEBSLVMVolumePermissionsWhenNeeded(t *testing.T) {
	list := authorizationv1.ResourceAttributes{
		Verb:     "list",
		Group:    "local.openebs.io",
		Resource: "lvmvolumes",
	}
	patch := authorizationv1.ResourceAttributes{
		Verb:     "patch",
		Group:    "local.openebs.io",
		Resource: "lvmvolumes",
	}

	inspectOnly := collectAllowedAccessReviews(t, domain.WorkloadSpec{}, true, false)
	if !hasAccessReview(inspectOnly, list) || hasAccessReview(inspectOnly, patch) {
		t.Fatalf("inspect-only access reviews=%#v", inspectOnly)
	}

	withAutoEnable := collectAllowedAccessReviews(t, domain.WorkloadSpec{}, true, true)
	if !hasAccessReview(withAutoEnable, list) || !hasAccessReview(withAutoEnable, patch) {
		t.Fatalf("auto-enable access reviews=%#v", withAutoEnable)
	}
}

func TestCheckRBACRejectsMissingSessionLeasePermission(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()
			attributes := review.Spec.ResourceAttributes
			review.Status.Allowed = attributes.Group != "coordination.k8s.io" ||
				attributes.Resource != "leases" ||
				attributes.Verb != "create"
			review.Status.Reason = "Lease permission denied"

			return true, review, nil
		},
	)

	plan := &domain.MigrationPlan{Ready: true}
	New(
		client,
		nil,
	).checkRBAC(context.Background(), plan, rbacTestSpec("stage", domain.WorkloadSpec{}), false, false)

	if plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, "create system/leases") {
		t.Fatalf("RBAC result=%#v", plan.Checks)
	}
}

func TestCheckRBACIncludesControllerSpecificPermissions(t *testing.T) {
	tests := []struct {
		name     string
		workload domain.WorkloadSpec
		want     []authorizationv1.ResourceAttributes
		exclude  []authorizationv1.ResourceAttributes
	}{
		{
			name: "StatefulSet",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadStatefulSet,
				Controller: domain.ObjectReference{Namespace: "app", Name: "db"},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "update", Group: "apps", Resource: "statefulsets"},
				{
					Namespace: "app",
					Verb:      "list",
					Group:     "autoscaling",
					Resource:  "horizontalpodautoscalers",
				},
			},
		},
		{
			name: "KubeBlocks alternate API group",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion: "operations.kubeblocks.io/v1alpha1",
				},
			},
			want: []authorizationv1.ResourceAttributes{
				{
					Namespace: "app",
					Verb:      "create",
					Group:     "operations.kubeblocks.io",
					Resource:  "opsrequests",
				},
				{Namespace: "app", Verb: "get", Group: "apps.kubeblocks.io", Resource: "clusters"},
				{
					Namespace: "app",
					Verb:      "update",
					Group:     "apps.kubeblocks.io",
					Resource:  "clusters",
				},
			},
		},
		{
			name: "KubeBlocks InstanceSet",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				Controller: domain.ObjectReference{
					APIVersion: "workloads.kubeblocks.io/v1alpha1",
					Kind:       "InstanceSet",
					Namespace:  "app",
					Name:       "cluster-db",
				},
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion: "operations.kubeblocks.io/v1alpha1",
				},
			},
			want: []authorizationv1.ResourceAttributes{
				{
					Namespace: "app",
					Verb:      "get",
					Group:     "workloads.kubeblocks.io",
					Resource:  "instancesets",
				},
				{
					Namespace: "app",
					Verb:      "update",
					Group:     "workloads.kubeblocks.io",
					Resource:  "instancesets",
				},
			},
			exclude: []authorizationv1.ResourceAttributes{
				{
					Namespace: "app",
					Verb:      "create",
					Group:     "operations.kubeblocks.io",
					Resource:  "opsrequests",
				},
				{
					Namespace: "app",
					Verb:      "update",
					Group:     "apps.kubeblocks.io",
					Resource:  "clusters",
				},
				{
					Namespace: "app",
					Verb:      "patch",
					Group:     "apps.kubeblocks.io",
					Resource:  "clusters",
				},
			},
		},
		{
			name: "KubeBlocks MongoDB native switchover",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				Controller: domain.ObjectReference{
					APIVersion: "workloads.kubeblocks.io/v1alpha1",
					Kind:       domain.KindInstanceSet,
				},
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion:       "apps.kubeblocks.io/v1alpha1",
					SwitchoverCandidate: "cluster-db-1",
					SwitchoverStrategy:  domain.KubeBlocksSwitchoverMongoDBNative,
				},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "create", Resource: "pods/exec"},
			},
			exclude: []authorizationv1.ResourceAttributes{
				{
					Namespace: "app",
					Verb:      "create",
					Group:     "apps.kubeblocks.io",
					Resource:  "opsrequests",
				},
			},
		},
		{
			name: "KubeBlocks InstanceSet OpsRequest switchover",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				Controller: domain.ObjectReference{
					APIVersion: "workloads.kubeblocks.io/v1alpha1",
					Kind:       domain.KindInstanceSet,
				},
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion:       "operations.kubeblocks.io/v1alpha1",
					SwitchoverCandidate: "cluster-db-1",
					SwitchoverStrategy:  domain.KubeBlocksSwitchoverOpsRequest,
				},
			},
			want: []authorizationv1.ResourceAttributes{
				{
					Namespace: "app",
					Verb:      "create",
					Group:     "operations.kubeblocks.io",
					Resource:  "opsrequests",
				},
			},
		},
		{
			name: "legacy KubeBlocks ignores stale native switchover",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadKubeBlocks,
				Controller: domain.ObjectReference{Kind: domain.KindStatefulSet},
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion:      "apps.kubeblocks.io/v1alpha1",
					SwitchoverStrategy: domain.KubeBlocksSwitchoverMongoDBNative,
				},
			},
			exclude: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "create", Resource: "pods/exec"},
			},
		},
		{
			name: "VMCluster",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadVMCluster,
				Controller: domain.ObjectReference{Namespace: "app", Name: "metrics"},
				VMCluster: &domain.VMClusterSpec{
					APIVersion: "operator.victoriametrics.com/v1beta1",
				},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "update", Group: "apps", Resource: "statefulsets"},
				{
					Namespace: "app",
					Verb:      "get",
					Group:     "operator.victoriametrics.com",
					Resource:  "vmclusters",
				},
				{
					Namespace: "app",
					Verb:      "update",
					Group:     "operator.victoriametrics.com",
					Resource:  "vmclusters",
				},
			},
		},
		{
			name: "Grafana",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadGrafana,
				Controller: domain.ObjectReference{Namespace: "app", Name: "grafana"},
				Grafana:    &domain.GrafanaSpec{APIVersion: "grafana.integreatly.org/v1beta1"},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "update", Group: "apps", Resource: "deployments"},
				{
					Namespace: "app",
					Verb:      "list",
					Group:     "autoscaling",
					Resource:  "horizontalpodautoscalers",
				},
				{
					Namespace: "app",
					Verb:      "get",
					Group:     "grafana.integreatly.org",
					Resource:  "grafanas",
				},
				{
					Namespace: "app",
					Verb:      "update",
					Group:     "grafana.integreatly.org",
					Resource:  "grafanas",
				},
			},
		},
		{
			name: "Deployment",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadDeployment,
				Controller: domain.ObjectReference{Namespace: "app", Name: "web"},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "get", Group: "apps", Resource: "deployments"},
				{Namespace: "app", Verb: "update", Group: "apps", Resource: "deployments"},
				{
					Namespace: "app",
					Verb:      "list",
					Group:     "autoscaling",
					Resource:  "horizontalpodautoscalers",
				},
			},
		},
		{
			name: "Victoria Logs",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadVictoriaLogs,
				Controller: domain.ObjectReference{Namespace: "app", Name: "logs"},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "get", Group: "apps", Resource: "statefulsets"},
				{Namespace: "app", Verb: "update", Group: "apps", Resource: "statefulsets"},
				{
					Namespace: "app",
					Verb:      "list",
					Group:     "autoscaling",
					Resource:  "horizontalpodautoscalers",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := kubernetesfake.NewClientset()
			seen := make([]authorizationv1.ResourceAttributes, 0)
			client.PrependReactor(
				"create",
				"selfsubjectaccessreviews",
				func(action clienttesting.Action) (bool, runtime.Object, error) {
					review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
						t,
						action,
					).DeepCopy()
					seen = append(seen, *review.Spec.ResourceAttributes.DeepCopy())
					review.Status.Allowed = true

					return true, review, nil
				},
			)

			plan := &domain.MigrationPlan{Ready: true}
			New(
				client,
				nil,
			).checkRBAC(context.Background(), plan, rbacTestSpec("stage", tt.workload), false, false)

			if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
				t.Fatalf("RBAC result: %#v", plan.Checks)
			}

			for _, want := range tt.want {
				if !hasAccessReview(seen, want) {
					t.Fatalf("missing access review %#v", want)
				}
			}

			for _, excluded := range tt.exclude {
				if hasAccessReview(seen, excluded) {
					t.Fatalf("unexpected access review %#v", excluded)
				}
			}
		})
	}
}

func TestCheckRBACDeduplicatesEqualSourceAndStagingNamespace(t *testing.T) {
	client := kubernetesfake.NewClientset()
	podGets := 0
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()

			attributes := review.Spec.ResourceAttributes
			if attributes.Namespace == "app" && attributes.Verb == "get" &&
				attributes.Resource == "pods" && attributes.Subresource == "" {
				podGets++
			}

			review.Status.Allowed = true

			return true, review, nil
		},
	)

	plan := &domain.MigrationPlan{Ready: true}
	New(
		client,
		nil,
	).checkRBAC(context.Background(), plan, rbacTestSpec("app", domain.WorkloadSpec{}), false, false)

	if podGets != 1 {
		t.Fatalf("Pod get reviews=%d want=1", podGets)
	}
}

func TestCheckRBACAggregatesDeniedPermissionsAndReasons(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()
			attributes := review.Spec.ResourceAttributes

			review.Status.Allowed = true
			if attributes.Namespace == "app" && attributes.Verb == "delete" &&
				attributes.Resource == "pods" {
				review.Status.Allowed = false
				review.Status.Reason = "policy denied"
			}

			if attributes.Namespace == "" && attributes.Verb == "delete" &&
				attributes.Resource == "persistentvolumes" {
				review.Status.Allowed = false
				review.Status.EvaluationError = "authorizer unavailable"
			}

			return true, review, nil
		},
	)

	plan := &domain.MigrationPlan{Ready: true}
	New(
		client,
		nil,
	).checkRBAC(context.Background(), plan, rbacTestSpec("stage", domain.WorkloadSpec{}), false, false)

	if plan.Ready || len(plan.Checks) != 1 {
		t.Fatalf("RBAC result: %#v", plan.Checks)
	}

	for _, expected := range []string{"delete app/pods (policy denied)", "delete persistentvolumes (authorizer unavailable)"} {
		if !strings.Contains(plan.Checks[0].Message, expected) {
			t.Fatalf("RBAC message omits %q: %s", expected, plan.Checks[0].Message)
		}
	}
}

func TestCheckRBACStopsOnReviewError(t *testing.T) {
	client := kubernetesfake.NewClientset()
	calls := 0
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			calls++
			return true, nil, errors.New("authorization API unavailable")
		},
	)

	plan := &domain.MigrationPlan{Ready: true}
	New(
		client,
		nil,
	).checkRBAC(context.Background(), plan, rbacTestSpec("stage", domain.WorkloadSpec{}), false, false)

	if calls != 1 || plan.Ready || len(plan.Checks) != 1 ||
		!strings.Contains(plan.Checks[0].Message, "authorization API unavailable") {
		t.Fatalf("calls=%d checks=%#v", calls, plan.Checks)
	}
}

func hasAccessReview(
	seen []authorizationv1.ResourceAttributes,
	want authorizationv1.ResourceAttributes,
) bool {
	if resource, subresource, ok := strings.Cut(want.Resource, "/"); ok {
		want.Resource, want.Subresource = resource, subresource
	}

	for _, attributes := range seen {
		if attributes.Namespace == want.Namespace && attributes.Verb == want.Verb &&
			attributes.Group == want.Group &&
			attributes.Resource == want.Resource && attributes.Subresource == want.Subresource &&
			attributes.Name == want.Name {
			return true
		}
	}

	return false
}

func collectAllowedAccessReviews(
	t *testing.T,
	workload domain.WorkloadSpec,
	inspectOpenEBSLVMShared, enableOpenEBSLVMShared bool,
) []authorizationv1.ResourceAttributes {
	t.Helper()

	client := kubernetesfake.NewClientset()
	seen := make([]authorizationv1.ResourceAttributes, 0)
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			review := testutil.MustActionObject[*authorizationv1.SelfSubjectAccessReview](
				t,
				action,
			).DeepCopy()
			seen = append(seen, *review.Spec.ResourceAttributes.DeepCopy())
			review.Status.Allowed = true

			return true, review, nil
		},
	)

	plan := &domain.MigrationPlan{Ready: true}
	New(
		client,
		nil,
	).checkRBAC(context.Background(), plan, rbacTestSpec("stage", workload), inspectOpenEBSLVMShared, enableOpenEBSLVMShared)

	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("RBAC result: %#v", plan.Checks)
	}

	return seen
}

func rbacTestSpec(staging string, workload domain.WorkloadSpec) domain.SessionSpec {
	return domain.NewPodMigrationSessionSpec(domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: staging,
		DestinationNamespace: "app", SessionNamespace: "system",
	}, workload, domain.SessionWorkflowOptions{Strategies: []string{domain.StrategyLocal}}, 1, false)
}

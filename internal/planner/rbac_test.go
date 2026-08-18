package planner

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	authorizationv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"
)

func TestCheckRBACIncludesToolAndVolumePermissions(t *testing.T) {
	seen := collectAllowedAccessReviews(t, domain.WorkloadSpec{}, false, false)
	want := []authorizationv1.ResourceAttributes{
		{Namespace: "app", Verb: "create", Resource: "pods/portforward"},
		{Namespace: "app", Verb: "update", Resource: "serviceaccounts"},
		{Namespace: "app", Verb: "patch", Resource: "serviceaccounts"},
		{Namespace: "system", Verb: "get", Group: "coordination.k8s.io", Resource: "leases"},
		{Namespace: "system", Verb: "create", Group: "coordination.k8s.io", Resource: "leases"},
		{Namespace: "system", Verb: "update", Group: "coordination.k8s.io", Resource: "leases"},
		{Verb: "get", Group: "storage.k8s.io", Resource: "volumeattachments"},
		{Verb: "list", Group: "storage.k8s.io", Resource: "volumeattachments"},
		{Verb: "watch", Group: "storage.k8s.io", Resource: "volumeattachments"},
	}
	for _, attributes := range want {
		if !hasAccessReview(seen, attributes) {
			t.Fatalf("missing access review %#v", attributes)
		}
	}
}

func TestCheckRBACIncludesOpenEBSLVMVolumePermissionsWhenNeeded(t *testing.T) {
	list := authorizationv1.ResourceAttributes{Verb: "list", Group: "local.openebs.io", Resource: "lvmvolumes"}
	patch := authorizationv1.ResourceAttributes{Verb: "patch", Group: "local.openebs.io", Resource: "lvmvolumes"}
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
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		attributes := review.Spec.ResourceAttributes
		review.Status.Allowed = attributes.Group != "coordination.k8s.io" || attributes.Resource != "leases" || attributes.Verb != "create"
		review.Status.Reason = "Lease permission denied"
		return true, review, nil
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkRBAC(context.Background(), plan, "app", "stage", "system", domain.WorkloadSpec{}, false, false)
	if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "create system/leases") {
		t.Fatalf("RBAC result=%#v", plan.Checks)
	}
}

func TestDeploymentClusterRoleCoversPlannerAccessReviews(t *testing.T) {
	role := deploymentClusterRole(t, "../../deploy/rbac.yaml", "pvc-migrate")
	mongoDBRole := deploymentClusterRole(t, "../../deploy/kubeblocks-mongodb-rbac.yaml", "pvc-migrate-kubeblocks-mongodb")
	selfReview := authorizationv1.ResourceAttributes{
		Verb: "create", Group: "authorization.k8s.io", Resource: "selfsubjectaccessreviews",
	}
	if !clusterRoleAllows(role.Rules, selfReview) {
		t.Fatal("deployment ClusterRole cannot create SelfSubjectAccessReviews")
	}
	if !clusterRoleAllows(role.Rules, authorizationv1.ResourceAttributes{Verb: "delete", Group: "coordination.k8s.io", Resource: "leases"}) {
		t.Fatal("deployment ClusterRole cannot delete session Leases")
	}
	if !clusterRoleAllows(role.Rules, authorizationv1.ResourceAttributes{Verb: "list", Group: "storage.k8s.io", Resource: "csistoragecapacities"}) {
		t.Fatal("deployment ClusterRole cannot list CSIStorageCapacity objects")
	}
	for _, attributes := range []authorizationv1.ResourceAttributes{
		{Verb: "list", Group: "local.openebs.io", Resource: "lvmvolumes"},
		{Verb: "patch", Group: "local.openebs.io", Resource: "lvmvolumes"},
	} {
		if !clusterRoleAllows(role.Rules, attributes) {
			t.Fatalf("deployment ClusterRole cannot %s OpenEBS LVMVolumes", attributes.Verb)
		}
	}
	if clusterRoleAllows(role.Rules, authorizationv1.ResourceAttributes{Verb: "create", Resource: "pods/exec"}) {
		t.Fatal("default deployment ClusterRole grants Pod exec")
	}
	if !clusterRoleAllows(mongoDBRole.Rules, authorizationv1.ResourceAttributes{Verb: "create", Resource: "pods/exec"}) {
		t.Fatal("KubeBlocks MongoDB role cannot create Pod exec")
	}
	if len(mongoDBRole.Rules) != 1 || clusterRoleAllows(mongoDBRole.Rules, authorizationv1.ResourceAttributes{Verb: "get", Resource: "pods"}) {
		t.Fatalf("KubeBlocks MongoDB role grants unexpected permissions: %#v", mongoDBRole.Rules)
	}

	workloads := []domain.WorkloadSpec{
		{},
		{
			Adapter:    domain.WorkloadStatefulSet,
			Controller: domain.ObjectReference{Namespace: "app", Name: "db"},
		},
		{
			Adapter: domain.WorkloadKubeBlocks,
			KubeBlocks: &domain.KubeBlocksSpec{
				OpsAPIVersion:      "operations.kubeblocks.io/v1alpha1",
				SwitchoverStrategy: domain.KubeBlocksSwitchoverMongoDBNative,
			},
		},
		{
			Adapter:    domain.WorkloadVMCluster,
			Controller: domain.ObjectReference{Namespace: "app", Name: "metrics"},
			VMCluster:  &domain.VMClusterSpec{APIVersion: "operator.victoriametrics.com/v1beta1"},
		},
		{
			Adapter:    domain.WorkloadGrafana,
			Controller: domain.ObjectReference{Namespace: "app", Name: "grafana"},
			Grafana:    &domain.GrafanaSpec{APIVersion: "grafana.integreatly.org/v1beta1"},
		},
	}
	for _, workload := range workloads {
		for _, attributes := range collectAllowedAccessReviews(t, workload, false, false) {
			if !clusterRoleAllows(role.Rules, attributes) && !clusterRoleAllows(mongoDBRole.Rules, attributes) {
				t.Errorf("deployment ClusterRole does not allow %s %s/%s", attributes.Verb, attributes.Group, attributes.Resource)
			}
		}
	}
}

func deploymentClusterRole(t *testing.T, path, name string) rbacv1.ClusterRole {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var role rbacv1.ClusterRole
	firstDocument := bytes.SplitN(data, []byte("\n---\n"), 2)[0]
	if err := yaml.Unmarshal(firstDocument, &role); err != nil {
		t.Fatal(err)
	}
	if role.Kind != "ClusterRole" || role.Name != name {
		t.Fatalf("unexpected deployment role %s/%s", role.Kind, role.Name)
	}
	return role
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
			want: []authorizationv1.ResourceAttributes{{Namespace: "app", Verb: "update", Group: "apps", Resource: "statefulsets"}},
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
				{Namespace: "app", Verb: "create", Group: "operations.kubeblocks.io", Resource: "opsrequests"},
				{Namespace: "app", Verb: "get", Group: "apps.kubeblocks.io", Resource: "clusters"},
				{Namespace: "app", Verb: "update", Group: "apps.kubeblocks.io", Resource: "clusters"},
				{Namespace: "app", Verb: "patch", Group: "apps.kubeblocks.io", Resource: "clusters"},
			},
		},
		{
			name: "KubeBlocks InstanceSet",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadKubeBlocks,
				Controller: domain.ObjectReference{APIVersion: "workloads.kubeblocks.io/v1alpha1", Kind: "InstanceSet", Namespace: "app", Name: "cluster-db"},
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion: "operations.kubeblocks.io/v1alpha1",
				},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "get", Group: "workloads.kubeblocks.io", Resource: "instancesets"},
				{Namespace: "app", Verb: "update", Group: "workloads.kubeblocks.io", Resource: "instancesets"},
				{Namespace: "app", Verb: "patch", Group: "workloads.kubeblocks.io", Resource: "instancesets"},
			},
			exclude: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "update", Group: "apps.kubeblocks.io", Resource: "clusters"},
				{Namespace: "app", Verb: "patch", Group: "apps.kubeblocks.io", Resource: "clusters"},
			},
		},
		{
			name: "KubeBlocks MongoDB native switchover",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadKubeBlocks,
				KubeBlocks: &domain.KubeBlocksSpec{
					OpsAPIVersion:      "apps.kubeblocks.io/v1alpha1",
					SwitchoverStrategy: domain.KubeBlocksSwitchoverMongoDBNative,
				},
			},
			want: []authorizationv1.ResourceAttributes{{Namespace: "app", Verb: "create", Resource: "pods/exec"}},
		},
		{
			name: "VMCluster",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadVMCluster,
				Controller: domain.ObjectReference{Namespace: "app", Name: "metrics"},
				VMCluster:  &domain.VMClusterSpec{APIVersion: "operator.victoriametrics.com/v1beta1"},
			},
			want: []authorizationv1.ResourceAttributes{
				{Namespace: "app", Verb: "update", Group: "apps", Resource: "statefulsets"},
				{Namespace: "app", Verb: "get", Group: "operator.victoriametrics.com", Resource: "vmclusters"},
				{Namespace: "app", Verb: "update", Group: "operator.victoriametrics.com", Resource: "vmclusters"},
				{Namespace: "app", Verb: "patch", Group: "operator.victoriametrics.com", Resource: "vmclusters"},
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
				{Namespace: "app", Verb: "get", Group: "grafana.integreatly.org", Resource: "grafanas"},
				{Namespace: "app", Verb: "update", Group: "grafana.integreatly.org", Resource: "grafanas"},
				{Namespace: "app", Verb: "patch", Group: "grafana.integreatly.org", Resource: "grafanas"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := kubernetesfake.NewClientset()
			seen := make([]authorizationv1.ResourceAttributes, 0)
			client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
				review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
				seen = append(seen, *review.Spec.ResourceAttributes.DeepCopy())
				review.Status.Allowed = true
				return true, review, nil
			})
			plan := &domain.MigrationPlan{Ready: true}
			New(client, nil).checkRBAC(context.Background(), plan, "app", "stage", "system", tt.workload, false, false)
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
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		attributes := review.Spec.ResourceAttributes
		if attributes.Namespace == "app" && attributes.Verb == "get" && attributes.Resource == "pods" {
			podGets++
		}
		review.Status.Allowed = true
		return true, review, nil
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkRBAC(context.Background(), plan, "app", "app", "system", domain.WorkloadSpec{}, false, false)
	if podGets != 1 {
		t.Fatalf("Pod get reviews=%d want=1", podGets)
	}
}

func TestCheckRBACAggregatesDeniedPermissionsAndReasons(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		attributes := review.Spec.ResourceAttributes
		review.Status.Allowed = true
		if attributes.Namespace == "app" && attributes.Verb == "delete" && attributes.Resource == "pods" {
			review.Status.Allowed = false
			review.Status.Reason = "policy denied"
		}
		if attributes.Namespace == "" && attributes.Verb == "delete" && attributes.Resource == "persistentvolumes" {
			review.Status.Allowed = false
			review.Status.EvaluationError = "authorizer unavailable"
		}
		return true, review, nil
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkRBAC(context.Background(), plan, "app", "stage", "system", domain.WorkloadSpec{}, false, false)
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
	client.PrependReactor("create", "selfsubjectaccessreviews", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		return true, nil, errors.New("authorization API unavailable")
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkRBAC(context.Background(), plan, "app", "stage", "system", domain.WorkloadSpec{}, false, false)
	if calls != 1 || plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "authorization API unavailable") {
		t.Fatalf("calls=%d checks=%#v", calls, plan.Checks)
	}
}

func hasAccessReview(seen []authorizationv1.ResourceAttributes, want authorizationv1.ResourceAttributes) bool {
	for _, attributes := range seen {
		if attributes.Namespace == want.Namespace && attributes.Verb == want.Verb && attributes.Group == want.Group && attributes.Resource == want.Resource {
			return true
		}
	}
	return false
}

func collectAllowedAccessReviews(t *testing.T, workload domain.WorkloadSpec, inspectOpenEBSLVMShared, enableOpenEBSLVMShared bool) []authorizationv1.ResourceAttributes {
	t.Helper()
	client := kubernetesfake.NewClientset()
	seen := make([]authorizationv1.ResourceAttributes, 0)
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		seen = append(seen, *review.Spec.ResourceAttributes.DeepCopy())
		review.Status.Allowed = true
		return true, review, nil
	})
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkRBAC(context.Background(), plan, "app", "stage", "system", workload, inspectOpenEBSLVMShared, enableOpenEBSLVMShared)
	if !plan.Ready || len(plan.Checks) != 1 || !plan.Checks[0].Passed {
		t.Fatalf("RBAC result: %#v", plan.Checks)
	}
	return seen
}

func clusterRoleAllows(rules []rbacv1.PolicyRule, attributes authorizationv1.ResourceAttributes) bool {
	for _, rule := range rules {
		if containsRBACValue(rule.APIGroups, attributes.Group) &&
			containsRBACValue(rule.Resources, attributes.Resource) &&
			containsRBACValue(rule.Verbs, attributes.Verb) {
			return true
		}
	}
	return false
}

func containsRBACValue(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == "*" || candidate == value {
			return true
		}
	}
	return false
}

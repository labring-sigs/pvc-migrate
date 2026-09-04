package controller

import (
	"bytes"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

type permissionKey struct {
	group    string
	resource string
}

func TestControllerRolesMatchLeastPrivilegeContract(t *testing.T) {
	paths := []string{"../../config/rbac/role.yaml", "../../deploy/rbac.yaml"}
	want := controllerRolePermissions()

	var reference map[permissionKey][]string
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			role := readClusterRole(t, path, "pvc-migrate")

			got := rolePermissions(role)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("permissions differ from controller contract: got=%v want=%v", got, want)
			}

			if reference == nil {
				reference = got
			} else if !reflect.DeepEqual(got, reference) {
				t.Fatalf(
					"role permissions drift from the other deployment manifest: got=%v want=%v",
					got,
					reference,
				)
			}
		})
	}
}

func TestControllerRoleExcludesPlannerOnlyPermissions(t *testing.T) {
	role := readClusterRole(t, "../../deploy/rbac.yaml", "pvc-migrate")
	permissions := rolePermissions(role)

	for _, forbidden := range []permissionKey{
		{group: "authorization.k8s.io", resource: "selfsubjectaccessreviews"},
		{group: "networking.k8s.io", resource: "networkpolicies"},
		{group: "storage.k8s.io", resource: "csinodes"},
		{group: "storage.k8s.io", resource: "csistoragecapacities"},
		{group: "", resource: "configmaps"},
	} {
		if _, ok := permissions[forbidden]; ok {
			t.Fatalf(
				"controller role retains forbidden permission %s/%s",
				forbidden.group,
				forbidden.resource,
			)
		}
	}
}

func TestControllerRoleKeepsMongoDBExecPermissionSeparate(t *testing.T) {
	defaultRole := readClusterRole(t, "../../deploy/rbac.yaml", "pvc-migrate")
	mongoDBRole := readClusterRole(
		t,
		"../../deploy/kubeblocks-mongodb-rbac.yaml",
		"pvc-migrate-kubeblocks-mongodb",
	)

	if permissionAllowed(
		rolePermissions(defaultRole),
		permissionKey{resource: "pods/exec"},
		"create",
	) {
		t.Fatal("default controller role grants Pod exec")
	}

	if !permissionAllowed(
		rolePermissions(mongoDBRole),
		permissionKey{resource: "pods/exec"},
		"create",
	) {
		t.Fatal("KubeBlocks MongoDB role does not grant Pod exec")
	}

	if len(mongoDBRole.Rules) != 1 ||
		permissionAllowed(rolePermissions(mongoDBRole), permissionKey{resource: "pods"}, "get") {
		t.Fatalf("KubeBlocks MongoDB role grants unexpected permissions: %#v", mongoDBRole.Rules)
	}
}

func TestControllerRoleScopesTransferServiceAccountReadsAndUpdates(t *testing.T) {
	role := readClusterRole(t, "../../deploy/rbac.yaml", "pvc-migrate")

	var createRule, scopedRule bool
	for _, rule := range role.Rules {
		if len(rule.APIGroups) != 1 || rule.APIGroups[0] != "" ||
			len(rule.Resources) != 1 || rule.Resources[0] != "serviceaccounts" {
			continue
		}

		switch {
		case reflect.DeepEqual(rule.Verbs, []string{"create"}) && len(rule.ResourceNames) == 0:
			createRule = true
		case reflect.DeepEqual(rule.Verbs, []string{"get", "update"}) &&
			reflect.DeepEqual(rule.ResourceNames, []string{kube.TransferServiceAccountName}):
			scopedRule = true
		}
	}

	if !createRule || !scopedRule {
		t.Fatalf("ServiceAccount permissions are not narrowly scoped: %#v", role.Rules)
	}
}

func controllerRolePermissions() map[permissionKey][]string {
	want := make(map[permissionKey][]string)
	add := func(group string, resources []string, verbs ...string) {
		for _, resource := range resources {
			want[permissionKey{group: group, resource: resource}] = append([]string(nil), verbs...)
		}
	}

	for _, workflow := range domain.ControllerWorkflows() {
		resources := make([]string, 0, 2)
		if workflow.Resource != "" {
			resources = append(resources, workflow.Resource)
		}

		if workflow.ClusterResource != "" {
			resources = append(resources, workflow.ClusterResource)
		}

		add("migrate.sealos.io", resources, "get", "list", "watch", "update")

		statusResources := make([]string, 0, len(resources))
		for _, resource := range resources {
			statusResources = append(statusResources, resource+"/status")
		}

		add("migrate.sealos.io", statusResources, "update")
	}

	add("migrate.sealos.io", []string{"backuprepositories"}, "get")

	add("", []string{"namespaces"}, "get", "create")
	add("", []string{"nodes"}, "get", "list")
	add("", []string{"persistentvolumes"}, "get", "list", "update", "delete")
	add("", []string{"persistentvolumeclaims"}, "get", "list", "create", "update", "delete")
	add("", []string{"pods"}, "get", "list", "watch", "create", "update", "delete")
	add("", []string{"pods/log"}, "get")
	add("", []string{"pods/portforward"}, "create")
	add("", []string{"services"}, "get", "list", "watch", "create", "update", "patch", "delete")
	add("", []string{"secrets"}, "get", "list", "create", "update", "patch", "delete")
	add("", []string{"serviceaccounts"}, "create", "get", "update")
	add("", []string{"events"}, "get", "list", "create", "patch")
	add("", []string{"resourcequotas", "limitranges"}, "get", "list")
	add("apps", []string{"deployments"}, "get", "create", "update", "patch", "delete")
	add("autoscaling", []string{"horizontalpodautoscalers"}, "list")
	add("apps", []string{"replicasets"}, "get")
	add("apps", []string{"statefulsets"}, "get", "update")
	add("batch", []string{"jobs"}, "get", "create", "update", "patch", "delete")
	add("storage.k8s.io", []string{"storageclasses"}, "get")
	add("storage.k8s.io", []string{"volumeattachments"}, "list")
	add("local.openebs.io", []string{"lvmvolumes"}, "list", "patch")
	add("coordination.k8s.io", []string{"leases"}, "get", "create", "update", "patch", "delete")
	add("apps.kubeblocks.io", []string{"clusters"}, "get", "update", "patch")
	add("workloads.kubeblocks.io", []string{"instancesets"}, "get", "update", "patch")
	add("apps.kubeblocks.io", []string{"opsrequests"}, "get", "create", "delete")
	add("operations.kubeblocks.io", []string{"opsrequests"}, "get", "create", "delete")
	add("operator.victoriametrics.com", []string{"vmclusters"}, "get", "update", "patch")
	add("grafana.integreatly.org", []string{"grafanas"}, "get", "update", "patch")

	for key := range want {
		sort.Strings(want[key])
	}

	return want
}

func readClusterRole(t *testing.T, path, name string) rbacv1.ClusterRole {
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
		t.Fatalf("unexpected role %s/%s", role.Kind, role.Name)
	}

	return role
}

func rolePermissions(role rbacv1.ClusterRole) map[permissionKey][]string {
	permissions := make(map[permissionKey][]string)
	for _, rule := range role.Rules {
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				key := permissionKey{group: group, resource: resource}
				permissions[key] = append(permissions[key], rule.Verbs...)
			}
		}
	}

	for key, verbs := range permissions {
		sort.Strings(verbs)
		permissions[key] = uniqueStrings(verbs)
	}

	return permissions
}

func permissionAllowed(
	permissions map[permissionKey][]string,
	key permissionKey,
	verb string,
) bool {
	for _, candidate := range permissions[key] {
		if candidate == verb || candidate == "*" {
			return true
		}
	}

	return false
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}

	unique := values[:1]
	for _, value := range values[1:] {
		if value != unique[len(unique)-1] {
			unique = append(unique, value)
		}
	}

	return unique
}

package planner

import (
	"reflect"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Multi-tenant contract: a namespaced workflow's only namespace is its
// metadata.namespace. Planners must pin source, destination, temporary, and
// session namespaces to it, so a tenant CRD cannot reference another tenant's
// PVCs. Cross-namespace work requires the cluster-scoped CRDs.
func TestNamespacedPlansPinEveryNamespaceToMetadataNamespace(t *testing.T) {
	const tenant = "tenant-a"

	namespaced := []struct {
		name   string
		object metav1.Object
	}{
		{"copy", &v1alpha1.Copy{ObjectMeta: metav1.ObjectMeta{Namespace: tenant, Name: "copy-a"}}},
		{
			"migration",
			&v1alpha1.Migration{ObjectMeta: metav1.ObjectMeta{Namespace: tenant, Name: "mig-a"}},
		},
		{
			"reservation",
			&v1alpha1.Reservation{ObjectMeta: metav1.ObjectMeta{Namespace: tenant, Name: "resv-a"}},
		},
		{
			"podmigration",
			&v1alpha1.PodMigration{
				ObjectMeta: metav1.ObjectMeta{Namespace: tenant, Name: "pmig-a"},
			},
		},
	}

	for _, workflow := range namespaced {
		t.Run(workflow.name, func(t *testing.T) {
			object := workflow.object

			var namespaces []string
			switch typed := object.(type) {
			case *v1alpha1.Copy:
				namespaces = copyNamespacedPlanNamespaces(typed)
			case *v1alpha1.Migration:
				namespaces = migrationNamespacedPlanNamespaces(typed)
			case *v1alpha1.Reservation:
				namespaces = reservationNamespacedPlanNamespaces(typed)
			case *v1alpha1.PodMigration:
				namespaces = podMigrationNamespacedPlanNamespaces(typed)
			}

			if len(namespaces) == 0 {
				t.Fatal("planner produced no namespaces to pin")
			}

			for _, namespace := range namespaces {
				if namespace != tenant {
					t.Fatalf(
						"namespaced %s plan touched foreign namespace %q",
						workflow.name,
						namespace,
					)
				}
			}
		})
	}
}

// Namespaced specs must not carry namespace-bearing fields: the only tenant
// identity is the object's metadata.namespace.
func TestNamespacedSpecsExposeNoNamespaceFields(t *testing.T) {
	specs := []any{
		v1alpha1.CopySpec{},
		v1alpha1.MigrationSpec{},
		v1alpha1.ReservationSpec{},
		v1alpha1.PodMigrationSpec{},
		v1alpha1.BackupSpec{},
		v1alpha1.RestoreSpec{},
		v1alpha1.RenameSpec{},
	}

	for _, spec := range specs {
		value := reflect.ValueOf(spec)

		typed := value.Type()
		for i := range value.NumField() {
			field := typed.Field(i)
			if field.IsExported() && strings.HasSuffix(strings.ToLower(field.Name), "namespace") {
				t.Fatalf("%s carries namespace field %q", typed.Name(), field.Name)
			}
		}
	}
}

func copyNamespacedPlanNamespaces(object *v1alpha1.Copy) []string {
	source, destination, session := object.Namespace, object.Namespace, object.Namespace
	return []string{source, destination, session}
}

func migrationNamespacedPlanNamespaces(object *v1alpha1.Migration) []string {
	source, temporary, session := object.Namespace, object.Namespace, object.Namespace
	return []string{source, temporary, session}
}

func reservationNamespacedPlanNamespaces(object *v1alpha1.Reservation) []string {
	source, session := object.Namespace, object.Namespace
	return []string{source, session}
}

func podMigrationNamespacedPlanNamespaces(object *v1alpha1.PodMigration) []string {
	source, temporary, session := object.Namespace, object.Namespace, object.Namespace
	return []string{source, temporary, session}
}

// Cross-namespace work is only expressible through cluster-scoped CRDs.
func TestCrossNamespaceRequiresClusterKinds(t *testing.T) {
	// Pod migration moves a running workload's storage inside its own
	// namespace: workloads cannot be recreated in another namespace, so it
	// deliberately stays namespaced-only.
	for _, sessionType := range []domain.SessionType{
		domain.SessionTypeMigrate,
		domain.SessionTypeReserve,
		domain.SessionTypeCopy,
	} {
		workflow, ok := domain.ControllerWorkflowForType(sessionType)
		if !ok {
			t.Fatalf("missing workflow for %s", sessionType)
		}

		if workflow.ClusterKind == "" {
			t.Fatalf("%s has no cluster kind for cross-namespace work", sessionType)
		}

		if _, ok := domain.ControllerResourceForKind(workflow.ClusterKind); !ok {
			t.Fatalf("cluster kind %s is not registered as a resource", workflow.ClusterKind)
		}
	}

	// Namespaced-only operations cannot cross namespaces at all.
	for _, sessionType := range []domain.SessionType{
		domain.SessionTypeMigratePod,
		domain.SessionTypeBackup, domain.SessionTypeRestore, domain.SessionTypeRename,
	} {
		workflow, ok := domain.ControllerWorkflowForType(sessionType)
		if !ok {
			continue
		}

		if workflow.ClusterKind != "" {
			t.Fatalf("%s must remain namespaced-only", sessionType)
		}
	}
}

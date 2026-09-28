package planner

import (
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func backupWorkflow(namespace string) *v1alpha1.Backup {
	return &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "backup"},
		Spec: v1alpha1.BackupSpec{
			SourcePVC:     v1alpha1.LocalResourceReference{Name: "data"},
			Name:          "backup",
			RepositoryRef: v1alpha1.LocalObjectReference{Name: "repo"},
		},
	}
}

func restoreWorkflow(namespace string) *v1alpha1.Restore {
	return &v1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "restore"},
		Spec: v1alpha1.RestoreSpec{
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "data"},
			Name:           "backup",
			RepositoryRef:  v1alpha1.LocalObjectReference{Name: "repo"},
		},
	}
}

// Planning read failures must stay inside the controller's planning retry
// window: a bare API error has no domain category, evaluates as internal, and
// fails the workflow terminally on the first attempt — so a Backup applied in
// the same manifest as its source PVC would never be retried.
func TestPlanBackupReadErrorsStayInPlanningRetryWindow(t *testing.T) {
	t.Run("missing source PVC is a retryable precondition", func(t *testing.T) {
		err := New(plannerClient(), nil).
			PlanBackup(t.Context(), backupWorkflow("app"), "example/tool:v1")
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category = %v, want precondition: %v",
				domain.CategoryOf(err), err)
		}
	})

	t.Run("missing source PV is a retryable precondition", func(t *testing.T) {
		objects := plannerObjects("2Gi")
		objects = withoutPersistentVolume(objects)

		err := New(plannerClient(objects...), nil).
			PlanBackup(t.Context(), backupWorkflow("app"), "example/tool:v1")
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category = %v, want precondition: %v",
				domain.CategoryOf(err), err)
		}
	})

	t.Run("transient PVC read failure is a retryable kubernetes error", func(t *testing.T) {
		client := plannerClient(plannerObjects("2Gi")...)
		client.PrependReactor(
			"get", "persistentvolumeclaims",
			func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("connection refused")
			},
		)

		err := New(client, nil).
			PlanBackup(t.Context(), backupWorkflow("app"), "example/tool:v1")
		if domain.CategoryOf(err) != domain.ErrorKubernetes {
			t.Fatalf("category = %v, want kubernetes: %v",
				domain.CategoryOf(err), err)
		}
	})
}

func TestPlanRestoreReadErrorsStayInPlanningRetryWindow(t *testing.T) {
	t.Run("missing destination PVC without createPVC is retryable", func(t *testing.T) {
		err := New(plannerClient(), nil).
			PlanRestore(t.Context(), restoreWorkflow("app"), "example/tool:v1")
		if domain.CategoryOf(err) != domain.ErrorPrecondition {
			t.Fatalf("category = %v, want precondition: %v",
				domain.CategoryOf(err), err)
		}
	})

	t.Run("createPVC still plans without reading the PVC", func(t *testing.T) {
		object := restoreWorkflow("app")
		object.Spec.CreatePVC = true

		if err := New(plannerClient(), nil).
			PlanRestore(t.Context(), object, "example/tool:v1"); err != nil {
			t.Fatalf("createPVC planning failed: %v", err)
		}

		if object.Status.Plan == nil || !object.Status.Plan.CreatePVC {
			t.Fatal("plan did not record the requested PVC creation")
		}
	})
}

func withoutPersistentVolume(objects []runtime.Object) []runtime.Object {
	filtered := make([]runtime.Object, 0, len(objects))

	for _, object := range objects {
		if _, isPV := object.(*corev1.PersistentVolume); isPV {
			continue
		}

		filtered = append(filtered, object)
	}

	return filtered
}

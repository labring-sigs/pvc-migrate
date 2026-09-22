package app

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func pendingCopy(
	t *testing.T,
	phase v1alpha1.WorkflowPhase,
) (*CopyExecutor, *v1alpha1.Copy, *fake.Clientset) {
	t.Helper()

	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "app", UID: "workflow"},
		Spec: v1alpha1.CopySpec{Volumes: []v1alpha1.VolumeRequest{
			{SourcePVC: v1alpha1.LocalResourceReference{Name: "data"}},
		}},
		Status: v1alpha1.CopyStatus{WorkflowStatus: v1alpha1.WorkflowStatus{
			Phase: phase, ResumeFrom: domain.PhasePlanned,
		}},
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(object).WithObjects(object).Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object, err = store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	resources := fake.NewClientset()
	executor := NewCopyExecutor(resources, store,
		&fakeSessionLocker{lock: &fakeSessionLock{}}, nil, CopyExecutorConfig{})

	return executor, object, resources
}

func TestUnplannedLifecycleNeverTouchesStorage(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{domain.PhasePlanned, domain.PhaseFailed, domain.PhaseAborted} {
		t.Run(string(phase), func(t *testing.T) {
			executor, object, resources := pendingCopy(t, phase)
			if err := executor.Validate(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			for range 2 {
				if err := executor.ValidateAbort(object); err != nil {
					t.Fatal(err)
				}

				if err := executor.Abort(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}

			if object.Status.Phase != domain.PhaseAborted {
				t.Fatal("abort did not stop request")
			}

			options := CopyCleanupOptions{Finalize: true, DeleteSession: true}
			if err := executor.ValidateCleanup(t.Context(), object, options); err != nil {
				t.Fatal(err)
			}

			if err := executor.Cleanup(t.Context(), object, options); err != nil {
				t.Fatal(err)
			}

			if _, err := executor.store.Load(
				t.Context(),
				crclient.ObjectKeyFromObject(object),
			); !apierrors.IsNotFound(
				err,
			) {
				t.Fatalf("workflow was not deleted: %v", err)
			}

			if len(resources.Actions()) != 0 {
				t.Fatalf("unplanned operation touched resources: %v", resources.Actions())
			}
		})
	}
}

func TestUnplannedLifecycleRejectsPlanningRace(t *testing.T) {
	for _, planned := range []bool{false, true} {
		executor, object, resources := pendingCopy(t, domain.PhasePlanned)

		latest := object.DeepCopy()
		if planned {
			latest.Status.Plan = &v1alpha1.CopyPlan{TargetNode: "node"}
		} else {
			latest.Status.Phase = domain.PhaseAborted
		}

		if err := executor.store.Save(t.Context(), latest); err != nil {
			t.Fatal(err)
		}

		for _, err := range []error{
			executor.Abort(t.Context(), object),
			executor.Cleanup(t.Context(), object, CopyCleanupOptions{Finalize: true, DeleteSession: true}),
		} {
			if domain.CategoryOf(err) != domain.ErrorConflict {
				t.Fatalf("expected conflict, got %v", err)
			}
		}

		if len(resources.Actions()) != 0 {
			t.Fatal("stale request touched resources")
		}
	}
}

package controller

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type moveControllerLocker struct{ namespaces []string }

func (l *moveControllerLocker) AcquireSessionLock(
	_ context.Context,
	namespace, _ string,
) (kube.SessionLock, error) {
	l.namespaces = append(l.namespaces, namespace)
	return &runnerSessionLock{}, nil
}

func newMoveReconcileFixture(
	t *testing.T,
) (*MoveReconciler, crclient.Client, reconcile.Request, *moveControllerLocker) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Move{
		ObjectMeta: metav1.ObjectMeta{Name: "move", UID: "workflow", Generation: 1},
		Spec: v1alpha1.MoveSpec{
			SourceNamespace: "data", DestinationNamespace: "archive", SessionNamespace: "system",
			SourcePVC: v1alpha1.LocalResourceReference{Name: "source"},
		},
	}
	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		Build()

	store, err := kube.NewCRDWorkflowStore(
		client,
		func() *v1alpha1.Move { return &v1alpha1.Move{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	locker := &moveControllerLocker{}
	r := &MoveReconciler{
		store:  store,
		client: fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}}),
		locker: locker,
		active: &sync.Map{},
		checkCollision: func(_ context.Context, name string, namespaces []string) error {
			if name != "move" || !slices.Equal(namespaces, []string{"data", "archive", "system"}) {
				t.Fatalf("collision check lost namespace roles: %s; %v", name, namespaces)
			}
			return nil
		},
	}

	return r, client, reconcile.Request{
		NamespacedName: crclient.ObjectKey{Name: object.Name},
	}, locker
}

func TestMoveReconcilePlanningFailureRequiresExplicitResume(t *testing.T) {
	r, _, request, _ := newMoveReconcileFixture(t)
	plans := 0
	r.planner = func(context.Context, *v1alpha1.Move, string) (*domain.PVCIdentityReport, error) {
		plans++
		return nil, errors.New("source is busy")
	}

	entry := &kindWorkflowReconciler{
		parent: &WorkflowReconciler{move: r},
		kind:   domain.ControllerKindMove,
	}
	for range 2 {
		if _, err := entry.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}

	object, err := r.store.Load(t.Context(), request.NamespacedName)
	if err != nil || plans != 1 || object.Status.Phase != domain.PhaseFailed {
		t.Fatalf("planning failure was not suspended: %v; %+v", err, object)
	}

	executor := app.NewMoveExecutor(r.client, r.store, r.locker, "system")
	if err := executor.RequestResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := entry.Reconcile(t.Context(), request); err != nil || plans != 2 {
		t.Fatalf("explicit resume did not retry planning: %v", err)
	}
}

func TestMoveReconcileDeletionKeepsPersistedStorageNamespace(t *testing.T) {
	r, client, request, locker := newMoveReconcileFixture(t)
	r.planner = func(_ context.Context, object *v1alpha1.Move, namespace string) (*domain.PVCIdentityReport, error) {
		if namespace != "system" {
			t.Fatal("planner received wrong storage namespace")
		}

		object.Status.Plan = &v1alpha1.MovePlan{
			SourceNamespace: "data", DestinationNamespace: "archive", SessionNamespace: "system",
			Identity: v1alpha1.MoveIdentity{
				SourcePVC:      v1alpha1.LocalResourceReference{Name: "source", UID: "source-uid"},
				SourcePV:       v1alpha1.LocalResourceReference{Name: "pv", UID: "pv-uid"},
				DestinationPVC: v1alpha1.LocalResourceReference{Name: "source"},
				SourceTemplate: v1alpha1.PVCSourceTemplate{
					ReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				},
			},
		}

		return &domain.PVCIdentityReport{PlanSummary: domain.PlanSummary{Ready: true}}, nil
	}

	result, err := r.Reconcile(t.Context(), request)
	if err != nil || result.RequeueAfter == 0 {
		t.Fatalf("plan was not scheduled: %+v; %v", result, err)
	}

	object, err := r.store.Load(t.Context(), request.NamespacedName)
	if err != nil {
		t.Fatal(err)
	}

	object.Status.Phase = domain.PhaseAborted
	if err := r.store.Save(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	object.Spec.SessionNamespace = "unrelated"

	object.Generation = 2
	if err := client.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := client.Delete(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if _, err := r.store.Load(t.Context(), request.NamespacedName); !apierrors.IsNotFound(err) {
		t.Fatalf("deletion did not release protection: %v", err)
	}

	if len(locker.namespaces) != 2 || locker.namespaces[0] != "system" ||
		locker.namespaces[1] != "system" {
		t.Fatalf("spec edit redirected lease: %v", locker.namespaces)
	}
}

package controller

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestWorkflowQueuesAdmitAbortRequests(t *testing.T) {
	previous := &v1alpha1.ClusterCopy{
		ObjectMeta: metav1.ObjectMeta{Name: "copy"},
		Status: v1alpha1.ClusterCopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopying},
		},
	}
	current := previous.DeepCopy()
	current.Annotations = map[string]string{
		kube.WorkflowAbortRequestedAnnotation: "2026-10-08T00:00:00Z",
	}

	cancelled := false
	cancel := func(crclient.Object) { cancelled = true }

	update := event.UpdateEvent{ObjectOld: previous, ObjectNew: current}
	if !workflowQueuePredicate(false, cancel).Update(update) {
		t.Fatal("abort request did not wake execution")
	}

	if !cancelled {
		t.Fatal("abort request did not interrupt the active reconcile")
	}

	pausing := previous.DeepCopy()
	pausing.Status.Phase = domain.PhasePausing
	pausingCurrent := pausing.DeepCopy()
	pausingCurrent.Annotations = current.Annotations

	if !workflowRecoveryQueuePredicate(cancel).Update(event.UpdateEvent{
		ObjectOld: pausing, ObjectNew: pausingCurrent,
	}) {
		t.Fatal("abort request did not wake recovery")
	}

	// Consuming the request must admit the follow-up reconcile without
	// interrupting anything: the abort already ran.
	consumeCancelled := false

	consume := func(crclient.Object) { consumeCancelled = true }
	if !workflowEventPredicate(consume).Update(event.UpdateEvent{
		ObjectOld: current, ObjectNew: previous,
	}) {
		t.Fatal("consumed abort request did not admit its reconcile")
	}

	if consumeCancelled {
		t.Fatal("consuming an abort request interrupted a reconcile")
	}
}

func TestAbortRequestReconcileConverges(t *testing.T) {
	object := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "copy",
			Namespace: "tenant",
			UID:       "workflow",
			Annotations: map[string]string{
				kube.WorkflowAbortRequestedAnnotation: "2026-10-08T00:00:00Z",
			},
		},
		Spec: v1alpha1.CopySpec{Volumes: []v1alpha1.VolumeRequest{
			{SourcePVC: v1alpha1.LocalResourceReference{Name: "a"}},
		}},
		Status: v1alpha1.CopyStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseWarmCopying},
			Plan: &v1alpha1.CopyPlan{
				Volumes: []v1alpha1.VolumeSpec{
					{
						SourcePVC: v1alpha1.LocalResourceReference{
							Name: "a",
							UID:  types.UID("source-a"),
						},
						SourcePV: v1alpha1.LocalResourceReference{
							Name: "pv-a",
							UID:  types.UID("pv-a"),
						},
						DestinationPVC: v1alpha1.LocalResourceReference{Name: "reserved-a"},
						SourceCapacity: "1Gi",
						Capacity:       "1Gi",
						StorageClass:   "storage",
						VolumeMode:     corev1.PersistentVolumeFilesystem,
						AccessModes:    []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					},
				},
				ToolImage:      "example/tool:v1",
				Strategies:     []string{domain.StrategyMount},
				VerifyChecksum: true,
			},
			Volumes: []v1alpha1.CopyVolumeStatus{{
				VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
					SourcePVCName: "a",
					DestinationPVC: &v1alpha1.LocalResourceReference{
						Name: "reserved-a",
						UID:  types.UID("dest-a"),
					},
					DestinationPV: &v1alpha1.LocalResourceReference{
						Name: "pv-reserved-a",
						UID:  types.UID("pv-dest-a"),
					},
					Reserved: true,
				},
			}},
		},
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(object).
		WithObjects(object).
		Build()

	r := NewWorkflowReconciler().WithSupportedKinds([]domain.ControllerKind{domain.ControllerKindCopy})
	if err := r.configureTransferControllers(
		client,
		ManagerOptions{
			KubernetesClient: fake.NewClientset(
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}},
			),
			NamespacedCopyPlanner: func(
				context.Context, *v1alpha1.Copy, string,
			) (*domain.TransferPlan, error) {
				t.Fatal("abort request must not replan")
				return nil, nil
			},
		},
		&moveControllerLocker{},
		"trusted/tool:v1",
		nil,
	); err != nil {
		t.Fatal(err)
	}

	r.namespacedCopy.checkCollision = func(context.Context, string, []string) error {
		return nil
	}

	entry := &kindWorkflowReconciler{parent: r, kind: domain.ControllerKindCopy}

	request := reconcile.Request{NamespacedName: crclient.ObjectKeyFromObject(object)}
	if _, err := entry.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	if err := client.Get(t.Context(), request.NamespacedName, object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseAborted {
		t.Fatalf("abort request did not converge: %s", object.Status.Phase)
	}

	if kube.WorkflowAbortRequested(object) {
		t.Fatal("handled abort request was not consumed")
	}

	if _, err := entry.Reconcile(t.Context(), request); err != nil {
		t.Fatal(err)
	}
}

func TestAbortRequestTransientClassification(t *testing.T) {
	contention := domain.WrapError(
		domain.ErrorConflict,
		"acquire session lock",
		"session copy is already being changed",
		kube.ErrSessionLockContention,
	)
	if !abortRequestTransient(contention) {
		t.Fatal("session lock contention must retry")
	}

	conflict := apierrors.NewConflict(
		schema.GroupResource{Group: "migrate.sealos.io", Resource: "copies"},
		"copy",
		errors.New("object was modified"),
	)
	if !abortRequestTransient(conflict) {
		t.Fatal("optimistic concurrency conflict must retry")
	}

	if !abortRequestTransient(
		domain.NewError(domain.ErrorKubernetes, "abort", "api server unreachable"),
	) {
		t.Fatal("kubernetes failures must retry")
	}

	if abortRequestTransient(
		domain.NewError(domain.ErrorPrecondition, "abort", "workflow already aborted"),
	) {
		t.Fatal("durable rejections must not retry")
	}

	if abortRequestTransient(
		domain.NewError(domain.ErrorValidation, "abort", "plan is malformed"),
	) {
		t.Fatal("validation rejections must not retry")
	}
}

func TestAbortRequestTreatsConvergingToolAsTransient(t *testing.T) {
	if !abortRequestTransient(kube.ErrTransferToolStillExists) {
		t.Fatal("a tool still being converged by the interrupted run must retry")
	}

	wrapped := domain.WrapError(
		domain.ErrorPrecondition,
		"finalize workflow",
		"transfer Pod tenant/rclone-a still exists; wait for transfer cleanup",
		kube.ErrTransferToolStillExists,
	)
	if !abortRequestTransient(wrapped) {
		t.Fatal("the classified still-exists rejection lost its sentinel")
	}

	genuine := domain.NewError(
		domain.ErrorPrecondition,
		"abort backup",
		"completed backup cannot be aborted",
	)
	if abortRequestTransient(genuine) {
		t.Fatal("durable rejections must not retry")
	}
}

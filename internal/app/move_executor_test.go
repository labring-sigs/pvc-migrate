package app

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type moveCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.Move]
	object  *v1alpha1.Move
	failure error
	deleted bool
}

func (s *moveCheckpointStore) Load(context.Context, crclient.ObjectKey) (*v1alpha1.Move, error) {
	return s.object.DeepCopy(), nil
}

func (s *moveCheckpointStore) Save(ctx context.Context, object *v1alpha1.Move) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.failure != nil {
		return s.failure
	}

	s.object = object.DeepCopy()

	return nil
}

func (s *moveCheckpointStore) Delete(ctx context.Context, _ *v1alpha1.Move) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.deleted = true

	return nil
}

func plannedMoveObject() *v1alpha1.Move {
	return &v1alpha1.Move{
		ObjectMeta: metav1.ObjectMeta{Name: "move", UID: "workflow", ResourceVersion: "1"},
		Spec: v1alpha1.MoveSpec{
			SourceNamespace: "app", DestinationNamespace: "archive", SessionNamespace: "system",
			SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
		},
		Status: v1alpha1.MoveStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.MovePlan{
				SourceNamespace: "app", DestinationNamespace: "archive", SessionNamespace: "system",
				Identity: v1alpha1.MoveIdentity{
					SourcePVC: v1alpha1.LocalResourceReference{
						Name: "data",
						UID:  "source-uid",
					},
					SourcePV:       v1alpha1.LocalResourceReference{Name: "pv", UID: "pv-uid"},
					DestinationPVC: v1alpha1.LocalResourceReference{Name: "data"},
					SourceTemplate: v1alpha1.PVCSourceTemplate{
						ReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
					},
				},
			},
		},
	}
}

func TestMoveExecutorRecoversCrossNamespaceIdentity(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "move", true: "rollback"}[rollback], func(t *testing.T) {
			object := plannedMoveObject()
			object.Status.Phase = domain.PhaseMoving
			namespace := "archive"

			wantPhase := domain.PhaseCompleted
			if rollback {
				object.Status.Phase = domain.PhaseRollingBack
				object.Status.Activation.ActivePVC = &v1alpha1.ObjectReference{
					Namespace: "archive",
					Name:      "data",
					UID:       "previous-destination",
				}
				namespace, wantPhase = "app", domain.PhaseRolledBack
			}

			capacity := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "data",
					Namespace:   namespace,
					UID:         "recovered-uid",
					Annotations: map[string]string{kube.SessionKey: object.Name},
					Labels: map[string]string{
						kube.SessionKey:        object.Name,
						kube.ResourceRoleLabel: kube.ResourceRoleActive,
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					VolumeName: "pv",
					Resources:  corev1.VolumeResourceRequirements{Requests: capacity},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Phase:    corev1.ClaimBound,
					Capacity: capacity,
				},
			}
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv",
					UID:  "pv-uid",
					Labels: map[string]string{
						kube.SessionKey:        object.Name,
						kube.ResourceRoleLabel: kube.ResourceRoleActive,
					},
				},
				Spec: corev1.PersistentVolumeSpec{
					Capacity:                      capacity,
					PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
					ClaimRef: &corev1.ObjectReference{
						Name:      pvc.Name,
						Namespace: namespace,
						UID:       pvc.UID,
					},
				},
				Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
			}
			object.Status.Plan.Identity.SourceTemplate.Spec = *pvc.Spec.DeepCopy()
			client := fake.NewClientset(pvc, pv)
			store := &moveCheckpointStore{object: object.DeepCopy()}

			executor := NewMoveExecutor(
				client,
				store,
				&fakeSessionLocker{lock: &fakeSessionLock{}},
				"system",
			)
			if err := executor.Run(t.Context(), object); err != nil {
				t.Fatal(err)
			}

			active := object.Status.Activation.ActivePVC
			if object.Status.Phase != wantPhase || active == nil || active.Namespace != namespace ||
				active.UID != pvc.UID {
				t.Fatalf("recovery lost namespace or identity: %+v", object.Status)
			}

			for _, action := range client.Actions() {
				if action.GetResource().Resource == "persistentvolumeclaims" &&
					(action.GetVerb() == "create" || action.GetVerb() == "delete") {
					t.Fatal("recovery repeated PVC replacement")
				}
			}
		})
	}
}

func TestMoveExecutorRejectsActivePVCInWrongNamespace(t *testing.T) {
	object := plannedMoveObject()
	object.Status.Phase = domain.PhaseCompleted

	object.Status.Activation.ActivePVC = &v1alpha1.ObjectReference{
		Namespace: "app",
		Name:      "data",
		UID:       "target-uid",
	}
	if err := validateMoveObject(object, "system"); err == nil {
		t.Fatal("source namespace was accepted as completed destination")
	}
}

func TestMoveExecutorAllowsSameNamespaceWithDistinctPVCNames(t *testing.T) {
	object := plannedMoveObject()
	object.Spec.DestinationNamespace = object.Spec.SourceNamespace
	object.Spec.DestinationPVC = &v1alpha1.LocalResourceReference{Name: "renamed"}
	object.Status.Plan.DestinationNamespace = object.Status.Plan.SourceNamespace

	object.Status.Plan.Identity.DestinationPVC = *object.Spec.DestinationPVC
	if err := validateMoveObject(object, "system"); err != nil {
		t.Fatal(err)
	}
}

func TestMoveExecutorDeletionIgnoresLaterSpecRetargeting(t *testing.T) {
	object := plannedMoveObject()
	object.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	object.Generation, object.Status.ObservedGeneration = 2, 1
	object.Spec.SourceNamespace, object.Spec.SessionNamespace = "unrelated", "unrelated"
	object.Status.Phase = domain.PhaseAborted
	client := fake.NewClientset()
	store := &moveCheckpointStore{object: object.DeepCopy()}

	executor := NewMoveExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"system",
	)
	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !store.deleted {
		t.Fatal("deletion did not release workflow protection")
	}

	for _, action := range client.Actions() {
		if action.GetNamespace() == "unrelated" {
			t.Fatal("spec edit redirected deletion")
		}
	}
}

func TestMoveExecutorAbortWriteFailurePreservesCheckpoint(t *testing.T) {
	object := plannedMoveObject()
	object.Status = v1alpha1.MoveStatus{}
	failure := errors.New("checkpoint write failed")
	store := &moveCheckpointStore{object: object.DeepCopy(), failure: failure}

	executor := NewMoveExecutor(nil, store, &fakeSessionLocker{lock: &fakeSessionLock{}}, "system")
	if err := executor.Abort(
		t.Context(),
		object,
	); !errors.Is(err, failure) ||
		object.Status.Phase != "" {
		t.Fatalf("failed abort advanced state: %v; %+v", err, object.Status)
	}

	store.failure = nil

	if err := executor.Abort(
		t.Context(),
		object,
	); err != nil ||
		object.Status.Phase != domain.PhaseAborted {
		t.Fatalf("abort could not retry: %v; %+v", err, object.Status)
	}
}

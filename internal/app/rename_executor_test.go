package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type renameCheckpointStore struct {
	kube.WorkflowStore[*v1alpha1.Rename]
	object       *v1alpha1.Rename
	failure      error
	writes       int
	deleted      bool
	loadErr      error
	failurePhase v1alpha1.WorkflowPhase
}

func (s *renameCheckpointStore) Delete(ctx context.Context, _ *v1alpha1.Rename) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.deleted = true

	return nil
}

func (s *renameCheckpointStore) Save(ctx context.Context, object *v1alpha1.Rename) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.writes++
	if s.failure != nil && (s.failurePhase == "" || s.failurePhase == object.Status.Phase) {
		return s.failure
	}

	s.object = object.DeepCopy()

	return nil
}

func (s *renameCheckpointStore) Load(
	context.Context,
	crclient.ObjectKey,
) (*v1alpha1.Rename, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}

	return s.object.DeepCopy(), nil
}

func plannedRenameObject() *v1alpha1.Rename {
	return &v1alpha1.Rename{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "rename",
			Namespace:       "app",
			UID:             "workflow",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.RenameSpec{
			SourcePVC:      v1alpha1.LocalResourceReference{Name: "source"},
			DestinationPVC: v1alpha1.LocalResourceReference{Name: "target"},
		},
		Status: v1alpha1.RenameStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
			Plan: &v1alpha1.RenamePlan{PVCIdentityFields: v1alpha1.PVCIdentityFields{
				SourcePVC:      v1alpha1.LocalResourceReference{Name: "source", UID: "source-uid"},
				SourcePV:       v1alpha1.LocalResourceReference{Name: "pv", UID: "pv-uid"},
				DestinationPVC: v1alpha1.LocalResourceReference{Name: "target"},
				SourceTemplate: v1alpha1.PVCSourceTemplate{
					ReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				},
			}},
		},
	}
}

func TestRenameFailureCheckpointSurvivesCancellationAndRetries(t *testing.T) {
	object := plannedRenameObject()
	object.Status.Phase = domain.PhaseRenaming
	before := object.Status.DeepCopy()
	writeErr := errors.New("checkpoint unavailable")
	store := &renameCheckpointStore{failure: writeErr}
	executor := NewRenameExecutor(nil, store, nil, "sessions")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	cause := context.Canceled

	err := executor.fail(ctx, object, cause)
	if !errors.Is(err, cause) || !errors.Is(err, writeErr) ||
		!reflect.DeepEqual(object.Status, *before) {
		t.Fatalf("failed checkpoint corrupted retry state: %v; %+v", err, object.Status)
	}

	store.failure = nil

	if err := executor.fail(ctx, object, cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}

	if store.writes != 2 || store.object.Status.Phase != domain.PhaseRenaming ||
		store.object.Status.ResumeFrom != "" || store.object.Status.Message != cause.Error() {
		t.Fatalf("lost recovery checkpoint: %+v", store.object)
	}
}

func TestRenameLockRejectsReplacedWorkflowBeforeResourceOperations(t *testing.T) {
	object := plannedRenameObject()
	replacement := object.DeepCopy()
	replacement.UID = "replacement"
	store := &renameCheckpointStore{object: replacement}

	executor := NewRenameExecutor(
		nil,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"sessions",
	)
	if err := executor.Run(t.Context(), object); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("replaced workflow reached execution: %v", err)
	}

	if store.writes != 0 {
		t.Fatal("stale workflow wrote a checkpoint")
	}
}

func TestRenameRejectsUnrelatedOrIncompleteCheckpoint(t *testing.T) {
	for _, mutate := range []struct {
		name  string
		apply func(*v1alpha1.Rename)
	}{
		{"foreign phase", func(o *v1alpha1.Rename) { o.Status.Phase = domain.PhaseWarmCopying }},
		{"missing plan during execution", func(o *v1alpha1.Rename) {
			o.Status.Plan = nil
			o.Status.Phase = domain.PhaseRenaming
		}},
		{"checkpoint without plan", func(o *v1alpha1.Rename) {
			o.Status.Plan = nil
			o.Status.Activation.ActivatedAt = &o.Status.UpdatedAt
		}},
		{"missing activation", func(o *v1alpha1.Rename) { o.Status.Phase = domain.PhaseCompleted }},
		{"wrong source UID", func(o *v1alpha1.Rename) { o.Spec.SourcePVC.UID = "replaced" }},
		{"unrelated active PVC", func(o *v1alpha1.Rename) {
			o.Status.Activation.ActivePVC = &v1alpha1.LocalResourceReference{Name: "unrelated", UID: "other"}
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			object := plannedRenameObject()
			mutate.apply(object)

			if err := validateRenameObject(object); err == nil {
				t.Fatal("invalid checkpoint accepted")
			}
		})
	}
}

func TestRenameTransitionDoesNotAdvanceAfterFenceLoss(t *testing.T) {
	object := plannedRenameObject()
	before := object.Status.DeepCopy()
	lost := errors.New("lease lost")
	ctx := withHeldSessionLock(
		t.Context(), heldSessionLock{lock: &fakeSessionLock{err: lost}},
	)
	store := &renameCheckpointStore{}

	executor := &RenameExecutor{store: store, now: time.Now}
	if err := executor.transition(
		ctx,
		object,
		domain.PhaseRenaming,
		"start",
	); !errors.Is(
		err,
		lost,
	) {
		t.Fatal(err)
	}

	if store.writes != 0 || !reflect.DeepEqual(object.Status, *before) {
		t.Fatal("lost fence advanced the workflow")
	}
}

func TestRenameAbortBeforeControllerPlanning(t *testing.T) {
	object := plannedRenameObject()
	object.Status = v1alpha1.RenameStatus{}
	store := &renameCheckpointStore{object: object.DeepCopy()}

	executor := NewRenameExecutor(nil, store, &fakeSessionLocker{lock: &fakeSessionLock{}}, "app")
	if err := executor.ValidateResume(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if err := executor.Abort(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.writes != 1 || store.object.Status.Phase != domain.PhaseAborted ||
		store.object.Status.StartedAt.IsZero() {
		t.Fatalf("unplanned abort lost its initial checkpoint: %+v", store.object.Status)
	}
}

func TestRenameDeletionUsesPersistedPlanDespiteSpecMutation(t *testing.T) {
	object := plannedRenameObject()
	object.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	object.Generation = 2
	object.Status.ObservedGeneration = 1
	object.Spec.SourcePVC.Name = "unrelated"
	object.Spec.DestinationPVC.Name = "also-unrelated"
	object.Status.Phase = domain.PhaseCompleted
	object.Status.Activation.ActivePVC = &v1alpha1.LocalResourceReference{
		Name: "target", UID: "target-uid",
	}
	store := &renameCheckpointStore{object: object.DeepCopy()}
	client := fake.NewClientset()

	executor := NewRenameExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"app",
	)
	if err := executor.Run(t.Context(), object); err == nil {
		t.Fatal("normal execution accepted a deleting workflow")
	}

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !store.deleted {
		t.Fatal("finalization did not release the workflow")
	}

	for _, action := range client.Actions() {
		if get, ok := action.(interface{ GetName() string }); ok &&
			(get.GetName() == "unrelated" || get.GetName() == "also-unrelated") {
			t.Fatal("spec mutation redirected deletion")
		}
	}
}

func TestRenameDeletionBeforePlanningDoesNotAccessResources(t *testing.T) {
	object := plannedRenameObject()
	object.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	object.Status = v1alpha1.RenameStatus{}
	object.Spec = v1alpha1.RenameSpec{}
	store := &renameCheckpointStore{object: object.DeepCopy()}

	executor := NewRenameExecutor(nil, store, &fakeSessionLocker{lock: &fakeSessionLock{}}, "app")
	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !store.deleted || object.Status.Phase != domain.PhaseAborted {
		t.Fatalf("unplanned deletion did not finish: %+v", object.Status)
	}
}

type renameDeletionLock struct {
	fakeSessionLock
	deleteErr error
	deletes   int
}

func (l *renameDeletionLock) Delete(context.Context) error {
	l.deletes++
	return l.deleteErr
}

func TestRenameDeletionRemovesWorkflowBeforeLeaseCleanup(t *testing.T) {
	object := plannedRenameObject()
	object.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	object.Status = v1alpha1.RenameStatus{}
	store := &renameCheckpointStore{object: object.DeepCopy()}
	failure := errors.New("lease cleanup failed")
	lock := &renameDeletionLock{deleteErr: failure}

	executor := NewRenameExecutor(nil, store, &fakeSessionLocker{lock: lock}, "app")
	if err := executor.FinalizeDeleted(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if !store.deleted || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("protected workflow delete did not complete before lease cleanup")
	}
	if lock.deletes != 1 {
		t.Fatalf("lease cleanup was not attempted after workflow deletion: %d", lock.deletes)
	}
}

func TestRenameDeletionRemovesLeaseIfWorkflowDisappearedWhileLocking(t *testing.T) {
	object := plannedRenameObject()
	object.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	store := &renameCheckpointStore{
		loadErr: apierrors.NewNotFound(
			schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "renames"},
			object.Name,
		),
	}
	lock := &renameDeletionLock{}

	executor := NewRenameExecutor(nil, store, &fakeSessionLocker{lock: lock}, "app")
	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if lock.deletes != 1 || store.writes != 0 || store.deleted {
		t.Fatal("absent workflow left a recreated Lease or entered resource cleanup")
	}
}

func TestRenameRecoversCompletedRebindAfterCheckpointWriteFailure(t *testing.T) {
	object := plannedRenameObject()
	object.Status.Phase = domain.PhaseRenaming
	capacity := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "target",
			Namespace:   "app",
			UID:         "target-uid",
			Annotations: map[string]string{kube.SessionKey: object.Name},
			Labels: map[string]string{
				kube.SessionKey:        object.Name,
				kube.ResourceRoleLabel: kube.ResourceRoleActive,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv", Resources: corev1.VolumeResourceRequirements{Requests: capacity},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: capacity},
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
				Namespace: pvc.Namespace,
				UID:       pvc.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	object.Status.Plan.SourceTemplate.Spec = *pvc.Spec.DeepCopy()
	client := fake.NewClientset(pvc, pv)
	failure := errors.New("completion checkpoint unavailable")
	store := &renameCheckpointStore{
		object:       object.DeepCopy(),
		failure:      failure,
		failurePhase: domain.PhaseCompleted,
	}

	executor := NewRenameExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"app",
	)
	if err := executor.Run(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseRenaming ||
		object.Status.Activation.ActivePVC != nil {
		t.Fatalf("completion failure advanced durable identity: %+v", object.Status)
	}

	store.failure = nil

	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseCompleted ||
		object.Status.Activation.ActivePVC.UID != pvc.UID {
		t.Fatalf("rebind was not recovered: %+v", object.Status)
	}

	for _, action := range client.Actions() {
		if action.GetResource().Resource == "persistentvolumeclaims" &&
			(action.GetVerb() == "create" || action.GetVerb() == "delete") {
			t.Fatal("checkpoint retry repeated PVC replacement")
		}
	}
}

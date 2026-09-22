package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func identityStorageFixture(
	t *testing.T,
	name string,
) (*fake.Clientset, corev1.PersistentVolumeClaimSpec) {
	t.Helper()

	const namespace = "app"

	capacity := corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: "source-uid"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: capacity},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: capacity},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: "pv-uid"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      capacity,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Namespace: namespace,
				Name:      name,
				UID:       pvc.UID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	client := fake.NewClientset(pvc, pv)
	claims := corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")
	volumes := corev1.SchemeGroupVersion.WithResource("persistentvolumes")
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			deleted := testutil.MustType[ktesting.DeleteAction](t, action)

			claim, err := client.Tracker().Get(claims, deleted.GetNamespace(), deleted.GetName())
			if err != nil {
				return true, nil, err
			}

			volume, err := client.Tracker().
				Get(volumes, "", testutil.MustType[*corev1.PersistentVolumeClaim](t, claim).Spec.VolumeName)
			if err != nil {
				return true, nil, err
			}

			pv := testutil.MustType[*corev1.PersistentVolume](t, volume)
			pv.Status.Phase = corev1.VolumeReleased

			return false, nil, client.Tracker().Update(volumes, pv, "")
		},
	)

	sequence := 0
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create := testutil.MustType[ktesting.CreateAction](t, action)
			claim := testutil.MustType[*corev1.PersistentVolumeClaim](t, create.GetObject())

			options := testutil.MustType[interface{ GetCreateOptions() metav1.CreateOptions }](
				t,
				action,
			).GetCreateOptions()
			if len(options.DryRun) != 0 {
				return true, claim, nil
			}

			sequence++
			claim.UID = types.UID(fmt.Sprintf("created-%d", sequence))
			claim.Status.Phase = corev1.ClaimBound
			claim.Status.Capacity = capacity.DeepCopy()

			volume, err := client.Tracker().Get(volumes, "", claim.Spec.VolumeName)
			if err != nil {
				return true, nil, err
			}

			pv := testutil.MustType[*corev1.PersistentVolume](t, volume)
			pv.Status.Phase = corev1.VolumeBound
			pv.Spec.ClaimRef = &corev1.ObjectReference{
				Namespace: claim.Namespace,
				Name:      claim.Name,
				UID:       claim.UID,
			}

			return false, nil, client.Tracker().Update(volumes, pv, "")
		},
	)

	return client, *pvc.Spec.DeepCopy()
}

func TestRenameExecutorRunRollbackAndFinalize(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{domain.PhasePlanned, domain.PhaseRenaming} {
		t.Run(string(phase), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			object := plannedRenameObject()
			client, spec := identityStorageFixture(t, "source")
			object.Status.Plan.SourceTemplate.Spec = spec
			object.Status.Phase = phase
			store := &renameCheckpointStore{object: object.DeepCopy()}

			executor := NewRenameExecutor(
				client,
				store,
				&fakeSessionLocker{lock: &fakeSessionLock{}},
				"system",
			)
			if err := executor.Run(ctx, object); err != nil {
				t.Fatal(err)
			}

			active := object.Status.Activation.ActivePVC
			if object.Status.Phase != domain.PhaseCompleted || active.Name != "target" ||
				active.UID != "created-1" {
				t.Fatalf("rename checkpoint: %+v", object.Status)
			}

			if err := executor.ValidateRollback(ctx, object); err != nil {
				t.Fatal(err)
			}

			if err := executor.Rollback(ctx, object); err != nil {
				t.Fatal(err)
			}

			active = object.Status.Activation.ActivePVC
			if object.Status.Phase != domain.PhaseRolledBack || active.Name != "source" ||
				active.UID != "created-2" ||
				object.Status.Activation.RolledBackAt == nil ||
				object.Status.Message != "PVC name restored" {
				t.Fatalf("rollback checkpoint: %+v", object.Status)
			}

			if err := executor.Cleanup(
				ctx,
				object,
				IdentityCleanupOptions{Finalize: true, DeleteSession: true},
			); err != nil {
				t.Fatal(err)
			}

			pv, err := client.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			pvc, err := client.CoreV1().
				PersistentVolumeClaims("app").
				Get(ctx, "source", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if !store.deleted ||
				pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete ||
				pv.Labels[kube.SessionKey] != "" ||
				pvc.Annotations[kube.SessionKey] != "" {
				t.Fatalf(
					"finalization lost storage or ownership: pv=%+v pvc=%+v deleted=%v",
					pv,
					pvc,
					store.deleted,
				)
			}
		})
	}
}

func TestMoveExecutorRunAndRollbackPreserveNamespaces(t *testing.T) {
	for _, phase := range []v1alpha1.WorkflowPhase{domain.PhasePlanned, domain.PhaseMoving} {
		t.Run(string(phase), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			object := plannedMoveObject()
			client, spec := identityStorageFixture(t, "data")
			object.Status.Plan.Identity.SourceTemplate.Spec = spec
			object.Status.Plan.Identity.SourceTemplate.ReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			object.Status.Phase = phase
			store := &moveCheckpointStore{object: object.DeepCopy()}

			executor := NewMoveExecutor(
				client,
				store,
				&fakeSessionLocker{lock: &fakeSessionLock{}},
				"system",
			)
			if err := executor.Run(ctx, object); err != nil {
				t.Fatal(err)
			}

			active := object.Status.Activation.ActivePVC
			if object.Status.Phase != domain.PhaseCompleted || active.Namespace != "archive" ||
				active.UID != "created-1" {
				t.Fatalf("move checkpoint: %+v", object.Status)
			}

			if err := executor.Rollback(ctx, object); err != nil {
				t.Fatal(err)
			}

			active = object.Status.Activation.ActivePVC
			if object.Status.Phase != domain.PhaseRolledBack || active.Namespace != "app" ||
				active.Name != "data" ||
				active.UID != "created-2" ||
				object.Status.Message != "PVC namespace and name restored" {
				t.Fatalf("rollback checkpoint: %+v", object.Status)
			}
		})
	}
}

func TestIdentityExecutorsRejectAbortDuringRebinding(t *testing.T) {
	for _, failed := range []bool{false, true} {
		rename := plannedRenameObject()
		move := plannedMoveObject()

		rename.Status.Phase, move.Status.Phase = domain.PhaseRenaming, domain.PhaseMoving
		if failed {
			rename.Status.ResumeFrom, move.Status.ResumeFrom = rename.Status.Phase, move.Status.Phase
			rename.Status.Phase, move.Status.Phase = domain.PhaseFailed, domain.PhaseFailed
		}

		if err := (&RenameExecutor{}).ValidateAbort(
			rename,
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("rename abort during rebind: %v", err)
		}

		if err := (&MoveExecutor{storageNamespace: "system"}).ValidateAbort(
			move,
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("move abort during rebind: %v", err)
		}
	}
}

func TestRenameExecutorValidatesConsumersBeforeStorageMutation(t *testing.T) {
	object := plannedRenameObject()
	client, spec := identityStorageFixture(t, "source")
	object.Status.Plan.SourceTemplate.Spec = spec
	store := &renameCheckpointStore{object: object.DeepCopy()}
	executor := NewRenameExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"system",
	)

	_, err := client.CoreV1().Pods("app").Create(t.Context(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "app"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "source"},
		}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := executor.ValidateResume(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("dry-run: %v", err)
	}

	if store.writes != 0 {
		t.Fatal("dry-run persisted workflow")
	}

	if err := executor.Run(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("run: %v", err)
	}

	for _, action := range client.Actions() {
		if action.GetResource().Resource == "persistentvolumeclaims" && action.GetVerb() != "get" &&
			action.GetVerb() != "list" {
			t.Fatalf("storage mutated with an active consumer: %v", action)
		}
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhasePlanned {
		t.Fatalf("failure lost retry phase: %+v", object.Status)
	}
}

func TestRenameExecutorRejectsSourceSnapshotDrift(t *testing.T) {
	for _, drift := range []string{"spec", "labels", "annotations", "owner", "reclaim"} {
		t.Run(drift, func(t *testing.T) {
			object := plannedRenameObject()
			client, spec := identityStorageFixture(t, "source")
			object.Status.Plan.SourceTemplate.Spec = spec

			pvc, err := client.CoreV1().
				PersistentVolumeClaims("app").
				Get(t.Context(), "source", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			switch drift {
			case "spec":
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
			case "labels":
				pvc.Labels = map[string]string{"owner": "changed"}
			case "annotations":
				pvc.Annotations = map[string]string{"owner": "changed"}
			case "owner":
				pvc.OwnerReferences = []metav1.OwnerReference{{Name: "new-owner", UID: "new-owner"}}
			case "reclaim":
				pv, err := client.CoreV1().
					PersistentVolumes().
					Get(t.Context(), "pv", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}

				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
				if _, err := client.CoreV1().
					PersistentVolumes().
					Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := client.CoreV1().
				PersistentVolumeClaims("app").
				Update(t.Context(), pvc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}

			client.ClearActions()

			store := &renameCheckpointStore{object: object.DeepCopy()}

			executor := NewRenameExecutor(
				client,
				store,
				&fakeSessionLocker{lock: &fakeSessionLock{}},
				"system",
			)
			if err := executor.Run(
				t.Context(),
				object,
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("drift accepted: %v", err)
			}

			if object.Status.Phase != domain.PhaseFailed ||
				object.Status.ResumeFrom != domain.PhasePlanned {
				t.Fatalf("drift advanced execution: %+v", object.Status)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("drift caused resource mutation: %v", action)
				}
			}
		})
	}
}

func TestRenameExecutorRollbackRejectsBothEndpointsBeforeCheckpoint(t *testing.T) {
	object := plannedRenameObject()
	client, spec := identityStorageFixture(t, "target")
	object.Status.Plan.SourceTemplate.Spec = spec
	object.Status.Phase = domain.PhaseCompleted

	object.Status.Activation.ActivePVC = &v1alpha1.LocalResourceReference{
		Name: "target",
		UID:  "source-uid",
	}
	if err := client.Tracker().Add(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "source", UID: "conflicting-source",
	}}); err != nil {
		t.Fatal(err)
	}

	store := &renameCheckpointStore{object: object.DeepCopy()}

	executor := NewRenameExecutor(
		client,
		store,
		&fakeSessionLocker{lock: &fakeSessionLock{}},
		"system",
	)
	if err := executor.Rollback(
		t.Context(),
		object,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("rollback: %v", err)
	}

	if store.writes != 0 || object.Status.Phase != domain.PhaseCompleted {
		t.Fatal("conflict advanced workflow")
	}
}

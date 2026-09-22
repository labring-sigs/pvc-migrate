package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func reservationCleanupFixture(
	t *testing.T,
) (*ClusterReservationExecutor, *v1alpha1.ClusterReservation, *reservationCheckpointStore, *fake.Clientset) {
	t.Helper()

	executor, object, store, _ := reservationExecutorFixture(t)
	for i := range object.Status.Plan.Volumes {
		object.Status.Plan.Volumes[i].SourceReclaimPolicy = corev1.PersistentVolumeReclaimDelete
	}

	store.object = object.DeepCopy()
	if err := executor.Run(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	resources := make([]runtime.Object, 0, 4*len(object.Status.Plan.Volumes))
	for index, volume := range object.Status.Plan.Volumes {
		checkpoint := object.Status.Volumes[index]
		source := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "source", Name: volume.SourcePVC.Name, UID: volume.SourcePVC.UID,
				Annotations: map[string]string{kube.SessionKey: object.Name},
			},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: volume.SourcePV.Name},
		}
		sourcePV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: volume.SourcePV.Name,
				UID:  volume.SourcePV.UID,
				Labels: map[string]string{
					kube.SessionKey:        object.Name,
					kube.ManagedByLabel:    kube.ManagedByValue,
					kube.ResourceRoleLabel: kube.ResourceRoleSource,
				},
				Annotations: map[string]string{kube.OriginalPolicyAnnotation: "Delete"},
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				ClaimRef: &corev1.ObjectReference{
					Namespace: source.Namespace,
					Name:      source.Name,
					UID:       source.UID,
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		}
		destination := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "destination",
				Name:      checkpoint.DestinationPVC.Name,
				UID:       checkpoint.DestinationPVC.UID,
				Labels: map[string]string{
					kube.SessionKey:        object.Name,
					kube.ManagedByLabel:    kube.ManagedByValue,
					kube.ResourceRoleLabel: kube.ResourceRoleDestination,
				},
				Annotations: map[string]string{
					kube.SessionKey:             object.Name,
					kube.SourcePVCUIDAnnotation: string(source.UID),
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{VolumeName: checkpoint.DestinationPV.Name},
		}
		destinationPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: checkpoint.DestinationPV.Name,
				UID:  checkpoint.DestinationPV.UID,
				Labels: map[string]string{
					kube.SessionKey:        object.Name,
					kube.ManagedByLabel:    kube.ManagedByValue,
					kube.ResourceRoleLabel: kube.ResourceRoleDestination,
				},
				Annotations: map[string]string{kube.OriginalPolicyAnnotation: "Delete"},
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				ClaimRef: &corev1.ObjectReference{
					Namespace: destination.Namespace,
					Name:      destination.Name,
					UID:       destination.UID,
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		}
		resources = append(resources, source, sourcePV, destination, destinationPV)
	}

	client := fake.NewClientset(resources...)
	executor.client = client

	return executor, object, store, client
}

func TestReservationCleanupPreviewAndFinalizationRetry(t *testing.T) {
	executor, object, store, client := reservationCleanupFixture(t)
	object.Status.Volumes[0].DestinationPV = nil
	object.Status.Volumes[0].Reserved = false
	object.Status.Phase = domain.PhaseAborted
	store.object = object.DeepCopy()
	before := object.DeepCopy()
	options := ReservationCleanupOptions{Finalize: true}

	if err := executor.ValidateCleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("preview changed the input workflow")
	}

	assertReservationCleanupReadOnly(t, client)

	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	if store.object.Status.Volumes[0].DestinationPV == nil {
		t.Fatal("recovered PV was not checkpointed")
	}

	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatalf("finalization retry failed: %v", err)
	}

	for _, volume := range object.Status.Plan.Volumes {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims("source").
			Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil || pvc.UID != volume.SourcePVC.UID || pvc.Annotations[kube.SessionKey] != "" {
			t.Fatalf("source PVC was not retained and released: %+v, %v", pvc, err)
		}

		pv, err := client.CoreV1().
			PersistentVolumes().
			Get(t.Context(), volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil || pv.UID != volume.SourcePV.UID ||
			pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
			t.Fatalf("source PV was not restored: %+v, %v", pv, err)
		}
	}
}

func TestReservationCleanupCheckpointFailurePreventsMutations(t *testing.T) {
	executor, object, store, client := reservationCleanupFixture(t)
	object.Status.Volumes[0].DestinationPV = nil
	object.Status.Volumes[0].Reserved = false
	object.Status.Phase = domain.PhaseAborted
	store.object = object.DeepCopy()
	before := object.DeepCopy()
	failure := errors.New("checkpoint unavailable")
	store.failAt = store.writes + 1
	store.err = failure

	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true},
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(object, before) {
		t.Fatal("failed save changed the in-memory checkpoint")
	}

	assertReservationCleanupReadOnly(t, client)
}

func TestReservationCleanupPreflightFailurePreventsMutations(t *testing.T) {
	executor, object, _, client := reservationCleanupFixture(t)
	failure := errors.New("Pod inventory unavailable")
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, failure
	})

	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true},
	); !errors.Is(
		err,
		failure,
	) {
		t.Fatalf("error = %v", err)
	}

	assertReservationCleanupReadOnly(t, client)
}

func assertReservationCleanupReadOnly(t *testing.T, client *fake.Clientset) {
	t.Helper()

	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("unexpected mutation: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestReservationCleanupRetainsSourcePVAfterSourcePVCDisappears(t *testing.T) {
	executor, object, _, client := reservationCleanupFixture(t)

	source := object.Status.Plan.Volumes[0]
	if err := client.CoreV1().
		PersistentVolumeClaims("source").
		Delete(t.Context(), source.SourcePVC.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true},
	); err != nil {
		t.Fatal(err)
	}

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(t.Context(), source.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain ||
		pv.Labels[kube.SessionKey] != "" {
		t.Fatalf("released source storage is not protected: %+v", pv)
	}
}

func TestReservationDeletionRetainsMetadataWhenLeaseCleanupFails(t *testing.T) {
	executor, object, store, _ := reservationCleanupFixture(t)
	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	store.object = object.DeepCopy()
	failure := errors.New("lease deletion unavailable")
	lock := &renameDeletionLock{deleteErr: failure}
	executor.locker = &fakeSessionLocker{lock: lock}

	if err := executor.FinalizeDeleted(t.Context(), object); !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if store.object == nil {
		t.Fatal("workflow metadata was removed before lease cleanup succeeded")
	}

	if lock.deletes != 1 {
		t.Fatalf("lease cleanup was not attempted: %d", lock.deletes)
	}

	lock.deleteErr = nil

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.object != nil || lock.deletes != 2 {
		t.Fatal("workflow did not converge after lease cleanup recovered")
	}
}

func TestReservationDeletionBeforePlanningNeedsNoDataPlane(t *testing.T) {
	executor, object, store, _ := reservationExecutorFixture(t)
	object.Status = v1alpha1.ClusterReservationStatus{}
	object.DeletionTimestamp = &metav1.Time{Time: executor.now()}
	store.object = object.DeepCopy()
	executor.client = nil

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if store.object != nil || object.Status.Phase != domain.PhaseAborted {
		t.Fatal("unplanned deletion did not complete")
	}
}

func TestReservationCleanupDeletesOnlyDestinationStorage(t *testing.T) {
	executor, object, _, client := reservationCleanupFixture(t)
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetNamespace() != "destination" {
				t.Fatal("cleanup attempted to delete a source PVC")
			}

			deletion, ok := action.(k8stesting.DeleteAction)
			if !ok {
				t.Fatal("expected PVC deletion action")
			}

			name := deletion.GetName()
			for _, checkpoint := range object.Status.Volumes {
				if checkpoint.DestinationPVC.Name != name {
					continue
				}

				resource := schema.GroupVersionResource{
					Version:  "v1",
					Resource: "persistentvolumes",
				}

				stored, err := client.Tracker().Get(resource, "", checkpoint.DestinationPV.Name)
				if err != nil {
					return true, nil, err
				}

				current, ok := stored.(*corev1.PersistentVolume)
				if !ok {
					t.Fatal("expected a stored PV")
				}

				pv := current.DeepCopy()

				pv.Status.Phase = corev1.VolumeReleased
				if err := client.Tracker().Update(resource, pv, ""); err != nil {
					return true, nil, err
				}
			}

			return false, nil, nil
		},
	)

	object.Status.Phase = domain.PhaseAborted
	options := ReservationCleanupOptions{Finalize: true, UnusedStoragePolicy: "Delete"}

	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatalf("deletion retry failed: %v", err)
	}

	pvs, err := client.CoreV1().PersistentVolumes().List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if len(pvs.Items) != len(object.Status.Plan.Volumes) {
		t.Fatalf("remaining PVs = %d", len(pvs.Items))
	}

	for _, volume := range object.Status.Plan.Volumes {
		pvc, err := client.CoreV1().
			PersistentVolumeClaims("source").
			Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil || pvc.UID != volume.SourcePVC.UID {
			t.Fatalf("source was not retained: %+v, %v", pvc, err)
		}
	}
}

func TestReservationCleanupRejectsChangedSourceBindingBeforeMutation(t *testing.T) {
	executor, object, _, client := reservationCleanupFixture(t)
	source := object.Status.Plan.Volumes[0]

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(t.Context(), source.SourcePV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.Spec.ClaimRef.UID = "foreign-claim"
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	client.ClearActions()

	if err := executor.Cleanup(
		t.Context(),
		object,
		ReservationCleanupOptions{Finalize: true},
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("error = %v", err)
	}

	assertReservationCleanupReadOnly(t, client)
}

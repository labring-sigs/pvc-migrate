package kube

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestReservationRejectsUnrelatedCheckpointBeforeMutation(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		for _, checkpoint := range []v1alpha1.ClusterVolumeReservationStatus{
			{SourcePVCName: "other"},
			{DestinationPVC: &v1alpha1.ObjectReference{Namespace: "other", Name: "destination"}},
			{DestinationPVC: &v1alpha1.ObjectReference{Namespace: "system", Name: "other"}},
		} {
			sourcePVC, sourcePV := reserveSourceObjects()
			client := fake.NewClientset(sourcePVC, sourcePV)
			reserver := NewReserver(client)
			before := checkpoint.DeepCopy()
			desired := reservationDestinationFixture()

			var err error
			if dryRun {
				err = reserver.ValidateVolumeReservation(
					t.Context(),
					ReservationRequest{SessionID: "session"},
					PVCReference(sourcePVC),
					PVReference(sourcePV),
					"1Gi",
					desired,
					checkpoint,
				)
			} else {
				err = reserver.ReserveVolume(t.Context(), ReservationRequest{SessionID: "session"},
					PVCReference(sourcePVC), PVReference(sourcePV), "1Gi", desired, &checkpoint)
			}

			if domain.CategoryOf(err) != domain.ErrorConflict ||
				!reflect.DeepEqual(checkpoint, *before) {
				t.Fatalf("dryRun=%v checkpoint=%+v err=%v", dryRun, checkpoint, err)
			}

			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("mismatched checkpoint caused mutation: %+v", action)
				}
			}
		}
	}
}

func TestReservationPreflightOwnsDestinationManifest(t *testing.T) {
	sourcePVC, sourcePV := reserveSourceObjects()
	client := fake.NewClientset(sourcePVC, sourcePV)
	created := false
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			create := testutil.MustType[interface {
				clienttesting.CreateAction
				GetCreateOptions() metav1.CreateOptions
			}](t, action)

			pvc := testutil.MustType[*corev1.PersistentVolumeClaim](t, create.GetObject())
			if !reflect.DeepEqual(create.GetCreateOptions().DryRun, []string{metav1.DryRunAll}) ||
				pvc.Labels[SessionKey] != "session" || pvc.Labels[ManagedByLabel] != ManagedByValue ||
				pvc.Labels[ResourceRoleLabel] != ResourceRoleDestination ||
				pvc.Annotations[SourcePVCUIDAnnotation] != string(sourcePVC.UID) ||
				pvc.Annotations[SourcePVAnnotation] != sourcePV.Name || pvc.Labels["application"] != "data" {
				t.Fatalf("incorrect dry-run manifest: %+v", pvc)
			}

			created = true

			return true, pvc, nil
		},
	)

	desired := reservationDestinationFixture()
	desired.Labels = map[string]string{"application": "data", SessionKey: "untrusted"}
	desired.Annotations = map[string]string{SourcePVAnnotation: "untrusted"}
	before := desired.DeepCopy()

	err := NewReserver(
		client,
	).ValidateVolumeReservation(t.Context(), ReservationRequest{SessionID: "session"},
		PVCReference(
			sourcePVC,
		), PVReference(sourcePV), "1Gi", desired, v1alpha1.ClusterVolumeReservationStatus{})
	if err != nil || !created || !reflect.DeepEqual(desired, before) {
		t.Fatalf(
			"preflight mutated input or failed: created=%v err=%v desired=%+v",
			created,
			err,
			desired,
		)
	}
}

func reservationDestinationFixture() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "destination"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
	}
}

func TestReservationPreflightRejectsMissingRecordedDestination(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		sourcePVC, sourcePV := reserveSourceObjects()
		client := fake.NewClientset(sourcePVC, sourcePV)
		client.PrependReactor(
			"create",
			"persistentvolumeclaims",
			func(action clienttesting.Action) (bool, runtime.Object, error) {
				create := testutil.MustType[interface {
					clienttesting.CreateAction
					GetCreateOptions() metav1.CreateOptions
				}](t, action)
				if !reflect.DeepEqual(
					create.GetCreateOptions().DryRun,
					[]string{metav1.DryRunAll},
				) {
					t.Fatal("preflight attempted a persistent create")
				}

				return true, create.GetObject(), nil
			},
		)

		checkpoint := v1alpha1.ClusterVolumeReservationStatus{
			SourcePVCName: sourcePVC.Name,
			Reserved:      reserved,
			DestinationPVC: &v1alpha1.ObjectReference{
				Namespace: "system",
				Name:      "destination",
				UID:       "original-pvc",
			},
			DestinationPV: &v1alpha1.ObjectReference{Name: "destination-pv", UID: "original-pv"},
		}
		before := checkpoint.DeepCopy()

		err := NewReserver(
			client,
		).ValidateVolumeReservation(t.Context(), ReservationRequest{SessionID: "session"},
			PVCReference(
				sourcePVC,
			), PVReference(sourcePV), "1Gi", reservationDestinationFixture(), checkpoint)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("reserved=%t error=%v", reserved, err)
		}

		if !reflect.DeepEqual(checkpoint, *before) {
			t.Fatal("preflight changed recorded identities")
		}
	}
}

func TestReservationFailureDoesNotPublishEmptyIdentities(t *testing.T) {
	sourcePVC, sourcePV := reserveSourceObjects()
	storageClass := "fast"
	client := fake.NewClientset(sourcePVC, sourcePV, &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: storageClass},
	}, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		}}},
	})
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Resource: "persistentvolumeclaims"},
				"destination",
				nil,
			)
		},
	)

	desired := reservationDestinationFixture()
	desired.Spec.StorageClassName = &storageClass
	checkpoint := &v1alpha1.ClusterVolumeReservationStatus{}
	err := NewReserver(client).ReserveVolume(t.Context(), ReservationRequest{SessionID: "session"},
		PVCReference(sourcePVC), PVReference(sourcePV), "1Gi", desired, checkpoint)

	if !apierrors.IsForbidden(err) || checkpoint.SourcePVCName != "data" || checkpoint.Reserved ||
		checkpoint.DestinationPVC != nil || checkpoint.DestinationPV != nil {
		t.Fatalf(
			"failed reservation published invalid identities: checkpoint=%+v err=%v",
			checkpoint,
			err,
		)
	}
}

func TestReservationDoesNotRecreateMissingRecordedDestination(t *testing.T) {
	for i := range 2 {
		completed := i == 1
		client := fake.NewClientset()
		pvc := v1alpha1.ObjectReference{
			Namespace: "system",
			Name:      "destination",
			UID:       "original-pvc",
		}
		pv := v1alpha1.ObjectReference{Name: "destination-pv", UID: "original-pv"}
		policy := corev1.PersistentVolumeReclaimDelete
		reserved := i == 1

		err := NewReserver(
			client,
		).reserveVolumeLive(t.Context(), ReservationRequest{SessionID: "session"}, "source", reservationDestinationFixture(), &pvc, &pv, &policy, &reserved)
		if domain.CategoryOf(err) != domain.ErrorConflict {
			t.Fatalf("completed=%t error=%v", completed, err)
		}

		for _, action := range client.Actions() {
			if action.GetVerb() != "get" {
				t.Fatalf("missing destination caused mutation: %v", action)
			}
		}

		if pvc.UID != "original-pvc" || pv.UID != "original-pv" || reserved != completed {
			t.Fatal("missing destination rewrote checkpoint")
		}
	}
}

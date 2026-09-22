package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func reservationRecoveryFixture() (v1alpha1.ObjectReference, *corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	source := v1alpha1.ObjectReference{Namespace: "source", Name: "data", UID: "source-uid"}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "target", Name: "data", UID: "destination-uid",
			Labels: map[string]string{
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.SessionKey:        "reservation",
				kube.ResourceRoleLabel: kube.ResourceRoleDestination,
			},
			Annotations: map[string]string{
				kube.SessionKey:             "reservation",
				kube.SourcePVCUIDAnnotation: string(source.UID),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "destination-pv"},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "destination-pv", UID: "pv-uid",
			Labels: map[string]string{
				kube.ManagedByLabel:    kube.ManagedByValue,
				kube.SessionKey:        "reservation",
				kube.ResourceRoleLabel: kube.ResourceRoleDestination,
			},
			Annotations: map[string]string{kube.OriginalPolicyAnnotation: "Delete"},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef: &corev1.ObjectReference{
				Namespace: pvc.Namespace,
				Name:      pvc.Name,
				UID:       pvc.UID,
			},
		},
	}

	return source, pvc, pv
}

func TestReservationRecoveryCompletesKnownPVCCheckpoint(t *testing.T) {
	source, pvc, pv := reservationRecoveryFixture()
	ref := kube.PVCReference(pvc)
	checkpoint := v1alpha1.ClusterVolumeReservationStatus{
		SourcePVCName:  source.Name,
		DestinationPVC: &ref,
	}
	before := checkpoint.DeepCopy()
	client := fake.NewClientset(pvc, pv)

	recovered, err := recoverReservationVolume(
		context.Background(),
		client,
		"reservation",
		source,
		ref,
		checkpoint,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}

	if recovered.DestinationPV == nil || recovered.DestinationPV.UID != pv.UID ||
		recovered.DestinationPolicy != corev1.PersistentVolumeReclaimDelete || recovered.Reserved {
		t.Fatalf("unexpected recovered checkpoint: %+v", recovered)
	}

	recovered.DestinationPVC.Name = "changed"

	if !reflect.DeepEqual(checkpoint, *before) {
		t.Fatal("recovery aliased the original checkpoint")
	}
}

func TestReservationRecoveryRejectsConflictingIdentityAtomically(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
	}{
		{"replaced PVC", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.UID = "replacement" }},
		{"foreign PVC", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			pvc.Labels[kube.SessionKey] = "foreign"
		}},
		{"foreign source", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			pvc.Annotations[kube.SourcePVCUIDAnnotation] = "other-source"
		}},
		{"foreign PV", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Labels[kube.SessionKey] = "foreign"
		}},
		{"foreign claim", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.Spec.ClaimRef.UID = "foreign" }},
		{"invalid policy", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Annotations[kube.OriginalPolicyAnnotation] = "invalid"
		}},
		{"changed policy", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Annotations[kube.OriginalPolicyAnnotation] = "Retain"
		}},
		{"changed live policy", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Spec.PersistentVolumeReclaimPolicy = "unexpected"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, pvc, pv := reservationRecoveryFixture()
			ref := kube.PVCReference(pvc)
			checkpoint := v1alpha1.ClusterVolumeReservationStatus{
				SourcePVCName: source.Name, DestinationPVC: &ref,
				DestinationPolicy: corev1.PersistentVolumeReclaimDelete,
			}
			before := checkpoint.DeepCopy()

			tc.mutate(pvc, pv)

			recovered, err := recoverReservationVolume(
				context.Background(),
				fake.NewClientset(pvc, pv),
				"reservation",
				source,
				ref,
				checkpoint,
				true,
			)
			if err == nil {
				t.Fatal("accepted conflicting storage")
			}

			if !reflect.DeepEqual(checkpoint, *before) || !reflect.DeepEqual(recovered, *before) {
				t.Fatal("failed recovery changed a checkpoint")
			}
		})
	}
}

func TestReservationRecoveryPVReadFailureDoesNotCheckpointDiscoveredPVC(t *testing.T) {
	source, pvc, pv := reservationRecoveryFixture()
	client := fake.NewClientset(pvc, pv)
	failure := errors.New("PV read unavailable")
	client.PrependReactor(
		"get",
		"persistentvolumes",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, failure
		},
	)

	destination := kube.PVCReference(pvc)
	destination.UID = ""
	checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: source.Name}

	recovered, err := recoverReservationVolume(
		t.Context(),
		client,
		"reservation",
		source,
		destination,
		checkpoint,
		true,
	)
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}

	if !reflect.DeepEqual(recovered, checkpoint) {
		t.Fatal("PV read failure returned a partially recovered checkpoint")
	}
}

func TestReservationRecoveryRequiresProvenanceForNewPVC(t *testing.T) {
	source, pvc, pv := reservationRecoveryFixture()
	ref := kube.PVCReference(pvc)
	pvc.Labels = nil
	pvc.Annotations = nil
	client := fake.NewClientset(pvc, pv)
	checkpoint := v1alpha1.ClusterVolumeReservationStatus{SourcePVCName: source.Name}
	unknown := ref

	unknown.UID = ""
	if _, err := recoverReservationVolume(
		context.Background(),
		client,
		"reservation",
		source,
		unknown,
		checkpoint,
		false,
	); err == nil {
		t.Fatal("adopted an unknown PVC without provenance")
	}

	checkpoint.DestinationPVC = &ref
	if _, err := recoverReservationVolume(
		context.Background(),
		client,
		"reservation",
		source,
		unknown,
		checkpoint,
		false,
	); err != nil {
		t.Fatalf("known finalized PVC rejected: %v", err)
	}
}

func TestReservationRecoveryUnownedProvisionedPV(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		allow, reserved, retain, wantError bool
	}{
		{"allowed incomplete checkpoint", true, false, false, false},
		{"not allowed", false, false, false, true},
		{"completed reservation", true, true, false, true},
		{"unowned retain PV", true, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, pvc, pv := reservationRecoveryFixture()
			pv.Labels = nil
			pv.Annotations = nil

			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			if tc.retain {
				pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			}

			checkpoint := v1alpha1.ClusterVolumeReservationStatus{
				SourcePVCName: source.Name,
				Reserved:      tc.reserved,
			}

			_, err := recoverReservationVolume(
				context.Background(),
				fake.NewClientset(pvc, pv),
				"reservation",
				source,
				kube.PVCReference(pvc),
				checkpoint,
				tc.allow,
			)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError = %v", err, tc.wantError)
			}
		})
	}
}

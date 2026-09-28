package app

import (
	"context"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestVerifyActiveStorageVolumeRejectsTerminatingActive(t *testing.T) {
	active := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "app",
			Name:      "data",
			UID:       "active-uid",
			Annotations: map[string]string{
				kube.SessionKey: "session",
			},
		},
		Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-destination"},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	destinationPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: "pv-uid"},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       "active-uid",
			},
		},
	}

	armed := metav1.NewTime(time.Unix(100, 0).UTC())
	active.DeletionTimestamp = &armed
	client := fake.NewClientset(active, destinationPV)

	source := v1alpha1.ObjectReference{Namespace: "app", Name: "data", UID: "source-uid"}
	expectedPV := v1alpha1.ObjectReference{Name: "pv-destination", UID: "pv-uid"}
	recorded := v1alpha1.ObjectReference{Namespace: "app", Name: "data", UID: "active-uid"}

	err := verifyActiveStorageVolume(
		t.Context(),
		client,
		"session",
		source,
		"app",
		expectedPV,
		&recorded,
	)
	if err == nil || !strings.Contains(err.Error(), "active PVC app/data is terminating") {
		t.Fatalf("terminating active claim accepted: %v", err)
	}

	// A deletion pass keeps converging instead of rejecting.
	deletionCtx := context.WithValue(t.Context(), workflowDeletionContextKey{}, true)
	if err := verifyActiveStorageVolume(
		deletionCtx,
		client,
		"session",
		source,
		"app",
		expectedPV,
		&recorded,
	); err != nil {
		t.Fatalf("deletion pass rejected a terminating active claim: %v", err)
	}
}

// A claim that finished deleting is the terminal state of the very deletion
// the terminating tolerance covers: the deletion pass must converge through
// cleanup instead of failing on the read, or a torn-down namespace wedges the
// workflow finalizer forever (its PVCs are gone before its CR is).
func TestVerifyActiveStorageVolumeConvergesWhenActiveDeleted(t *testing.T) {
	destinationPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-destination", UID: "pv-uid"},
	}
	// The active claim is gone entirely; only the PV remains.
	client := fake.NewClientset(destinationPV)

	source := v1alpha1.ObjectReference{Namespace: "app", Name: "data", UID: "source-uid"}
	expectedPV := v1alpha1.ObjectReference{Name: "pv-destination", UID: "pv-uid"}
	recorded := v1alpha1.ObjectReference{Namespace: "app", Name: "data", UID: "active-uid"}

	if err := verifyActiveStorageVolume(
		t.Context(),
		client,
		"session",
		source,
		"app",
		expectedPV,
		&recorded,
	); err == nil {
		t.Fatal("live pass accepted a deleted active claim")
	}

	deletionCtx := context.WithValue(t.Context(), workflowDeletionContextKey{}, true)
	if err := verifyActiveStorageVolume(
		deletionCtx,
		client,
		"session",
		source,
		"app",
		expectedPV,
		&recorded,
	); err != nil {
		t.Fatalf("deletion pass rejected a deleted active claim: %v", err)
	}

	// The PV vanishing mid-deletion converges the same way.
	empty := fake.NewClientset()
	if err := verifyActiveStorageVolume(
		deletionCtx,
		empty,
		"session",
		source,
		"app",
		expectedPV,
		&recorded,
	); err != nil {
		t.Fatalf("deletion pass rejected a deleted active pair: %v", err)
	}
}

package backup

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRestoreDestinationCheckpointRejectsLostSharedFence(t *testing.T) {
	lost := errors.New("workflow lease lost")
	lock := &recordingBackupSessionLock{err: lost}
	ctx := kube.WithLeaseFence(t.Context(), lock)
	client := fake.NewClientset()
	status := v1alpha1.RestoreStatus{
		DestinationPVC: &v1alpha1.ObjectReference{},
		DestinationPV:  &v1alpha1.ObjectReference{},
	}

	err := checkpointRestoreDestinationIdentity(ctx, client,
		v1alpha1.ObjectReference{Namespace: "app", Name: "data", UID: "pvc"}, "pv",
		status.DestinationPVC, status.DestinationPV,
		func(context.Context) error { t.Fatal("lost lease wrote a checkpoint"); return nil },
	)
	if !errors.Is(err, lost) || len(client.Actions()) != 0 || status.DestinationPVC.UID != "" ||
		status.DestinationPV.UID != "" {
		t.Fatalf("lost shared fence did not stop checkpoint: status=%#v error=%v", status, err)
	}
}

func TestRestoreCreationRejectsMissingPlannedPVC(t *testing.T) {
	client := fake.NewClientset(
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "restore-sc"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "worker"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	plan := v1alpha1.RestorePlan{
		DestinationPVC:          v1alpha1.LocalResourceReference{Name: "data", UID: "planned-uid"},
		DestinationStorageClass: "restore-sc", DestinationAccessMode: string(corev1.ReadWriteOnce),
	}

	err := createRestorePVC(
		t.Context(),
		client,
		"app", "restore",
		plan,
		objectstore.Config{},
		objectstore.Manifest{Capacity: "1Gi", VolumeMode: "Filesystem"},
		nil, nil,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("missing planned PVC error = %v", err)
	}

	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("recreated a missing planned PVC")
		}
	}
}

func TestRestoreCreationRejectsInvalidManifest(t *testing.T) {
	for _, manifest := range []objectstore.Manifest{
		{Capacity: "0", VolumeMode: "Filesystem"},
		{Capacity: "-1Gi", VolumeMode: "Filesystem"},
		{Capacity: "1Gi", VolumeMode: "Block"},
	} {
		t.Run(manifest.Capacity+manifest.VolumeMode, func(t *testing.T) {
			client := fake.NewClientset(
				&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "restore-sc"}},
			)
			plan := v1alpha1.RestorePlan{
				DestinationPVC:          v1alpha1.LocalResourceReference{Name: "data"},
				DestinationStorageClass: "restore-sc",
				DestinationAccessMode:   string(corev1.ReadWriteOnce),
			}

			err := createRestorePVC(
				t.Context(),
				client,
				"app", "restore",
				plan,
				objectstore.Config{},
				manifest,
				nil, nil,
			)
			if domain.CategoryOf(err) != domain.ErrorPrecondition {
				t.Fatalf("invalid manifest error = %v", err)
			}

			if len(client.Actions()) != 0 {
				t.Fatal("invalid backup accessed Kubernetes resources")
			}
		})
	}
}

func TestRestoreBindingPreservesIdentityAcrossProbe(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: "original"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
	client := fake.NewClientset(pvc)

	err := bindRestorePVC(
		t.Context(),
		client,
		func(ctx context.Context, input *corev1.PersistentVolumeClaim) error {
			input.UID = "replacement"
			input.Spec.VolumeName = "pv"
			input.Status.Phase = corev1.ClaimBound
			_, err := client.CoreV1().
				PersistentVolumeClaims(input.Namespace).
				Update(ctx, input, metav1.UpdateOptions{})

			return err
		},
		pvc,
	)
	if domain.CategoryOf(err) != domain.ErrorConflict || pvc.UID != "original" {
		t.Fatalf("probe changed expected identity: UID=%s error=%v", pvc.UID, err)
	}
}

func TestRestoreBindingRejectsTerminatingAndLostPVC(t *testing.T) {
	for _, state := range []string{"deleting", "lost"} {
		t.Run(state, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "app",
					Name:      "data",
					UID:       types.UID("pvc-uid"),
				},
			}
			if state == "deleting" {
				now := metav1.Now()
				pvc.DeletionTimestamp = &now
			} else {
				pvc.Status.Phase = corev1.ClaimLost
			}

			client := fake.NewClientset(pvc)
			if err := waitForRestorePVCBound(
				t.Context(),
				client,
				kube.PVCReference(pvc),
			); domain.CategoryOf(
				err,
			) != domain.ErrorConflict {
				t.Fatalf("terminal binding state error = %v", err)
			}
		})
	}
}

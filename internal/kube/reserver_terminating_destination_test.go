package kube

import (
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// reservedDestinationWorld builds a fully valid reserved destination pair —
// the state every transfer re-validation expects — plus its checkpoint.
func reservedDestinationWorld() (
	*fake.Clientset,
	*corev1.PersistentVolumeClaim,
	*corev1.PersistentVolume,
	v1alpha1.ClusterVolumeReservationStatus,
) {
	sourcePVC, sourcePV := reserveSourceObjects()

	destinationPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "system",
			Name:      "destination",
			UID:       "destination-pvc-uid",
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        "session",
				ResourceRoleLabel: ResourceRoleDestination,
			},
			Annotations: map[string]string{
				SessionKey:             "session",
				SourcePVCUIDAnnotation: string(sourcePVC.UID),
				SourcePVAnnotation:     sourcePV.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "destination-pv",
			StorageClassName: sourcePVC.Spec.StorageClassName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	destinationPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "destination-pv",
			UID:  "destination-pv-uid",
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				SessionKey:        "session",
				ResourceRoleLabel: ResourceRoleDestination,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Namespace: "system",
				Name:      "destination",
				UID:       "destination-pvc-uid",
			},
		},
	}

	client := fake.NewClientset(sourcePVC, sourcePV, destinationPVC, destinationPV)
	checkpoint := v1alpha1.ClusterVolumeReservationStatus{
		SourcePVCName: sourcePVC.Name,
		Reserved:      true,
		DestinationPVC: &v1alpha1.ObjectReference{
			Namespace: "system",
			Name:      "destination",
			UID:       "destination-pvc-uid",
		},
		DestinationPV: &v1alpha1.ObjectReference{Name: "destination-pv", UID: "destination-pv-uid"},
	}

	return client, sourcePVC, sourcePV, checkpoint
}

func TestReservationValidationRejectsTerminatingDestinationPV(t *testing.T) {
	client, sourcePVC, sourcePV, checkpoint := reservedDestinationWorld()

	desired := reservationDestinationFixture()
	storageClass := *sourcePVC.Spec.StorageClassName
	desired.Spec.StorageClassName = &storageClass

	validate := func() error {
		return NewReserver(client).ValidateVolumeReservation(
			t.Context(),
			ReservationRequest{SessionID: "session"},
			PVCReference(sourcePVC),
			PVReference(sourcePV),
			"1Gi",
			desired,
			checkpoint,
		)
	}

	if err := validate(); err != nil {
		t.Fatalf("valid reserved destination rejected: %v", err)
	}

	armed := metav1.NewTime(time.Unix(100, 0).UTC())

	pv, err := client.CoreV1().
		PersistentVolumes().
		Get(t.Context(), "destination-pv", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pv.DeletionTimestamp = &armed
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(t.Context(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	err = validate()
	if err == nil || domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "destination PV destination-pv is terminating") {
		t.Fatalf("terminating destination PV accepted: %v", err)
	}
}

package crosscluster_test

import (
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCleanupRetainReleasesOwnedOutputWithConsumerAndIsRepeatable(t *testing.T) {
	service, options, _ := crossFixture()
	options.UnusedStoragePolicy = "Delete"

	plan, err := service.PlanCopy(t.Context(), options)
	if err != nil || !plan.Ready {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}

	session, err := service.CreateCopySession(t.Context(), options, plan)
	if err != nil {
		t.Fatal(err)
	}

	client := service.DestinationClientForTest()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-copy",
			Namespace: "app",
			UID:       "destination-pvc",
			Labels:    map[string]string{SessionKey: session.ID, ManagedByLabel: ManagedBy},
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "destination-pv"},
	}
	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Create(t.Context(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "destination-pv", UID: "pv-uid"},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Name:      pvc.Name,
				Namespace: pvc.Namespace,
				UID:       pvc.UID,
			},
		},
	}
	if _, err := client.CoreV1().
		PersistentVolumes().
		Create(t.Context(), pv, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "app"},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
						},
					},
				},
			},
		},
	}
	if _, err := client.CoreV1().
		Pods("app").
		Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	if err := service.ValidateCleanup(t.Context(), session, ""); err == nil {
		t.Fatal("recorded Delete policy ignored active consumer")
	}

	if err := service.ValidateCleanup(t.Context(), session, "Keep"); err != nil {
		t.Fatal(err)
	}

	if err := service.Cleanup(t.Context(), session, "Keep", false); err != nil {
		t.Fatal(err)
	}

	if err := service.Cleanup(t.Context(), session, "Keep", true); err != nil {
		t.Fatal(err)
	}

	retained, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(t.Context(), pvc.Name, metav1.GetOptions{})
	if err != nil || retained.UID != pvc.UID || retained.Labels[SessionKey] != "" {
		t.Fatalf("retained PVC=%+v err=%v", retained, err)
	}
}

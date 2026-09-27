package app

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// protectedSourcePV is a source PV exactly as a graduated workflow sees it:
// the reservation's transfer protection switched it to Retain, and the copy's
// plan — built after that protection — recorded the live policy as the
// original. The annotation the first protection wrote is the authoritative
// original.
func protectedSourcePV() *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv",
			UID:  types.UID("pv"),
			Labels: map[string]string{
				kube.SessionKey:        "wf",
				kube.ResourceRoleLabel: kube.ResourceRoleSource,
				kube.ManagedByLabel:    kube.ManagedByValue,
			},
			Annotations: map[string]string{
				kube.OriginalPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef: &corev1.ObjectReference{
				Namespace: "data", Name: "src", UID: types.UID("pvc"),
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
}

func ownedSourcePVC() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "data",
			Name:      "src",
			UID:       types.UID("pvc"),
		},
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv"},
	}
}

// TestReleaseSourceOwnershipRestoresAnnotatedPolicyOverPlanRecord pins the
// graduated-workflow policy restore: the plan's SourceReclaimPolicy can be
// Retain (captured after the protection switched the PV), and restoring it
// verbatim silently leaves the tenant's PV on Retain. The release must prefer
// the original-policy annotation the first protection recorded.
func TestReleaseSourceOwnershipRestoresAnnotatedPolicyOverPlanRecord(t *testing.T) {
	pv := protectedSourcePV()
	client := fake.NewClientset(pv, ownedSourcePVC())

	err := releaseSourceOwnership(
		t.Context(),
		client,
		"wf",
		kube.PVCReference(ownedSourcePVC()),
		kube.PVReference(pv),
		// The plan recorded the live (already protected) policy.
		corev1.PersistentVolumeReclaimRetain,
	)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := client.CoreV1().PersistentVolumes().Get(t.Context(), "pv", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if restored.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf(
			"reclaim policy = %s, want Delete restored from the annotation",
			restored.Spec.PersistentVolumeReclaimPolicy,
		)
	}

	if restored.Labels[kube.SessionKey] != "" || restored.Labels[kube.ResourceRoleLabel] != "" {
		t.Fatalf("session labels retained: %v", restored.Labels)
	}

	if restored.Annotations[kube.OriginalPolicyAnnotation] != "" {
		t.Fatalf("original-policy annotation retained: %v", restored.Annotations)
	}
}

// Without the annotation (never protected, or already restored) the plan's
// recorded policy remains the restore target.
func TestReleaseSourceOwnershipFallsBackToPlanPolicy(t *testing.T) {
	pv := protectedSourcePV()
	pv.Annotations = nil
	client := fake.NewClientset(pv, ownedSourcePVC())

	err := releaseSourceOwnership(
		t.Context(),
		client,
		"wf",
		kube.PVCReference(ownedSourcePVC()),
		kube.PVReference(pv),
		corev1.PersistentVolumeReclaimDelete,
	)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := client.CoreV1().PersistentVolumes().Get(t.Context(), "pv", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if restored.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Fatalf(
			"reclaim policy = %s, want Delete from the plan record",
			restored.Spec.PersistentVolumeReclaimPolicy,
		)
	}
}

// A source PVC that is already gone must leave its PV on Retain even though
// the recorded original was Delete: reclaiming released storage is the
// orphan-cleanup pass's decision, never a side effect of ownership release.
func TestReleaseSourceOwnershipKeepsRetainForReleasedSource(t *testing.T) {
	pv := protectedSourcePV()
	client := fake.NewClientset(pv)

	if err := releaseSourceOwnership(
		t.Context(),
		client,
		"wf",
		kube.PVCReference(ownedSourcePVC()),
		kube.PVReference(pv),
		corev1.PersistentVolumeReclaimDelete,
	); err != nil {
		t.Fatal(err)
	}

	restored, err := client.CoreV1().PersistentVolumes().Get(t.Context(), "pv", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if restored.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf(
			"reclaim policy = %s, want Retain for a released source",
			restored.Spec.PersistentVolumeReclaimPolicy,
		)
	}

	if restored.Labels[kube.SessionKey] != "" {
		t.Fatalf("session labels retained: %v", restored.Labels)
	}
}

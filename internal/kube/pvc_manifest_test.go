package kube

import (
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBoundPVCManifestOwnsItsTemplate(t *testing.T) {
	controller := true
	storageClass := "source"
	spec := corev1.PersistentVolumeClaimSpec{
		StorageClassName: &storageClass,
		Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
		},
	}
	metadata := v1alpha1.PVCMetadata{
		Labels:          map[string]string{"app": "database"},
		Annotations:     map[string]string{"purpose": "original"},
		OwnerReferences: []metav1.OwnerReference{{Name: "database", Controller: &controller}},
	}
	originalSpec, originalMetadata := spec.DeepCopy(), metadata.DeepCopy()
	claim := v1alpha1.ObjectReference{Namespace: "app", Name: "renamed"}
	first := BoundPVCManifest("session", claim, "source-pv", spec, metadata)
	second := BoundPVCManifest("session", claim, "source-pv", spec, metadata)
	first.Labels["app"] = "changed"
	first.Annotations["purpose"] = "changed"
	*first.OwnerReferences[0].Controller = false
	*first.Spec.StorageClassName = "changed"
	first.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")

	if !reflect.DeepEqual(spec, *originalSpec) || !reflect.DeepEqual(metadata, *originalMetadata) {
		t.Fatal("manifest mutation changed the source template")
	}

	if !reflect.DeepEqual(second, BoundPVCManifest("session", claim, "source-pv", spec, metadata)) {
		t.Fatal("manifest mutation changed another recreation attempt")
	}
}

func TestRenameRejectsInvalidManifestBeforeMutatingStorage(t *testing.T) {
	for _, corrupt := range []func(*corev1.PersistentVolumeClaim){
		func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.VolumeName = "foreign-pv" },
		func(pvc *corev1.PersistentVolumeClaim) { pvc.Annotations[SessionKey] = "foreign-session" },
		func(pvc *corev1.PersistentVolumeClaim) { delete(pvc.Labels, ManagedByLabel) },
	} {
		switcher, session, volume := configureRenameFixture(t)
		manifest := BoundPVCManifest(session.ID, volume.DestinationPVC, volume.SourcePV.Name,
			volume.SourcePVCSpec, volume.SourcePVCMetadata)
		corrupt(manifest)

		_, err := switcher.RenamePVC(
			t.Context(),
			session.ID,
			volume.SourcePVC,
			volume.SourcePV,
			manifest,
			nil,
		)
		if domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("expected invalid manifest to be rejected: %v", err)
		}

		source, err := switcher.client.CoreV1().PersistentVolumeClaims(volume.SourcePVC.Namespace).
			Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
		if err != nil || source.UID != volume.SourcePVC.UID {
			t.Fatalf("invalid manifest changed source PVC: pvc=%v err=%v", source, err)
		}
	}
}

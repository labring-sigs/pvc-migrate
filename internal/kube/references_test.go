package kube

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestVolumeReferences(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "application",
		Name:            "data",
		UID:             types.UID("pvc-uid"),
		ResourceVersion: "12",
	}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name:            "pv-data",
		UID:             types.UID("pv-uid"),
		ResourceVersion: "34",
	}}

	if got := PVCReference(pvc); got != (v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       "application",
		Name:            "data",
		UID:             types.UID("pvc-uid"),
		ResourceVersion: "12",
	}) {
		t.Fatalf("PVC reference = %#v", got)
	}

	if got := PVReference(pv); got != (v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolume,
		Name:            "pv-data",
		UID:             types.UID("pv-uid"),
		ResourceVersion: "34",
	}) {
		t.Fatalf("PV reference = %#v", got)
	}

	if PVCReference(nil) != (v1alpha1.ObjectReference{}) ||
		PVReference(nil) != (v1alpha1.ObjectReference{}) {
		t.Fatal("nil volume objects should produce empty references")
	}
}

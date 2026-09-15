package kube

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
)

// PodReference records the identity used to constrain workload discovery.
func PodReference(pod *corev1.Pod) v1alpha1.ObjectReference {
	if pod == nil {
		return v1alpha1.ObjectReference{}
	}

	return v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPod,
		Namespace:       pod.Namespace,
		Name:            pod.Name,
		UID:             pod.UID,
		ResourceVersion: pod.ResourceVersion,
	}
}

// PVCReference records the identity fields used to fence PVC operations.
func PVCReference(pvc *corev1.PersistentVolumeClaim) v1alpha1.ObjectReference {
	if pvc == nil {
		return v1alpha1.ObjectReference{}
	}

	return v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolumeClaim,
		Namespace:       pvc.Namespace,
		Name:            pvc.Name,
		UID:             pvc.UID,
		ResourceVersion: pvc.ResourceVersion,
	}
}

// PVReference records the identity fields used to fence PV operations.
func PVReference(pv *corev1.PersistentVolume) v1alpha1.ObjectReference {
	if pv == nil {
		return v1alpha1.ObjectReference{}
	}

	return v1alpha1.ObjectReference{
		APIVersion:      domain.CoreAPIVersion,
		Kind:            domain.KindPersistentVolume,
		Name:            pv.Name,
		UID:             pv.UID,
		ResourceVersion: pv.ResourceVersion,
	}
}

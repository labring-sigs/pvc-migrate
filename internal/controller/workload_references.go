package controller

import (
	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func validateWorkloadScope(namespace string, adapter v1alpha1.WorkloadKind) error {
	if adapter == "" || (adapter != v1alpha1.WorkloadNone && namespace == "") {
		return domain.NewError(domain.ErrorValidation, "workload",
			"workload adapter and source namespace are required")
	}

	return nil
}

func qualifiedWorkloadReference(
	ref *v1alpha1.LocalResourceReference,
	namespace string,
) v1alpha1.ObjectReference {
	if ref == nil {
		return v1alpha1.ObjectReference{}
	}

	return v1alpha1.ObjectReference{
		APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: namespace,
		Name: ref.Name, UID: ref.UID, ResourceVersion: ref.ResourceVersion,
	}
}

func qualifiedWorkloadReferences(
	refs []v1alpha1.LocalResourceReference,
	namespace string,
) []v1alpha1.ObjectReference {
	result := make([]v1alpha1.ObjectReference, len(refs))
	for i := range refs {
		result[i] = qualifiedWorkloadReference(&refs[i], namespace)
	}

	return result
}

func workloadPodReference(pod *corev1.Pod) *v1alpha1.LocalResourceReference {
	return &v1alpha1.LocalResourceReference{
		APIVersion:      "v1",
		Kind:            "Pod",
		Name:            pod.Name,
		UID:             pod.UID,
		ResourceVersion: pod.ResourceVersion,
	}
}

func workloadObjectReference(
	apiVersion, kind, name string,
	uid types.UID,
	version string,
) *v1alpha1.LocalResourceReference {
	return &v1alpha1.LocalResourceReference{
		APIVersion: apiVersion, Kind: kind, Name: name, UID: uid, ResourceVersion: version,
	}
}

func localWorkloadReferences(refs []v1alpha1.ObjectReference) []v1alpha1.LocalResourceReference {
	result := make([]v1alpha1.LocalResourceReference, len(refs))
	for i, ref := range refs {
		result[i] = v1alpha1.LocalResourceReference{
			APIVersion:      ref.APIVersion,
			Kind:            ref.Kind,
			Name:            ref.Name,
			UID:             ref.UID,
			ResourceVersion: ref.ResourceVersion,
		}
	}

	return result
}

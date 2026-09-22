package app

import v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"

func qualifiedResourceReference(
	ref v1alpha1.LocalResourceReference,
	namespace string,
) v1alpha1.ObjectReference {
	return v1alpha1.ObjectReference{
		APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: namespace,
		Name: ref.Name, UID: ref.UID, ResourceVersion: ref.ResourceVersion,
	}
}

func localResourceReference(ref v1alpha1.ObjectReference) *v1alpha1.LocalResourceReference {
	return &v1alpha1.LocalResourceReference{
		APIVersion: ref.APIVersion, Kind: ref.Kind,
		Name: ref.Name, UID: ref.UID, ResourceVersion: ref.ResourceVersion,
	}
}

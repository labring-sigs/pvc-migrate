// Package v1alpha1 contains the Kubernetes API metadata for pvc-migrate.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "migrate.sealos.io", Version: "v1alpha1"}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		GroupVersion,
		&Migration{}, &MigrationList{},
		&PodMigration{}, &PodMigrationList{},
		&Reservation{}, &ReservationList{},
		&Copy{}, &CopyList{},
		&Backup{}, &BackupList{},
		&Restore{}, &RestoreList{},
		&Rename{}, &RenameList{},
		&Move{}, &MoveList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)

	return nil
}

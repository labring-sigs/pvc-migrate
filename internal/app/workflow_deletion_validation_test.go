package app

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// deletionValidationVolume returns one structurally valid planned volume whose
// source request form satisfies the identity checks of every kind.
func deletionValidationVolume(name string) v1alpha1.VolumeSpec {
	return v1alpha1.VolumeSpec{
		SourcePVC: v1alpha1.LocalResourceReference{
			Name: name,
			UID:  types.UID("source-" + name),
		},
		SourcePV: v1alpha1.LocalResourceReference{
			Name: "pv-" + name,
			UID:  types.UID("pv-" + name),
		},
		DestinationPVC: v1alpha1.LocalResourceReference{Name: "reserved-" + name},
		SourceCapacity: "1Gi",
		Capacity:       "1Gi",
		StorageClass:   "storage",
		VolumeMode:     corev1.PersistentVolumeFilesystem,
		AccessModes:    []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
	}
}

func deletionValidationRequest(name string) v1alpha1.VolumeRequest {
	return v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}}
}

func deletionValidationVolumes(
	names ...string,
) (specs []v1alpha1.VolumeSpec, requests []v1alpha1.VolumeRequest) {
	for _, name := range names {
		specs = append(specs, deletionValidationVolume(name))
		requests = append(requests, deletionValidationRequest(name))
	}

	return specs, requests
}

// runDeletionValidationCase asserts both halves of the deletion rule for one
// mutated workflow: the live object must fail validation, the deleting one
// must pass so the finalizer converges.
func runDeletionValidationCase[workflow metav1.Object](
	t *testing.T,
	live func() workflow,
	validate func(workflow) error,
) {
	t.Helper()

	if err := validate(live()); err == nil {
		t.Fatal("mutated spec of a live workflow passed validation")
	}

	deleting := live()
	deleting.SetDeletionTimestamp(&metav1.Time{})

	if err := validate(deleting); err != nil {
		t.Fatalf("deleting workflow failed validation: %v", err)
	}
}

// TestDeletionValidationForgivesSpecMutation proves the family property behind
// deletion convergence: a spec mutated after execution started must fail
// validation for a live workflow, yet pass for one converging to deletion so
// the finalizer can release. Every transfer kind follows the rule Rename,
// Move, Backup, and Restore already had.
func TestDeletionValidationForgivesSpecMutation(t *testing.T) {
	planned := v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned}

	copyObject := func(mutate func(*v1alpha1.CopySpec)) *v1alpha1.Copy {
		specs, requests := deletionValidationVolumes("a", "b")
		object := &v1alpha1.Copy{
			ObjectMeta: metav1.ObjectMeta{Name: "copy", Namespace: "data"},
			Spec:       v1alpha1.CopySpec{Volumes: requests},
			Status: v1alpha1.CopyStatus{
				WorkflowStatus: planned,
				Plan:           &v1alpha1.CopyPlan{Volumes: specs},
			},
		}
		mutate(&object.Spec)

		return object
	}

	clusterCopyObject := func(mutate func(*v1alpha1.ClusterCopySpec)) *v1alpha1.ClusterCopy {
		specs, requests := deletionValidationVolumes("a", "b")
		object := &v1alpha1.ClusterCopy{
			ObjectMeta: metav1.ObjectMeta{Name: "clustercopy"},
			Spec: v1alpha1.ClusterCopySpec{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				Volumes:              requests,
			},
			Status: v1alpha1.ClusterCopyStatus{
				WorkflowStatus: planned,
				Plan: &v1alpha1.ClusterCopyPlan{
					SourceNamespace:      "source",
					DestinationNamespace: "destination",
					Volumes:              specs,
				},
			},
		}
		mutate(&object.Spec)

		return object
	}

	migrationObject := func(mutate func(*v1alpha1.MigrationSpec)) *v1alpha1.Migration {
		specs, requests := deletionValidationVolumes("a", "b")
		object := &v1alpha1.Migration{
			ObjectMeta: metav1.ObjectMeta{Name: "migration", Namespace: "data"},
			Spec:       v1alpha1.MigrationSpec{Volumes: requests},
			Status: v1alpha1.MigrationStatus{
				WorkflowStatus: planned,
				Plan:           &v1alpha1.MigrationPlan{Volumes: specs},
			},
		}
		mutate(&object.Spec)

		return object
	}

	clusterMigrationObject := func(mutate func(*v1alpha1.ClusterMigrationSpec)) *v1alpha1.ClusterMigration {
		specs, requests := deletionValidationVolumes("a", "b")
		object := &v1alpha1.ClusterMigration{
			ObjectMeta: metav1.ObjectMeta{Name: "clustermigration"},
			Spec: v1alpha1.ClusterMigrationSpec{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				TemporaryNamespace:   "temporary",
				SessionNamespace:     "sessions",
				Volumes:              requests,
			},
			Status: v1alpha1.ClusterMigrationStatus{
				WorkflowStatus: planned,
				Plan: &v1alpha1.ClusterMigrationPlan{
					SourceNamespace:      "source",
					DestinationNamespace: "destination",
					TemporaryNamespace:   "temporary",
					SessionNamespace:     "sessions",
					Volumes:              specs,
				},
			},
		}
		mutate(&object.Spec)

		return object
	}

	podMigrationObject := func(mutate func(*v1alpha1.PodMigrationSpec)) *v1alpha1.PodMigration {
		specs, _ := deletionValidationVolumes("a", "b")
		object := &v1alpha1.PodMigration{
			ObjectMeta: metav1.ObjectMeta{Name: "podmigration", Namespace: "data"},
			Spec: v1alpha1.PodMigrationSpec{
				Pod:           v1alpha1.LocalResourceReference{Name: "web-0"},
				PrecopyPasses: 1,
			},
			Status: v1alpha1.PodMigrationStatus{
				WorkflowStatus: planned,
				Plan: &v1alpha1.PodMigrationPlan{
					Volumes: specs,
					Workload: v1alpha1.WorkloadSpec{
						Adapter: v1alpha1.WorkloadStatefulSet,
						Pod: &v1alpha1.LocalResourceReference{
							Name: "web-0",
							UID:  types.UID("pod-uid"),
						},
					},
					PrecopyPasses: 1,
				},
			},
		}
		mutate(&object.Spec)

		return object
	}

	reservationObject := func(mutate func(*v1alpha1.ReservationSpec)) *v1alpha1.Reservation {
		specs, requests := deletionValidationVolumes("a", "b")
		object := &v1alpha1.Reservation{
			ObjectMeta: metav1.ObjectMeta{Name: "reservation", Namespace: "data"},
			Spec:       v1alpha1.ReservationSpec{Volumes: requests},
			Status: v1alpha1.ReservationStatus{
				WorkflowStatus: planned,
				Plan:           &v1alpha1.ReservationPlan{Volumes: specs},
			},
		}
		mutate(&object.Spec)

		return object
	}

	clusterReservationObject := func(mutate func(*v1alpha1.ClusterReservationSpec)) *v1alpha1.ClusterReservation {
		specs, requests := deletionValidationVolumes("a", "b")
		object := &v1alpha1.ClusterReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "clusterreservation"},
			Spec: v1alpha1.ClusterReservationSpec{
				SourceNamespace:      "source",
				DestinationNamespace: "destination",
				SessionNamespace:     "sessions",
				Volumes:              requests,
			},
			Status: v1alpha1.ClusterReservationStatus{
				WorkflowStatus: planned,
				Plan: &v1alpha1.ClusterReservationPlan{
					SourceNamespace:      "source",
					DestinationNamespace: "destination",
					SessionNamespace:     "sessions",
					Volumes:              specs,
				},
			},
		}
		mutate(&object.Spec)

		return object
	}

	t.Run("copy requested volume identity", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.Copy {
			return copyObject(func(spec *v1alpha1.CopySpec) {
				spec.Volumes = append(spec.Volumes, deletionValidationRequest("c"))
			})
		}, validateCopyObject)
	})

	t.Run("copy online mode", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.Copy {
			return copyObject(func(spec *v1alpha1.CopySpec) { spec.Online = true })
		}, validateCopyObject)
	})

	t.Run("cluster copy namespace roles", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.ClusterCopy {
			return clusterCopyObject(func(spec *v1alpha1.ClusterCopySpec) {
				spec.DestinationNamespace = "elsewhere"
			})
		}, validateClusterCopyObject)
	})

	t.Run("cluster copy requested volume identity", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.ClusterCopy {
			return clusterCopyObject(func(spec *v1alpha1.ClusterCopySpec) {
				spec.Volumes = append(spec.Volumes, deletionValidationRequest("c"))
			})
		}, validateClusterCopyObject)
	})

	t.Run("migration requested volume identity", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.Migration {
			return migrationObject(func(spec *v1alpha1.MigrationSpec) {
				spec.Volumes = append(spec.Volumes, deletionValidationRequest("c"))
			})
		}, validateMigrationObject)
	})

	t.Run("cluster migration namespace roles", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.ClusterMigration {
			return clusterMigrationObject(func(spec *v1alpha1.ClusterMigrationSpec) {
				spec.DestinationNamespace = "elsewhere"
			})
		}, validateClusterMigrationObject)
	})

	t.Run("pod migration precopy passes", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.PodMigration {
			return podMigrationObject(func(spec *v1alpha1.PodMigrationSpec) {
				spec.PrecopyPasses = 2
			})
		}, validatePodMigrationObject)
	})

	t.Run("pod migration pod selector", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.PodMigration {
			return podMigrationObject(func(spec *v1alpha1.PodMigrationSpec) {
				spec.Pod.Name = "other-0"
			})
		}, validatePodMigrationObject)
	})

	t.Run("reservation requested volume identity", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.Reservation {
			return reservationObject(func(spec *v1alpha1.ReservationSpec) {
				spec.Volumes = append(spec.Volumes, deletionValidationRequest("c"))
			})
		}, validateReservationObject)
	})

	t.Run("cluster reservation namespace roles", func(t *testing.T) {
		runDeletionValidationCase(t, func() *v1alpha1.ClusterReservation {
			return clusterReservationObject(func(spec *v1alpha1.ClusterReservationSpec) {
				spec.SourceNamespace = "elsewhere"
			})
		}, validateClusterReservationObject)
	})
}

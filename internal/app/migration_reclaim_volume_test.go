package app

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// reclaimFixture builds a planned volume plus the live objects the reclaim
// helpers inspect. sourcePresent models the source-identity probes: when the
// PVC is absent the staged destination may be the only surviving copy.
type reclaimFixture struct {
	phase         v1alpha1.WorkflowPhase
	deleteUnused  bool
	sourcePresent bool
	reserved      bool
	activePVC     bool
}

func (f reclaimFixture) run(t *testing.T) []reclaimVolume {
	t.Helper()

	var objects []runtime.Object
	if f.sourcePresent {
		objects = append(objects, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "apps", Name: "src", UID: types.UID("src-uid"),
			},
		})
	}

	client := fake.NewSimpleClientset(objects...)

	planned := v1alpha1.VolumeSpec{
		SourcePVC: v1alpha1.LocalResourceReference{
			Name: "src", UID: types.UID("src-uid"),
		},
		SourcePV: v1alpha1.LocalResourceReference{
			Name: "pv-src", UID: types.UID("pv-src-uid"),
		},
		DestinationPVC:      v1alpha1.LocalResourceReference{Name: "staged"},
		SourceReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
	}

	checkpoint := v1alpha1.ClusterVolumeReservationStatus{
		DestinationPolicy: corev1.PersistentVolumeReclaimDelete,
		DestinationPVC: &v1alpha1.ObjectReference{
			Kind: "PersistentVolumeClaim", Namespace: "temporary",
			Name: "staged", UID: types.UID("staged-uid"),
		},
		DestinationPV: &v1alpha1.ObjectReference{
			Kind: "PersistentVolume", Name: "pv-staged", UID: types.UID("pv-staged-uid"),
		},
	}
	checkpoint.Reserved = f.reserved

	var active *v1alpha1.ObjectReference
	if f.activePVC {
		active = &v1alpha1.ObjectReference{
			Kind: "PersistentVolumeClaim", Namespace: "apps",
			Name: "src-new", UID: types.UID("active-uid"),
		}
	}

	_, volumes, err := prepareMigrationReclaimVolume(
		t.Context(), client, "workflow", f.phase,
		"apps", "temporary", planned, checkpoint, active, f.deleteUnused,
	)
	if err != nil {
		t.Fatalf("prepareMigrationReclaimVolume: %v", err)
	}

	return volumes
}

func findVolume(volumes []reclaimVolume, name string) *reclaimVolume {
	for i := range volumes {
		if volumes[i].pv.Name == name || volumes[i].pvc.Name == name {
			return &volumes[i]
		}
	}

	return nil
}

// TestPrepareMigrationReclaimVolume covers every terminal state and policy
// combination: the in-use copy is always kept, unused copies follow the
// policy, and a missing source identity keeps everything.
func TestPrepareMigrationReclaimVolume(t *testing.T) {
	const (
		keepSrc   = "pv-src"
		keepStged = "staged"
	)

	t.Run("completed keeps the migrated destination", func(t *testing.T) {
		for _, policy := range []bool{false, true} {
			volumes := reclaimFixture{
				phase: domain.PhaseCompleted, deleteUnused: policy, activePVC: true,
			}.run(t)

			destination := findVolume(volumes, "src-new")
			if destination == nil || destination.role != kube.ResourceRoleActive ||
				destination.delete {
				t.Fatalf("policy=%v active destination must be kept: %+v", policy, destination)
			}

			source := findVolume(volumes, keepSrc)
			if source == nil || source.delete != policy {
				t.Fatalf("policy=%v old source PV delete mismatch: %+v", policy, source)
			}

			if source.role != kube.ResourceRoleRollback {
				t.Fatalf("old source PV role = %s", source.role)
			}
		}
	})

	t.Run("aborted keeps the source and follows policy for the staged copy", func(t *testing.T) {
		for _, policy := range []bool{false, true} {
			volumes := reclaimFixture{
				phase:         domain.PhaseAborted,
				deleteUnused:  policy,
				sourcePresent: true,
				reserved:      true,
			}.run(t)

			staged := findVolume(volumes, keepStged)
			if staged == nil || staged.delete != policy {
				t.Fatalf("policy=%v staged destination delete mismatch: %+v", policy, staged)
			}

			source := findVolume(volumes, "src")
			if source == nil || source.delete {
				t.Fatalf("policy=%v aborted source must be kept: %+v", policy, source)
			}
		}
	})

	t.Run(
		"rolled back keeps the source and follows policy for the staged copy",
		func(t *testing.T) {
			for _, policy := range []bool{false, true} {
				volumes := reclaimFixture{
					phase:         domain.PhaseRolledBack,
					deleteUnused:  policy,
					sourcePresent: true,
					reserved:      true,
				}.run(t)

				staged := findVolume(volumes, keepStged)
				if staged == nil || staged.delete != policy {
					t.Fatalf(
						"policy=%v rolled-back staged destination delete mismatch: %+v",
						policy,
						staged,
					)
				}

				source := findVolume(volumes, "src")
				if source == nil || source.delete {
					t.Fatalf("policy=%v rolled-back source must be kept: %+v", policy, source)
				}
			}
		},
	)

	t.Run("aborted without the source identity keeps everything", func(t *testing.T) {
		volumes := reclaimFixture{
			phase: domain.PhaseAborted, deleteUnused: true,
			sourcePresent: false, reserved: true,
		}.run(t)

		for _, v := range volumes {
			if v.delete {
				t.Fatalf("missing source must keep every copy: %+v", v)
			}
		}
	})

	t.Run("aborted unreserved without activation keeps everything", func(t *testing.T) {
		// The failed-migration shape: no reservation checkpoint, no active
		// identity — the staged destination may be the only surviving copy.
		volumes := reclaimFixture{
			phase: domain.PhaseAborted, deleteUnused: true,
			sourcePresent: false, reserved: false,
		}.run(t)

		for _, v := range volumes {
			if v.delete {
				t.Fatalf("unresolvable source must keep every copy: %+v", v)
			}
		}
	})

	t.Run("failed keeps the source and ignores the policy", func(t *testing.T) {
		volumes := reclaimFixture{
			phase: domain.PhaseFailed, deleteUnused: true,
			sourcePresent: true, reserved: true,
		}.run(t)

		source := findVolume(volumes, "src")
		if source == nil || source.delete {
			t.Fatalf("failed source must be kept: %+v", source)
		}
	})
}

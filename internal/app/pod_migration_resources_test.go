package app

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func podSharedMountFixture() v1alpha1.SharedMountStatus {
	return v1alpha1.SharedMountStatus{
		SourcePV:       v1alpha1.LocalResourceReference{Name: "pv-a", UID: "pv-a"},
		LVMVolume:      v1alpha1.ObjectReference{Namespace: "openebs", Name: "lvm-a", UID: "lvm-a"},
		PreviousShared: "no", PreviousSharedSet: true,
	}
}

func TestPodMigrationReservationRestoresMountsBeforeProvisioning(t *testing.T) {
	executor, object, store, reserver := namespacedPodMigrationFixture(
		t,
		func(o *v1alpha1.PodMigration) {
			o.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{podSharedMountFixture()}
		},
	)
	manager := &recordingOpenEBSLVMSharedVolumeManager{}
	executor.sharedVolumes = manager
	failure := errors.New("recovery checkpoint rejected")

	store.failAt, store.err = 1, failure
	if err := executor.Reserve(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if len(object.Status.OpenEBSLVMSharedMounts) != 1 || len(reserver.calls) != 0 ||
		!reflect.DeepEqual(manager.restorePVs, []string{"pv-a"}) {
		t.Fatal("failed compensation checkpoint advanced provisioning or lost recovery data")
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	reserver.reserve = func(string, *v1alpha1.ClusterVolumeReservationStatus) error {
		if len(loaded.Status.OpenEBSLVMSharedMounts) != 0 || len(manager.restorePVs) != 2 {
			t.Fatal("provisioning started before recovery was persisted")
		}
		return nil
	}

	if err := executor.Reserve(t.Context(), loaded); err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Phase != domain.PhaseReserved ||
		len(loaded.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatalf("reservation did not recover: %+v", loaded.Status)
	}
}

func TestPodMigrationReservationRetriesDestinationSharingWithoutReprovisioning(t *testing.T) {
	executor, object, _, reserver := namespacedPodMigrationFixture(
		t,
		func(o *v1alpha1.PodMigration) {
			o.Spec.OpenEBSLVMEnableShared = true

			o.Status.Plan.OpenEBSLVMEnableShared = true
			for i := range o.Status.Plan.Volumes {
				o.Status.Plan.Volumes[i].ConcurrentConsumers = 2
			}
		},
	)
	for _, name := range []string{"destination-a", "destination-b"} {
		if _, err := executor.client.CoreV1().
			PersistentVolumes().
			Create(t.Context(), &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: "destination-pv"},
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
					},
				},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	failure := errors.New("LVMVolume update unavailable")
	manager := &recordingOpenEBSLVMSharedVolumeManager{ensureErr: failure}

	executor.sharedVolumes = manager
	if err := executor.Reserve(t.Context(), object); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseFailed ||
		object.Status.ResumeFrom != domain.PhaseReserving ||
		!object.Status.Volumes[0].Reserved ||
		!object.Status.Volumes[1].Reserved {
		t.Fatalf("sharing failure lost reservation checkpoints: %+v", object.Status)
	}

	manager.ensureErr = nil

	if err := executor.Reserve(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if object.Status.Phase != domain.PhaseReserved ||
		!reflect.DeepEqual(reserver.calls, []string{"a", "b"}) {
		t.Fatalf(
			"sharing retry reprovisioned storage: calls=%v",
			reserver.calls,
		)
	}
}

func TestPodMigrationRejectsUnrelatedSharedMountsBeforeLock(t *testing.T) {
	for _, mutation := range []func(*v1alpha1.SharedMountStatus){
		func(m *v1alpha1.SharedMountStatus) { m.SourcePV.UID = "replaced" },
		func(m *v1alpha1.SharedMountStatus) { m.SourcePV.Name = "unplanned" },
		func(m *v1alpha1.SharedMountStatus) { m.LVMVolume.UID = "" },
	} {
		mount := podSharedMountFixture()
		mutation(&mount)

		executor, object, _, _ := namespacedPodMigrationFixture(t)
		executor.locker = nil
		object.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{mount}
		err := executor.Reserve(t.Context(), object)

		if domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("invalid shared mount reached execution: %v", err)
		}
	}
}

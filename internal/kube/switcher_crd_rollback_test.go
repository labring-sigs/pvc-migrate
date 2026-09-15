package kube

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func TestRollbackPVCRecoversFailedCheckpoint(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)
	bindings := testPVCTransferBindings(volume)

	checkpoint := &v1alpha1.ClusterVolumeActivationStatus{}
	if err := switcher.ActivatePVC(t.Context(), session.ID, bindings,
		testMigrationPVCManifest(t, session.ID, volume), checkpoint, nil); err != nil {
		t.Fatal(err)
	}

	desired := BoundPVCManifest(session.ID, volume.SourcePVC, volume.SourcePV.Name,
		volume.SourcePVCSpec, volume.SourcePVCMetadata)
	desired.Annotations[RollbackPVAnnotation] = volume.DestinationPV.Name
	persisted := checkpoint.DeepCopy()
	failed := errors.New("checkpoint failed")

	err := switcher.RollbackPVC(t.Context(), session.ID, bindings, desired, checkpoint,
		func() error { return failed })
	if !errors.Is(err, failed) || !reflect.DeepEqual(checkpoint, persisted) {
		t.Fatalf("failed save advanced rollback checkpoint: %v; %+v", err, checkpoint)
	}

	if err := switcher.RollbackPVC(
		t.Context(),
		session.ID,
		bindings,
		desired,
		checkpoint,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if checkpoint.RolledBackAt == nil || checkpoint.ActivePVC == nil ||
		checkpoint.ActivePVC.UID == "" {
		t.Fatalf("incomplete rollback checkpoint: %+v", checkpoint)
	}

	restored := checkpoint.ActivePVC.UID
	if err := switcher.RollbackPVC(
		t.Context(),
		session.ID,
		bindings,
		desired,
		checkpoint,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if checkpoint.ActivePVC.UID != restored {
		t.Fatal("repeated rollback replaced the restored PVC")
	}
}

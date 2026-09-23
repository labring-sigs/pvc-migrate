package kube

import (
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRollbackPVCRecoversFailedCheckpoint(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)
	bindings := testPVCTransferBindings(volume)

	checkpoint := &v1alpha1.ClusterVolumeActivationStatus{}
	if err := switcher.ActivatePVC(t.Context(), session.ID, volume.SourcePVC.Namespace, bindings,
		testMigrationPVCManifest(t, session.ID, volume), checkpoint, nil); err != nil {
		t.Fatal(err)
	}

	desired := BoundPVCManifest(session.ID, volume.SourcePVC, volume.SourcePV.Name,
		volume.SourcePVCSpec, volume.SourcePVCMetadata)
	desired.Annotations[RollbackPVAnnotation] = volume.DestinationPV.Name
	persisted := checkpoint.DeepCopy()
	failed := errors.New("checkpoint failed")

	err := switcher.RollbackPVC(
		t.Context(),
		session.ID,
		volume.SourcePVC.Namespace,
		bindings,
		desired,
		checkpoint,
		func() error { return failed },
	)
	if !errors.Is(err, failed) || !reflect.DeepEqual(checkpoint, persisted) {
		t.Fatalf("failed save advanced rollback checkpoint: %v; %+v", err, checkpoint)
	}

	if err := switcher.RollbackPVC(
		t.Context(),
		session.ID,
		volume.SourcePVC.Namespace,
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
		volume.SourcePVC.Namespace,
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

func TestRollbackPVCRetiresCrossNamespaceActiveClaim(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)

	const destinationNamespace = "landing"

	bindings := testPVCTransferBindings(volume)

	activated := testMigrationPVCManifest(t, session.ID, volume)
	activated.Namespace = destinationNamespace

	checkpoint := &v1alpha1.ClusterVolumeActivationStatus{}
	if err := switcher.ActivatePVC(
		t.Context(),
		session.ID,
		destinationNamespace,
		bindings,
		activated,
		checkpoint,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	desired := BoundPVCManifest(session.ID, volume.SourcePVC, volume.SourcePV.Name,
		volume.SourcePVCSpec, volume.SourcePVCMetadata)
	desired.Annotations[RollbackPVAnnotation] = volume.DestinationPV.Name

	if err := switcher.RollbackPVC(
		t.Context(),
		session.ID,
		destinationNamespace,
		bindings,
		desired,
		checkpoint,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(destinationNamespace).
		Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cross-namespace active claim survived rollback: %v", err)
	}

	restored, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.SourcePVC.Namespace).
		Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("source claim was not restored: %v", err)
	}

	if restored.Spec.VolumeName != volume.SourcePV.Name {
		t.Fatalf("restored claim has the wrong binding: %+v", restored.Spec)
	}

	if checkpoint.RolledBackAt == nil ||
		checkpoint.ActivePVC.Namespace != volume.SourcePVC.Namespace {
		t.Fatalf("rollback checkpoint not recorded: %+v", checkpoint)
	}
}

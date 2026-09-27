package kube

import (
	"errors"
	"reflect"
	"strconv"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestActivatePVCRecoversEveryCRDCheckpointFailure(t *testing.T) {
	for failure := 1; failure <= 4; failure++ {
		t.Run(strconv.Itoa(failure), func(t *testing.T) {
			switcher, session, volume, _ := switcherFixture(t)
			bindings := testPVCTransferBindings(volume)
			desired := testMigrationPVCManifest(t, session.ID, volume)
			checkpoint := &v1alpha1.ClusterVolumeActivationStatus{}
			persisted := checkpoint.DeepCopy()
			calls := 0
			failed := errors.New("checkpoint failed")

			err := switcher.ActivatePVC(
				t.Context(),
				session.ID,
				volume.SourcePVC.Namespace,
				bindings,
				desired,
				checkpoint,
				func() error {
					calls++
					if calls == failure {
						return failed
					}

					persisted = checkpoint.DeepCopy()

					return nil
				},
			)
			if !errors.Is(err, failed) || !reflect.DeepEqual(checkpoint, persisted) {
				t.Fatalf(
					"failed save advanced local checkpoint: %v; %+v != %+v",
					err,
					checkpoint,
					persisted,
				)
			}

			if err := switcher.ActivatePVC(
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

			if checkpoint.ActivePVC == nil || checkpoint.ActivePVC.UID == "" ||
				checkpoint.ActivatedAt == nil ||
				!checkpoint.TemporaryPVCDeleted ||
				!checkpoint.SourcePVCDeleted ||
				!checkpoint.DestinationReserved {
				t.Fatalf("incomplete activation: %+v", checkpoint)
			}

			before := checkpoint.DeepCopy()
			if err := switcher.ActivatePVC(
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

			if checkpoint.ActivePVC.UID != before.ActivePVC.UID {
				t.Fatal("repeated activation replaced the active PVC")
			}
		})
	}
}

func TestActivatePVCRejectsInvalidManifestBeforeResourceAccess(t *testing.T) {
	for _, mutate := range []struct {
		name   string
		change func(*corev1.PersistentVolumeClaim)
	}{
		{"name", func(pvc *corev1.PersistentVolumeClaim) { pvc.Name = "another" }},
		{"namespace", func(pvc *corev1.PersistentVolumeClaim) { pvc.Namespace = "another" }},
		{"binding", func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.VolumeName = "another" }},
		{"ownership", func(pvc *corev1.PersistentVolumeClaim) { delete(pvc.Labels, SessionKey) }},
		{"capacity", func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.Resources.Requests = nil }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			switcher, session, volume, _ := switcherFixture(t)
			desired := testMigrationPVCManifest(t, session.ID, volume)
			mutate.change(desired)

			client := testutil.MustType[*fake.Clientset](t, switcher.client)
			client.ClearActions()

			if err := switcher.ActivatePVC(
				t.Context(),
				session.ID,
				volume.SourcePVC.Namespace,
				testPVCTransferBindings(volume),
				desired,
				&v1alpha1.ClusterVolumeActivationStatus{},
				nil,
			); err == nil {
				t.Fatal("invalid manifest accepted")
			}

			if len(client.Actions()) != 0 {
				t.Fatalf("invalid input accessed resources: %v", client.Actions())
			}
		})
	}
}

func TestActivatePVCLostLeaseAfterDeleteStopsCutover(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)
	client := testutil.MustType[*fake.Clientset](t, switcher.client)
	lost := errors.New("lease lost")
	fence := &testLeaseFence{}
	client.PrependReactor(
		"delete",
		"persistentvolumeclaims",
		func(ktesting.Action) (bool, runtime.Object, error) {
			fence.err = lost
			return false, nil, nil
		},
	)

	checkpoint := &v1alpha1.ClusterVolumeActivationStatus{}

	err := switcher.ActivatePVC(
		WithLeaseFence(t.Context(), fence),
		session.ID,
		volume.SourcePVC.Namespace,
		testPVCTransferBindings(volume),
		testMigrationPVCManifest(t, session.ID, volume),
		checkpoint,
		nil,
	)
	if !errors.Is(err, lost) {
		t.Fatalf("lost lease was ignored: %v", err)
	}

	if !reflect.DeepEqual(checkpoint, &v1alpha1.ClusterVolumeActivationStatus{}) {
		t.Fatalf("lost lease advanced checkpoint: %+v", checkpoint)
	}

	for _, action := range client.Actions() {
		if deletion, ok := action.(ktesting.DeleteAction); ok &&
			deletion.GetResource().Resource == "persistentvolumeclaims" &&
			deletion.GetName() == volume.SourcePVC.Name &&
			deletion.GetNamespace() == volume.SourcePVC.Namespace {
			t.Fatal("source was deleted after loss of lease")
		}
	}
}

func TestActivatePVCCrossNamespaceCutover(t *testing.T) {
	switcher, session, volume, _ := switcherFixture(t)

	const destinationNamespace = "landing"

	desired := testMigrationPVCManifest(t, session.ID, volume)
	desired.Namespace = destinationNamespace

	checkpoint := &v1alpha1.ClusterVolumeActivationStatus{}

	if err := switcher.ActivatePVC(
		t.Context(),
		session.ID,
		destinationNamespace,
		testPVCTransferBindings(volume),
		desired,
		checkpoint,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if checkpoint.ActivePVC == nil || checkpoint.ActivePVC.Namespace != destinationNamespace ||
		checkpoint.ActivePVC.Name != volume.SourcePVC.Name || checkpoint.ActivatedAt == nil {
		t.Fatalf("cross-namespace activation not recorded: %+v", checkpoint)
	}

	active, err := switcher.client.CoreV1().
		PersistentVolumeClaims(destinationNamespace).
		Get(t.Context(), volume.SourcePVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("activated PVC missing from destination namespace: %v", err)
	}

	if active.Spec.VolumeName != volume.DestinationPV.Name ||
		active.Labels[SessionKey] != session.ID {
		t.Fatalf("activated PVC has the wrong binding: %+v", active)
	}

	pv, err := switcher.client.CoreV1().
		PersistentVolumes().
		Get(t.Context(), volume.DestinationPV.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != destinationNamespace ||
		pv.Spec.ClaimRef.Name != volume.SourcePVC.Name {
		t.Fatalf(
			"destination PV claimRef was not reserved for the landing namespace: %+v",
			pv.Spec.ClaimRef,
		)
	}

	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(volume.DestinationPVC.Namespace).
		Get(t.Context(), volume.DestinationPVC.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("temporary claim survived cutover: %v", err)
	}

	// Every Retain pin must carry the recorded original reclaim policy in
	// the same update — the finalize path refuses to restore a pinned PV
	// without it.
	for _, pvName := range []string{volume.SourcePV.Name, volume.DestinationPV.Name} {
		pinned, err := switcher.client.CoreV1().
			PersistentVolumes().
			Get(t.Context(), pvName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if pinned.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			t.Fatalf("PV %s left unpinned: %+v", pvName, pinned.Spec)
		}

		if pinned.Annotations[OriginalPolicyAnnotation] == "" {
			t.Fatalf("PV %s pinned without its original reclaim policy", pvName)
		}
	}

	before := checkpoint.DeepCopy()
	if err := switcher.ActivatePVC(
		t.Context(),
		session.ID,
		destinationNamespace,
		testPVCTransferBindings(volume),
		desired,
		checkpoint,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if checkpoint.ActivePVC.UID != before.ActivePVC.UID {
		t.Fatal("repeated cross-namespace activation replaced the active PVC")
	}
}

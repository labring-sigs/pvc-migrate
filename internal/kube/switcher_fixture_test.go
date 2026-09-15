package kube

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// switcherSession is the minimal workflow identity the switcher tests need.
type switcherSession struct {
	ID string
}

// switcherVolume is one planned transfer volume with fully qualified
// references, as the switcher receives them via PVCTransferBindings.
type switcherVolume struct {
	SourcePVC         v1alpha1.ObjectReference
	SourcePV          v1alpha1.ObjectReference
	DestinationPVC    v1alpha1.ObjectReference
	DestinationPV     v1alpha1.ObjectReference
	Capacity          string
	StorageClass      string
	SourcePVCSpec     corev1.PersistentVolumeClaimSpec
	SourcePVCMetadata v1alpha1.PVCMetadata
}

func switcherFixture(
	t *testing.T,
) (*Switcher, *switcherSession, switcherVolume, *corev1.PersistentVolumeClaim) {
	t.Helper()

	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	pvcUID := v1alpha1.ObjectReference{
		APIVersion: "v1", Kind: "PersistentVolumeClaim",
		Namespace: "app", Name: "data", UID: "source-pvc-uid", ResourceVersion: "10",
	}
	pvUID := v1alpha1.ObjectReference{
		APIVersion: "v1", Kind: "PersistentVolume",
		Name: "pv-source", UID: "source-pv-uid", ResourceVersion: "20",
	}
	destinationPVC := v1alpha1.ObjectReference{
		APIVersion: "v1", Kind: "PersistentVolumeClaim",
		Namespace: "app", Name: "data-migrated", UID: "destination-pvc-uid", ResourceVersion: "1",
	}
	destinationPV := v1alpha1.ObjectReference{
		APIVersion: "v1", Kind: "PersistentVolume",
		Name: "pv-destination", UID: "destination-pv-uid", ResourceVersion: "2",
	}

	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "app",
			Name:            "data",
			UID:             "source-pvc-uid",
			ResourceVersion: "10",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-source", StorageClassName: &storageClass, VolumeMode: &mode,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	sourcePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pv-source",
			UID:             "source-pv-uid",
			ResourceVersion: "20",
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     "mig-fixture",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1", Kind: "PersistentVolumeClaim",
				Namespace: "app", Name: "data", UID: "source-pvc-uid",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	destinationPVObject := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pv-destination",
			UID:             "destination-pv-uid",
			ResourceVersion: "2",
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     "mig-fixture",
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			ClaimRef: &corev1.ObjectReference{
				APIVersion: "v1", Kind: "PersistentVolumeClaim",
				Namespace: "app", Name: "data-migrated", UID: "destination-pvc-uid",
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeAvailable},
	}

	// The reserved destination claim exists before cutover; the active claim
	// is (re)created by activation and rollback themselves.
	destinationPVCObject := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "app",
			Name:            "data-migrated",
			UID:             "destination-pvc-uid",
			ResourceVersion: "1",
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     "mig-fixture",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-destination", StorageClassName: &storageClass, VolumeMode: &mode,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
		sourcePV,
		destinationPVObject,
		destinationPVCObject,
	)
	// The fake clientset neither assigns UIDs nor runs the PV controller;
	// simulate both: claims gain a UID and bind, and PV claimRefs settle to
	// the live claim bound to them.
	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(ktesting.CreateAction)
			if !ok {
				return false, nil, nil
			}

			claim, ok := create.GetObject().(*corev1.PersistentVolumeClaim)
			if !ok {
				return false, nil, nil
			}

			if claim.UID == "" {
				claim.UID = types.UID(claim.Name + "-active-uid")
			}

			return false, nil, nil
		},
	)
	// Simulate the PV controller without re-entering the fake clientset
	// (reactors run under the clientset lock, so nested calls self-deadlock):
	// a claim bound to a PV settles the PV, and deleting the last bound claim
	// releases it.
	markBound := func(pv *corev1.PersistentVolume, claim *corev1.PersistentVolumeClaim) {
		if claim == nil || claim.Spec.VolumeName != pv.Name {
			return
		}

		pv.Spec.ClaimRef = &corev1.ObjectReference{
			APIVersion:      domain.CoreAPIVersion,
			Kind:            domain.KindPersistentVolumeClaim,
			Namespace:       claim.Namespace,
			Name:            claim.Name,
			UID:             claim.UID,
			ResourceVersion: claim.ResourceVersion,
		}
		pv.Status.Phase = corev1.VolumeBound
	}

	client.PrependReactor(
		"create",
		"persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(ktesting.CreateAction)
			if !ok {
				return false, nil, nil
			}

			claim, ok := create.GetObject().(*corev1.PersistentVolumeClaim)
			if !ok {
				return false, nil, nil
			}

			if claim.UID == "" {
				claim.UID = types.UID(claim.Name + "-active-uid")
			}

			if claim.Spec.VolumeName != "" {
				claim.Status.Phase = corev1.ClaimBound
			}

			// Settle the PV this claim binds to: write the settled object
			// back through the tracker (Get returns a copy).
			if pv, err := client.Tracker().Get(
				corev1.SchemeGroupVersion.WithResource("persistentvolumes"),
				"",
				claim.Spec.VolumeName,
			); err == nil {
				typed, ok := pv.(*corev1.PersistentVolume)
				if !ok {
					return false, nil, nil
				}

				settled := typed.DeepCopy()
				markBound(settled, claim)

				if updErr := client.Tracker().Update(
					corev1.SchemeGroupVersion.WithResource("persistentvolumes"),
					settled,
					"",
				); updErr != nil {
					t.Log(updErr)
				}
			}

			return false, nil, nil
		},
	)

	volume := switcherVolume{
		SourcePVC:      pvcUID,
		SourcePV:       pvUID,
		DestinationPVC: destinationPVC,
		DestinationPV:  destinationPV,
		Capacity:       "1Gi",
		StorageClass:   storageClass,
		SourcePVCSpec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass, VolumeMode: &mode,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
	}

	return NewSwitcher(client), &switcherSession{ID: "mig-fixture"}, volume, sourcePVC
}

func testPVCTransferBindings(volume switcherVolume) PVCTransferBindings {
	return PVCTransferBindings{
		SourcePVC:      volume.SourcePVC,
		SourcePV:       volume.SourcePV,
		DestinationPVC: volume.DestinationPVC,
		DestinationPV:  volume.DestinationPV,
	}
}

func testMigrationPVCManifest(
	t *testing.T,
	sessionID string,
	volume switcherVolume,
) *corev1.PersistentVolumeClaim {
	t.Helper()

	// Activation renames the reserved destination claim into the source
	// identity: desired name/namespace follow the source PVC while the claim
	// binds the destination PV.
	manifest := BoundPVCManifest(
		sessionID,
		volume.SourcePVC,
		volume.DestinationPV.Name,
		volume.SourcePVCSpec,
		volume.SourcePVCMetadata,
	)

	return manifest
}

func configureRenameFixture(t *testing.T) (*Switcher, *switcherSession, switcherVolume) {
	t.Helper()
	switcher, session, volume, sourcePVC := switcherFixture(t)
	volume.DestinationPVC.Name = "data-renamed"

	// Rename rebinds the live source claim; seed it into the fake cluster.
	if _, err := switcher.client.CoreV1().
		PersistentVolumeClaims(sourcePVC.Namespace).
		Create(t.Context(), sourcePVC, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	return switcher, session, volume
}

func reserveSourceObjects() (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem

	pvcUID := types.UID("source-pvc-uid")

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "app",
			Name:            "data",
			UID:             pvcUID,
			ResourceVersion: "10",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-source", StorageClassName: &storageClass, VolumeMode: &mode,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "pv-source",
			UID:             "source-pv-uid",
			ResourceVersion: "20",
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			ClaimRef: &corev1.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       pvcUID,
			},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}

	return pvc, pv
}

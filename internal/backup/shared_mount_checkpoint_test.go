package backup

import (
	"context"
	"errors"
	"reflect"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBackupSharedMountSaveFailureAllowsRetry(t *testing.T) {
	saveErr := errors.New("checkpoint unavailable")
	store := &backupCheckpointStore{err: saveErr}
	manager := &recordingBackupOpenEBSManager{prepared: kube.OpenEBSLVMSharedResult{
		NeedsChange: true,
		LVMVolume:   v1alpha1.ObjectReference{Namespace: "openebs", Name: "lvm", UID: "lvm-uid"},
	}}
	session := plannedBackupObject()
	save := func(ctx context.Context) error { return store.Save(ctx, session) }

	info := &PVCInfo{
		PVC: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"},
		},
		PV: &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: "pv-uid"},
			Spec: corev1.PersistentVolumeSpec{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: kube.OpenEBSLVMCSIDriver},
			},
		},
		Consumers: []string{"app/writer"},
	}
	if err := prepareBackupSharedMounts(
		context.Background(),
		manager,
		session.Name,
		true,
		&session.Status.OpenEBSLVMSharedMounts,
		save,
		info,
	); !errors.Is(
		err,
		saveErr,
	) {
		t.Fatalf("save error = %v", err)
	}

	if session.Status.OpenEBSLVMSharedMounts != nil || len(manager.enabled) != 0 {
		t.Fatal("failed checkpoint changed local recovery state or enabled the mount")
	}

	store.err = nil

	if err := prepareBackupSharedMounts(
		t.Context(),
		manager,
		session.Name,
		true,
		&session.Status.OpenEBSLVMSharedMounts,
		save,
		info,
	); err != nil {
		t.Fatal(err)
	}

	if store.writes != 2 || len(manager.enabled) != 1 ||
		len(store.object.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatal("retry did not persist before enabling")
	}

	previous := append(
		[]v1alpha1.SharedMountStatus(nil),
		session.Status.OpenEBSLVMSharedMounts...,
	)

	store.err = saveErr
	if err := kube.RestoreSharedMounts(
		context.Background(), manager, session.Name, &session.Status.OpenEBSLVMSharedMounts,
		save, nil,
	); !errors.Is(
		err,
		saveErr,
	) {
		t.Fatalf("restore save error = %v", err)
	}

	if !reflect.DeepEqual(previous, session.Status.OpenEBSLVMSharedMounts) {
		t.Fatal("failed restore checkpoint lost recovery state")
	}

	store.err = nil

	if err := kube.RestoreSharedMounts(
		context.Background(), manager, session.Name, &session.Status.OpenEBSLVMSharedMounts,
		save, nil,
	); err != nil {
		t.Fatal(err)
	}

	if len(session.Status.OpenEBSLVMSharedMounts) != 0 || len(manager.restored) != 2 {
		t.Fatal("restore retry did not converge")
	}
}

func TestBackupSharedMountRestoreRequiresDependencies(t *testing.T) {
	for _, missing := range []string{"manager", "store"} {
		t.Run(missing, func(t *testing.T) {
			session := plannedBackupObject()
			session.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{{}}
			manager := &recordingBackupOpenEBSManager{}

			store := &backupCheckpointStore{object: session.DeepCopy()}

			var dependency kube.OpenEBSLVMSharedVolumeManager = manager

			save := func(ctx context.Context) error { return store.Save(ctx, session) }
			if missing == "manager" {
				dependency = nil
			} else {
				save = nil
			}

			if err := kube.RestoreSharedMounts(
				context.Background(), dependency, session.Name,
				&session.Status.OpenEBSLVMSharedMounts,
				save, nil,
			); err == nil {
				t.Fatal("missing recovery dependency accepted")
			}

			if len(manager.restored) != 0 || len(session.Status.OpenEBSLVMSharedMounts) != 1 ||
				store.writes != 0 {
				t.Fatal("missing dependency changed recovery state")
			}
		})
	}
}

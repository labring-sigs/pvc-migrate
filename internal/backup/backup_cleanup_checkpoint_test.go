package backup

import (
	"context"
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type cleanupRepositoryResources struct{ cleaned bool }

func (*cleanupRepositoryResources) ValidateOwnedCleanup(
	context.Context, crclient.ObjectKey, v1alpha1.ObjectReference,
) error {
	return nil
}

func (r *cleanupRepositoryResources) CleanupOwned(
	context.Context, crclient.ObjectKey, v1alpha1.ObjectReference,
) error {
	r.cleaned = true
	return nil
}

func TestBackupCleanupCheckpointsMountRecoveryBeforeDeletingRepository(t *testing.T) {
	object := plannedBackupObject()
	object.Status.Phase = domain.PhaseCompleted
	object.Status.OpenEBSLVMSharedMounts = []v1alpha1.SharedMountStatus{{
		SourcePV: v1alpha1.LocalResourceReference{Name: "pv", UID: "pv"},
		LVMVolume: v1alpha1.ObjectReference{
			Namespace: "storage", Name: "volume", UID: "lvm",
		},
		PreviousShared: "no", PreviousSharedSet: true,
	}}
	failure := errors.New("checkpoint unavailable")
	store := &backupCheckpointStore{object: object.DeepCopy(), err: failure}
	manager := &recordingBackupOpenEBSManager{}
	resources := &cleanupRepositoryResources{}
	executor := NewBackupExecutor(fake.NewClientset(), store,
		backupExecutorLocker{&recordingBackupSessionLock{}}, "app",
		BackupExecutorConfig{SharedVolumeManager: manager, RepositoryResources: resources})
	options := BackupCleanupOptions{Finalize: true, DeleteSession: true}

	if err := executor.Cleanup(t.Context(), object, options); !errors.Is(err, failure) {
		t.Fatalf("cleanup error: %v", err)
	}

	if resources.cleaned || store.deleted || len(object.Status.OpenEBSLVMSharedMounts) != 1 {
		t.Fatal("failed recovery checkpoint discarded resources or retry state")
	}

	store.err = nil
	if err := executor.Cleanup(t.Context(), object, options); err != nil {
		t.Fatal(err)
	}

	if !resources.cleaned || !store.deleted || len(manager.restored) != 2 ||
		len(object.Status.OpenEBSLVMSharedMounts) != 0 {
		t.Fatal("retry did not finish recovery before cleanup")
	}
}

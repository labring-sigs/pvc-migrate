package backup

import (
	"context"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// recordingBackupOpenEBSManager is the shared-volume manager double used by
// backup checkpoint tests: it reports one prepared change and records whether
// the temporary shared setting was restored.
type recordingBackupOpenEBSManager struct {
	prepared kube.OpenEBSLVMSharedResult
	enabled  []v1alpha1.SharedMountStatus
	restored []v1alpha1.SharedMountStatus
}

func (m *recordingBackupOpenEBSManager) Shared(
	context.Context,
	v1alpha1.ObjectReference,
	v1alpha1.ObjectReference,
	string,
) (bool, error) {
	return false, nil
}

func (m *recordingBackupOpenEBSManager) PrepareShared(
	context.Context,
	v1alpha1.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return m.prepared, nil
}

func (m *recordingBackupOpenEBSManager) EnsureShared(
	context.Context,
	v1alpha1.ObjectReference,
	v1alpha1.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return m.prepared, nil
}

func (m *recordingBackupOpenEBSManager) EnableShared(
	_ context.Context,
	_ string,
	mount v1alpha1.SharedMountStatus,
) error {
	m.enabled = append(m.enabled, mount)
	return nil
}

func (m *recordingBackupOpenEBSManager) ValidateRestoreShared(
	context.Context,
	string,
	v1alpha1.SharedMountStatus,
) error {
	return nil
}

func (m *recordingBackupOpenEBSManager) RestoreShared(
	_ context.Context,
	_ string,
	mount v1alpha1.SharedMountStatus,
) error {
	m.restored = append(m.restored, mount)
	return nil
}

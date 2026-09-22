package backup

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBackupFinalizeDeletedRecoversFailedStatusWithoutResumePhase(t *testing.T) {
	object := plannedBackupObject()
	object.DeletionTimestamp = func() *metav1.Time { now := metav1.Now(); return &now }()
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = ""
	store := &backupCheckpointStore{object: object.DeepCopy()}
	lock := &recordingBackupSessionLock{}
	executor := NewBackupExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{lock},
		"app",
		BackupExecutorConfig{},
	)

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !store.deleted || !lock.released || object.Status.Phase != domain.PhaseAborted ||
		store.object.Status.Phase != domain.PhaseAborted {
		t.Fatalf(
			"backup deletion did not converge: deleted=%v released=%v status=%+v",
			store.deleted,
			lock.released,
			object.Status,
		)
	}
}

func TestRestoreFinalizeDeletedRecoversFailedStatusWithoutResumePhase(t *testing.T) {
	object := plannedRestoreObject()
	object.DeletionTimestamp = func() *metav1.Time { now := metav1.Now(); return &now }()
	object.Status.Phase = domain.PhaseFailed
	object.Status.ResumeFrom = ""
	store := &restoreCheckpointStore{object: object.DeepCopy()}
	lock := &recordingBackupSessionLock{}
	executor := NewRestoreExecutor(
		fake.NewClientset(),
		store,
		backupExecutorLocker{lock},
		"app",
		RestoreExecutorConfig{},
	)

	if err := executor.FinalizeDeleted(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	if !store.deleted || !lock.released || object.Status.Phase != domain.PhaseAborted ||
		store.object.Status.Phase != domain.PhaseAborted {
		t.Fatalf(
			"restore deletion did not converge: deleted=%v released=%v status=%+v",
			store.deleted,
			lock.released,
			object.Status,
		)
	}
}

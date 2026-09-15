package backup

import (
	"errors"
	"reflect"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBackupResumeRequestDoesNotReadRepository(t *testing.T) {
	for _, saveFailure := range []bool{false, true} {
		object := plannedBackupObject()
		object.Status.Phase, object.Status.ResumeFrom = domain.PhaseFailed, domain.PhasePlanned
		before := object.DeepCopy()

		store := &backupCheckpointStore{object: object.DeepCopy()}
		if saveFailure {
			store.err = errors.New("checkpoint failed")
		}

		client := fake.NewClientset()
		executor := NewBackupExecutor(
			client,
			store,
			backupExecutorLocker{&recordingBackupSessionLock{}},
			"app",
			BackupExecutorConfig{},
		)

		err := executor.RequestResume(t.Context(), object)
		if saveFailure {
			if !errors.Is(err, store.err) || !reflect.DeepEqual(object, before) ||
				!reflect.DeepEqual(store.object, before) {
				t.Fatalf("failed checkpoint changed resume state: %v", err)
			}
		} else if err != nil || store.object.Status.Phase != domain.PhasePlanned {
			t.Fatalf("resume request failed: %v", err)
		}

		if len(client.Actions()) != 0 {
			t.Fatalf("resume request accessed controller resources: %v", client.Actions())
		}
	}
}

func TestRestoreResumeRequestDoesNotReadRepository(t *testing.T) {
	for _, saveFailure := range []bool{false, true} {
		object := plannedRestoreObject()
		object.Status.Phase, object.Status.ResumeFrom = domain.PhaseFailed, domain.PhasePlanned
		before := object.DeepCopy()

		store := &restoreCheckpointStore{object: object.DeepCopy()}
		if saveFailure {
			store.err = errors.New("checkpoint failed")
		}

		client := fake.NewClientset()
		executor := NewRestoreExecutor(
			client,
			store,
			backupExecutorLocker{&recordingBackupSessionLock{}},
			"app",
			RestoreExecutorConfig{},
		)

		err := executor.RequestResume(t.Context(), object)
		if saveFailure {
			if !errors.Is(err, store.err) || !reflect.DeepEqual(object, before) ||
				!reflect.DeepEqual(store.object, before) {
				t.Fatalf("failed checkpoint changed resume state: %v", err)
			}
		} else if err != nil || store.object.Status.Phase != domain.PhasePlanned {
			t.Fatalf("resume request failed: %v", err)
		}

		if len(client.Actions()) != 0 {
			t.Fatalf("resume request accessed controller resources: %v", client.Actions())
		}
	}
}

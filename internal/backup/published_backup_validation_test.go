package backup

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBackupRepositoryValidationResumesPublishedCRDPlan(t *testing.T) {
	object := plannedBackupObject()
	repository := publishedBackupRepository(object)
	client := fake.NewClientset()

	executor := NewBackupExecutor(client, nil, nil, "", BackupExecutorConfig{})
	if err := executor.ValidateRepositoryPlan(t.Context(), object, repository); err != nil {
		t.Fatal(err)
	}

	if len(client.Actions()) != 0 {
		t.Fatal("published recovery point validation must not depend on live source resources")
	}

	object.Name = "another-backup"
	if err := executor.ValidateRepositoryPlan(t.Context(), object, repository); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("foreign backup accepted: %v", err)
	}
}

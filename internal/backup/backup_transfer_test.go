package backup

import (
	"errors"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func backupToolRequestFixture(
	namespace, id string,
	plan v1alpha1.BackupPlan,
	writableMount bool,
	store S3RepositoryStore,
	config string,
	overrides *kube.HelmOverrides,
) (pvmigrate.Backup, error) {
	transfer := backupTransfer{store: store}

	return transfer.toolRequest(namespace, id, plan, writableMount, config, overrides)
}

// TestClassifySyncErrorSurfacesRootCause pins that the tool failure text
// reaches the workflow status: domain errors render only their own message,
// so the classified message must embed the cause or a volume-mount failure
// surfaces as a bare, misdirecting "S3 data synchronization failed".
func TestClassifySyncErrorSurfacesRootCause(t *testing.T) {
	cause := errors.New(
		"pod backup-rclone-abc: Pending; FailedMount: cannot mount unformatted disk as we are manipulating it in read-only mode",
	)

	classified := classifySyncError(t.Context(), "backup", cause)
	if domain.CategoryOf(classified) != domain.ErrorCopy {
		t.Fatalf("category drifted: %v", domain.CategoryOf(classified))
	}

	if !strings.Contains(classified.Error(), "S3 data synchronization failed") ||
		!strings.Contains(classified.Error(), "cannot mount unformatted disk") {
		t.Fatalf("root cause was not embedded: %s", classified.Error())
	}
}

// TestClassifySyncErrorBoundsMessage keeps an oversized tool error inside the
// workflow status message limit so the failure checkpoint still persists.
func TestClassifySyncErrorBoundsMessage(t *testing.T) {
	cause := errors.New(strings.Repeat("x", 2*domain.MaxWorkflowMessageBytes))

	classified := classifySyncError(t.Context(), "backup", cause)

	var typed *domain.Error
	if !errors.As(classified, &typed) {
		t.Fatalf("classified error lost its domain type: %T", classified)
	}

	if len(typed.Message) > domain.MaxWorkflowMessageBytes {
		t.Fatalf(
			"message exceeded the CRD limit: %d > %d",
			len(typed.Message),
			domain.MaxWorkflowMessageBytes,
		)
	}
}

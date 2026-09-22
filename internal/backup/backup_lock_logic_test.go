package backup

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

type fakeRepositoryLocation struct {
	backend     string
	destination string
}

func (f fakeRepositoryLocation) Backend() string     { return f.backend }
func (f fakeRepositoryLocation) Destination() string { return f.destination }

func TestBackupTargetLockID(t *testing.T) {
	store := fakeRepositoryLocation{backend: "s3", destination: "endpoint/bucket/rp-1"}

	got := backupTargetLockID(store)
	if got == "" {
		t.Fatal("lock id must not be empty")
	}

	if len(got) != len("backup-target-")+32 {
		t.Errorf("lock id length = %d, want backup-target- prefix plus 32 hex chars", len(got))
	}

	if backupTargetLockID(store) != got {
		t.Error("lock id must be deterministic for the same location")
	}

	other := fakeRepositoryLocation{backend: "s3", destination: "endpoint/bucket/rp-2"}
	if backupTargetLockID(other) == got {
		t.Error("different destinations must produce different lock ids")
	}

	otherBackend := fakeRepositoryLocation{backend: "pvc", destination: "endpoint/bucket/rp-1"}
	if backupTargetLockID(otherBackend) == got {
		t.Error("different backends must produce different lock ids")
	}
}

func TestWrapBackupTargetLockError(t *testing.T) {
	source := domain.NewError(domain.ErrorKubernetes, "S3 lock", "read lock object")
	wrapped := wrapBackupTargetLockError("endpoint/bucket", "acquire backup lock", source)

	if domain.CategoryOf(wrapped) != domain.ErrorKubernetes {
		t.Errorf("kubernetes source should stay kubernetes, got %v", domain.CategoryOf(wrapped))
	}

	if !strings.Contains(wrapped.Error(), "endpoint/bucket") {
		t.Errorf("wrapped error should carry the destination: %v", wrapped)
	}

	// Internal errors are reclassified as conflicts: an internal failure while
	// holding someone else's lock must not look like our own bug to operators.
	internal := domain.NewError(domain.ErrorInternal, "S3 lock", "boom")

	wrapped = wrapBackupTargetLockError("endpoint/bucket", "acquire backup lock", internal)
	if domain.CategoryOf(wrapped) != domain.ErrorConflict {
		t.Errorf("internal source should become conflict, got %v", domain.CategoryOf(wrapped))
	}
}

func TestClassifyLeaseError(t *testing.T) {
	if got := classifyLeaseError(
		context.Background(), context.DeadlineExceeded,
	); domain.CategoryOf(got) != domain.ErrorTimeout {
		t.Errorf("deadline exceeded should classify as timeout, got %v", domain.CategoryOf(got))
	}

	if got := classifyLeaseError(
		context.Background(), context.Canceled,
	); domain.CategoryOf(got) != domain.ErrorTimeout {
		t.Errorf("canceled should classify as timeout, got %v", domain.CategoryOf(got))
	}

	tests := []struct {
		name string
		err  error
	}{
		{
			"conflict passthrough",
			domain.NewError(domain.ErrorConflict, "S3 lock", "locked by other"),
		},
		{"plain passthrough", errors.New("boom")},
	}
	for _, tc := range tests {
		if got := classifyLeaseError(context.Background(), tc.err); !errors.Is(got, tc.err) {
			t.Errorf(
				"%s: classifyLeaseError must pass the error through unchanged, got %v",
				tc.name,
				got,
			)
		}
	}
}

type testBackupSessionLocker struct{ lock kube.SessionLock }

func (l testBackupSessionLocker) AcquireSessionLock(
	context.Context,
	string,
	string,
) (kube.SessionLock, error) {
	return l.lock, nil
}

func TestAcquireBackupTargetLockExposesSessionLeaseFence(t *testing.T) {
	lost := errors.New("session lease lost")
	lock := &recordingBackupSessionLock{err: lost}

	ctx, _, cancel, err := acquireBackupTargetLock(
		context.Background(),
		testBackupSessionLocker{lock: lock},
		"sessions",
		fakeRepositoryLocation{backend: "s3", destination: "endpoint/bucket/rp-1"},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	if err := kube.LeaseFenceError(ctx); !errors.Is(err, lost) {
		t.Fatalf("backup context fence = %v, want lease loss", err)
	}
}

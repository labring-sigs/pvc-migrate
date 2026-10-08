package backup

import (
	"errors"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// TestLockRenewalAborts pins the renewal abort policy shared by the backup
// and restore lock loops: definite ownership loss always abandons the
// transfer, while a transient failure only aborts once half the TTL has
// passed without a successful renewal — one backend blip must not kill a
// healthy multi-hour transfer.
func TestLockRenewalAborts(t *testing.T) {
	transient := errors.New("connection reset")
	conflict := domain.NewError(domain.ErrorConflict, "lock", "fenced")
	ttl := time.Hour

	if !lockRenewalAborts(conflict, time.Now(), ttl) {
		t.Fatal("ownership conflict must abort immediately")
	}

	if lockRenewalAborts(transient, time.Now(), ttl) {
		t.Fatal("fresh transient failure must retry inside the TTL budget")
	}

	if lockRenewalAborts(transient, time.Now().Add(-ttl/4), ttl) {
		t.Fatal("expected retry while inside half the TTL")
	}

	if !lockRenewalAborts(transient, time.Now().Add(-ttl/2-time.Minute), ttl) {
		t.Fatal("transient failures past half the TTL must abort before a successor takes over")
	}

	// Backend unavailability surfaces as a retryable precondition, never as a
	// conflict: only definite ownership loss may abort immediately.
	precondition := domain.NewError(domain.ErrorPrecondition, "S3 lock", "read lock before renewal")
	if lockRenewalAborts(precondition, time.Now(), ttl) {
		t.Fatal("fresh precondition failure must retry inside the TTL budget")
	}

	if !lockRenewalAborts(precondition, time.Now().Add(-ttl/2-time.Minute), ttl) {
		t.Fatal("precondition failures past half the TTL must abort before a successor takes over")
	}
}

// TestReleaseFailureLeavesPublishedBackup pins which release errors may be
// tolerated after a successful run: anything that leaves the published
// recovery point intact while the lock object ages out, versus programming
// errors that must still surface.
func TestReleaseFailureLeavesPublishedBackup(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "conflict from raced renewal",
			err:  domain.NewError(domain.ErrorConflict, "S3 lock", "lock ownership changed"),
			want: true,
		},
		{
			name: "transport failure",
			err:  domain.NewError(domain.ErrorPrecondition, "S3 lock", "release backup lock"),
			want: true,
		},
		{
			name: "backend timeout",
			err:  domain.NewError(domain.ErrorTimeout, "S3 lock", "release backup lock"),
			want: true,
		},
		{
			name: "validation error",
			err:  domain.NewError(domain.ErrorValidation, "S3 lock", "lock ETag is required"),
			want: false,
		},
		{
			name: "internal error",
			err:  errors.New("bare failure"),
			want: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := releaseFailureLeavesPublishedBackup(test.err); got != test.want {
				t.Fatalf(
					"releaseFailureLeavesPublishedBackup(%v)=%t, want %t",
					test.err,
					got,
					test.want,
				)
			}
		})
	}
}

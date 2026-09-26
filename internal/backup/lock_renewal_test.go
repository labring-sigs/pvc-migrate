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
}

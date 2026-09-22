package objectstore_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

func TestLockHolder(t *testing.T) {
	expected := "pvc-migrate-" + hex.EncodeToString(sha256Sum("op-1"))

	if got := objectstore.LockHolder("op-1"); got != expected {
		t.Errorf("LockHolder(op-1)=%q want %q", got, expected)
	}

	second := objectstore.LockHolder("op-1")
	if second != expected {
		t.Error("LockHolder must be deterministic for the same operation ID")
	}

	if objectstore.LockHolder("op-2") == second {
		t.Error("different operation IDs must produce different holders")
	}

	if holder := objectstore.LockHolder(""); !strings.HasPrefix(holder, "pvc-migrate-") {
		t.Errorf("empty operation ID should still produce prefixed holder, got %q", holder)
	}
}

func sha256Sum(input string) []byte {
	digest := sha256.Sum256([]byte(input))
	return digest[:8]
}

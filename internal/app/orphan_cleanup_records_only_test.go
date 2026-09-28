package app

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/client-go/kubernetes/fake"
)

func orphanCleanupOptions() OrphanCleanupOptions {
	return OrphanCleanupOptions{
		SessionID:        "orphan-session",
		SessionNamespace: "sessions",
		SourceNamespace:  "tenants",
		SourcePVC:        "data",
	}
}

// A session whose storage is entirely deleted must converge: the records-only
// plan removes the stale record and lease instead of wedging forever on the
// deleted source PVC.
func TestOrphanCleanupRecordsOnlyConvergesWhenSourcePVCDeleted(t *testing.T) {
	client := fake.NewClientset()
	lock := &fakeSessionLock{}
	cleaner := NewOrphanCleaner(
		client,
		&fakeSessionLocker{lock: lock},
		orphanNoopLeaseCleaner{},
		orphanNoRecordsFinder{},
		nil,
	)

	plan, err := cleaner.CleanupOrphan(context.Background(), orphanCleanupOptions())
	if err != nil {
		t.Fatal(err)
	}

	if plan.Mode != domain.OrphanCleanupRecordsOnly {
		t.Fatalf("mode=%s, want RecordsOnly", plan.Mode)
	}

	if !plan.Ready {
		t.Fatalf("plan not ready: %+v", plan.Checks)
	}

	if !lock.deleted {
		t.Fatal("records-only cleanup did not delete the session lease")
	}
}

// Storage that still carries the session label blocks the plan with the
// leftovers named: deleting it without the source PVC anchor is the
// operator's data-deletion decision, not the recovery tool's.
func TestOrphanCleanupDeletedSourcePVCNamesLeftoverStorage(t *testing.T) {
	leftover := orphanPV("active-pv", "rollback-pv")
	leftover.Labels[kube.ManagedByLabel] = kube.ManagedByValue

	client := fake.NewClientset(leftover)
	cleaner := NewOrphanCleaner(client, nil, nil, orphanNoRecordsFinder{}, nil)

	plan, err := cleaner.PlanOrphanCleanup(context.Background(), orphanCleanupOptions())
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready {
		t.Fatalf("plan became ready with leftover storage: %+v", plan.Checks)
	}

	found := false
	for _, check := range plan.Checks {
		if !check.Passed &&
			strings.Contains(check.Message, "PV active-pv (role "+kube.ResourceRoleActive+")") {
			found = true
		}
	}

	if !found {
		t.Fatalf("no check names the leftover storage: %+v", plan.Checks)
	}
}

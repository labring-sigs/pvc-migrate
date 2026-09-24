package app

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type orphanRemovedKindFinder struct{}

func (orphanRemovedKindFinder) Find(
	context.Context,
	string,
	...string,
) (*kube.WorkflowOwner, error) {
	return &kube.WorkflowOwner{
		ID:               "orphan-session",
		SessionNamespace: "sessions",
		Backend:          kube.SessionBackendConfigMap,
		Resource:         domain.ControllerResource{},
		Phase:            domain.PhaseCompleted,
	}, nil
}

// TestOrphanCleanupTreatsRemovedKindRecordsAsAbsent keeps upgrade recovery
// unblocked: a record written by an older binary may carry a kind this build
// removed, and no lifecycle command can drive it. Planning must treat it as
// absent, and cleanup removes the stale record instead of dead-ending.
func TestOrphanCleanupTreatsRemovedKindRecordsAsAbsent(t *testing.T) {
	pvc := orphanPVC("rollback-pv")
	active := orphanPV("active-pv", "rollback-pv")
	rollback := orphanPV("rollback-pv", "active-pv")
	rollback.Labels[kube.ResourceRoleLabel] = kube.ResourceRoleRollback
	rollback.Status.Phase = corev1.VolumeReleased
	staleRecord := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "sessions",
			Name:      kube.SessionConfigMapName("orphan-session"),
			Labels: map[string]string{
				kube.ManagedByLabel: kube.ManagedByValue,
				kube.SessionKey:     "orphan-session",
			},
		},
	}

	client := fake.NewClientset(pvc, active, rollback, staleRecord)
	cleaner := NewOrphanCleaner(
		client,
		&fakeSessionLocker{},
		orphanNoopLeaseCleaner{},
		orphanRemovedKindFinder{},
		nil,
	)

	plan, err := cleaner.PlanOrphanCleanup(context.Background(), OrphanCleanupOptions{
		SessionID:        "orphan-session",
		SessionNamespace: "sessions",
		SourceNamespace:  "tenants",
		SourcePVC:        "data",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("removed-kind record blocked recovery: %+v", plan.Checks)
	}

	recordCheckPassed := false
	for _, check := range plan.Checks {
		if check.Name == domain.CheckNameSessionRecord && check.Passed &&
			strings.Contains(check.Message, "no longer serves") {
			recordCheckPassed = true
		}
	}

	if !recordCheckPassed {
		t.Fatalf("session-record check missing removed-kind note: %+v", plan.Checks)
	}

	if _, err := cleaner.CleanupOrphan(context.Background(), OrphanCleanupOptions{
		SessionID:        "orphan-session",
		SessionNamespace: "sessions",
		SourceNamespace:  "tenants",
		SourcePVC:        "data",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		ConfigMaps("sessions").
		Get(context.Background(), staleRecord.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale record survived cleanup: %v", err)
	}
}

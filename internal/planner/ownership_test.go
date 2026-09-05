package planner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlanRejectsPersistedSessionOwnershipBeforeMutation(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	old := domain.NewSession(
		"old-session",
		domain.NewOfflineMigrationSessionSpec(domain.SessionCommon{
			SourceNamespace:      "app",
			TemporaryNamespace:   "system",
			DestinationNamespace: "app",
			SessionNamespace:     "system",
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC: domain.ObjectReference{
						Namespace: "app",
						Name:      "data",
						UID:       "pvc-uid",
					},
					SourcePV: domain.ObjectReference{Name: "pv-source", UID: "pv-uid"},
					DestinationPVC: domain.ObjectReference{
						Namespace: "system",
						Name:      "data-migrated",
					},
				},
			},
		}, domain.SessionWorkflowOptions{}),
		time.Now(),
	)

	old.Status.Phase = domain.PhaseCompleted
	if err := kube.NewConfigMapSessionStore(client).Create(context.Background(), old); err != nil {
		t.Fatal(err)
	}

	pvc, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})
	pvc.Annotations = map[string]string{kube.SessionKey: old.ID}

	pvc.Labels = map[string]string{kube.SessionKey: old.ID}
	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	pv, _ := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-source", metav1.GetOptions{})

	pv.Labels = map[string]string{kube.SessionKey: old.ID}
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := New(client, nil).plan(context.Background(), planOptions{
		SessionID:          "new-session",
		Operation:          domain.OperationMigrate,
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "session-ownership") {
		t.Fatalf("plan=%#v", plan)
	}

	for _, check := range plan.Checks {
		if check.Name == "session-ownership" &&
			(!strings.Contains(check.Message, old.ID) || !strings.Contains(check.Message, string(domain.PhaseCompleted)) || !strings.Contains(check.Message, "migrate status") || !strings.Contains(check.Message, "migrate cleanup") || !strings.Contains(check.Message, "--dry-run=false")) {
			t.Fatalf("ownership guidance=%q", check.Message)
		}
	}
}

func TestPlanDiagnosesOrphanSessionOwnership(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	pvc, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})

	pvc.Annotations = map[string]string{kube.SessionKey: "missing-session"}
	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := New(client, nil).plan(context.Background(), planOptions{
		SessionID:          "new-session",
		Operation:          domain.OperationMigrate,
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "session-ownership") {
		t.Fatalf("plan=%#v", plan)
	}

	message := ownershipMessage(plan)
	if !strings.Contains(message, "orphan") || !strings.Contains(message, "cleanup-orphan") ||
		!strings.Contains(message, "missing-session") {
		t.Fatalf("orphan guidance=%q", message)
	}
}

func TestPlanRejectsConflictingSessionOwnership(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	pvc, _ := client.CoreV1().
		PersistentVolumeClaims("app").
		Get(context.Background(), "data", metav1.GetOptions{})

	pvc.Annotations = map[string]string{kube.SessionKey: "session-a"}
	if _, err := client.CoreV1().
		PersistentVolumeClaims("app").
		Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	pv, _ := client.CoreV1().
		PersistentVolumes().
		Get(context.Background(), "pv-source", metav1.GetOptions{})

	pv.Labels = map[string]string{kube.SessionKey: "session-b"}
	if _, err := client.CoreV1().
		PersistentVolumes().
		Update(context.Background(), pv, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	plan, err := New(client, nil).plan(context.Background(), planOptions{
		SessionID:          "new-session",
		Operation:          domain.OperationMigrate,
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}

	if plan.Ready || !hasFailedCheck(plan, "session-ownership") ||
		!strings.Contains(ownershipMessage(plan), "conflicting") {
		t.Fatalf("plan=%#v", plan)
	}
}

func ownershipMessage(plan *domain.MigrationPlan) string {
	for _, check := range plan.Checks {
		if check.Name == "session-ownership" {
			return check.Message
		}
	}

	return ""
}

func TestControllerOwnerGuidanceUsesWorkflowBackend(t *testing.T) {
	session := domain.NewSession("owned-copy", domain.NewSessionSpec(
		domain.OperationCopy,
		domain.SessionCommon{
			SourceNamespace:      "app",
			DestinationNamespace: "app",
			TemporaryNamespace:   "app",
			SessionNamespace:     "app",
		},
		false,
		domain.SessionWorkflowOptions{},
	), time.Now())
	session.Backend = kube.SessionBackendCRD
	session.BackendResource = domain.ControllerKindCopy
	session.Status.Phase = domain.PhaseWarmCopied

	message := persistedOwnerGuidance(session)
	for _, want := range []string{"--mode=controller", "--workflow-namespace app", "copy status owned-copy", "copy cleanup owned-copy"} {
		if !strings.Contains(message, want) {
			t.Fatalf("guidance %q missing %q", message, want)
		}
	}

	if strings.Contains(message, "cleanup-orphan") {
		t.Fatalf("persisted workflow called orphan: %s", message)
	}
}

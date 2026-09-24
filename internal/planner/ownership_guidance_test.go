package planner

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

func ownershipFixture(t *testing.T, sessionType domain.SessionType) *kube.WorkflowOwner {
	t.Helper()

	workflow, ok := domain.ControllerWorkflowForType(sessionType)
	if !ok {
		t.Fatalf("session type %q has no workflow", sessionType)
	}

	kind := workflow.Kind
	if kind == "" {
		kind = workflow.ClusterKind
	}

	resource, ok := domain.ControllerResourceForKind(kind)
	if !ok {
		t.Fatalf("workflow kind %q has no resource", kind)
	}

	return &kube.WorkflowOwner{
		ID:               "owner-session",
		SessionNamespace: "pvc-migrate-system",
		Backend:          kube.SessionBackendConfigMap,
		Resource:         resource,
		Phase:            domain.PhaseCompleted,
	}
}

// TestOwnershipControllerGuidanceOmitsCLICommands pins that check messages
// landing in workflow CR status never carry CLI command text: controller
// planning describes the releasing action instead.
func TestOwnershipControllerGuidanceOmitsCLICommands(t *testing.T) {
	cases := []struct {
		name  string
		phase domain.Phase
		want  string
	}{
		{"completed", domain.PhaseCompleted, "finalize the finished session"},
		{"aborted", domain.PhaseAborted, "finalize the finished session"},
		{"rolled-back", domain.PhaseRolledBack, "finalize the finished session"},
		{"failed-abortable", domain.PhaseFailed, "abort the failed session"},
		{"active", domain.PhaseWarmCopying, "resume or abort the owning session"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			owner := ownershipFixture(t, domain.SessionTypeMigratePod)
			owner.Phase = testCase.phase

			guidance := persistedOwnerGuidance(owner, domain.PresentationController)

			for _, forbidden := range []string{"pvc-migrate", "--dry-run", "cleanup owner-session"} {
				if strings.Contains(guidance, forbidden) {
					t.Fatalf("controller guidance carries CLI text %q: %s", forbidden, guidance)
				}
			}

			if !strings.Contains(guidance, testCase.want) {
				t.Fatalf("controller guidance missing %q: %s", testCase.want, guidance)
			}
		})
	}

	warmCopied := ownershipFixture(t, domain.SessionTypeCopy)

	warmCopied.Phase = domain.PhaseWarmCopied
	if guidance := persistedOwnerGuidance(
		warmCopied,
		domain.PresentationController,
	); !strings.Contains(
		guidance,
		"keep or discard the copied PVC",
	) {
		t.Fatalf("controller warm-copied copy guidance: %s", guidance)
	}

	reservation := ownershipFixture(t, domain.SessionTypeReserve)

	reservation.Phase = domain.PhaseReserved
	if guidance := persistedOwnerGuidance(
		reservation,
		domain.PresentationController,
	); !strings.Contains(
		guidance,
		"continue the reservation as a copy",
	) {
		t.Fatalf("controller reserved guidance: %s", guidance)
	}
}

// TestOwnershipCRDGuidanceStaysInKubectl keeps the controller-owned workflow
// branch on the kubectl surface in both modes.
func TestOwnershipCRDGuidanceStaysInKubectl(t *testing.T) {
	owner := ownershipFixture(t, domain.SessionTypeMigratePod)
	owner.Backend = kube.SessionBackendCRD

	for _, presentation := range []domain.Presentation{
		domain.PresentationCLI,
		domain.PresentationController,
	} {
		guidance := persistedOwnerGuidance(owner, presentation)
		if !strings.Contains(guidance, "kubectl get podmigrations") ||
			strings.Contains(guidance, "pvc-migrate ") {
			t.Fatalf("CRD guidance must stay in kubectl: %s", guidance)
		}
	}
}

// TestOwnershipCleanupGuidanceMatchesCleanupFlags pins that the printed
// cleanup commands only carry flags the referenced cleanup command accepts:
// rename, move, backup, and restore reject --unused-storage-policy.
func TestOwnershipCleanupGuidanceMatchesCleanupFlags(t *testing.T) {
	cases := []struct {
		sessionType domain.SessionType
		workflow    string
		acceptsFlag bool
	}{
		{domain.SessionTypeCopy, "copy", true},
		{domain.SessionTypeReserve, "reserve", true},
		{domain.SessionTypeMigrate, "migrate", true},
		{domain.SessionTypeMigratePod, "migrate-pod", true},
		{domain.SessionTypeRename, "rename", false},
		{domain.SessionTypeMove, "move", false},
		{domain.SessionTypeBackup, "backup", false},
		{domain.SessionTypeRestore, "restore", false},
	}

	for _, testCase := range cases {
		t.Run(testCase.workflow, func(t *testing.T) {
			guidance := persistedOwnerGuidance(
				ownershipFixture(t, testCase.sessionType),
				domain.PresentationCLI,
			)

			if !strings.Contains(guidance, testCase.workflow+" cleanup owner-session") {
				t.Fatalf("guidance missing cleanup command: %s", guidance)
			}

			if testCase.acceptsFlag &&
				!strings.Contains(guidance, "--unused-storage-policy Keep") {
				t.Fatalf("transfer guidance missing Keep policy: %s", guidance)
			}

			if !testCase.acceptsFlag &&
				strings.Contains(guidance, "--unused-storage-policy") {
				t.Fatalf(
					"%s cleanup rejects --unused-storage-policy: %s",
					testCase.workflow,
					guidance,
				)
			}
		})
	}
}

// TestOwnershipWarmCopiedGuidanceCoversCompletedCopy keeps the dedicated
// WarmCopied branch honest for copy workflows while other types fall through
// to the generic terminal guidance.
func TestOwnershipWarmCopiedGuidanceCoversCompletedCopy(t *testing.T) {
	owner := ownershipFixture(t, domain.SessionTypeCopy)
	owner.Phase = domain.PhaseWarmCopied

	guidance := persistedOwnerGuidance(owner, domain.PresentationCLI)
	if !strings.Contains(guidance, "preserve the copied PVC") ||
		!strings.Contains(guidance, "copy cleanup owner-session --unused-storage-policy Keep") {
		t.Fatalf("warm-copied copy guidance: %s", guidance)
	}

	reservation := ownershipFixture(t, domain.SessionTypeReserve)
	reservation.Phase = domain.PhaseReserved

	promotion := persistedOwnerGuidance(reservation, domain.PresentationCLI)
	if !strings.Contains(promotion, "validate copy with") ||
		!strings.Contains(promotion, "close the reservation") {
		t.Fatalf("reserved promotion guidance: %s", promotion)
	}
}

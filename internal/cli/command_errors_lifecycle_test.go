package cli

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func TestSessionTypeCommandName(t *testing.T) {
	cases := map[domain.SessionType]string{
		domain.SessionTypeMigrate:    "migrate",
		domain.SessionTypeMigratePod: "migrate-pod",
		domain.SessionTypeReserve:    "reserve",
		domain.SessionTypeCopy:       "copy",
		domain.SessionTypeBackup:     "backup",
		domain.SessionTypeRestore:    "restore",
		domain.SessionTypeRename:     "rename",
		domain.SessionTypeMove:       "move",
		domain.SessionType("bogus"):  "migrate",
	}
	for input, want := range cases {
		if got := sessionTypeCommandName(input); got != want {
			t.Errorf("sessionTypeCommandName(%q)=%q want %q", input, got, want)
		}
	}
}

func newRootForHints() *cobra.Command {
	root := &cobra.Command{Use: "pvc-migrate"}
	copyCmd := &cobra.Command{Use: "copy"}
	root.AddCommand(copyCmd)

	podCmd := &cobra.Command{Use: "migrate-pod"}
	root.AddCommand(podCmd)
	root.AddCommand(&cobra.Command{Use: "unknown-top"})

	crGroup := &cobra.Command{Use: "cr"}
	crCopy := &cobra.Command{Use: "copy"}
	crGroup.AddCommand(crCopy)
	crCopy.AddCommand(&cobra.Command{Use: "status"})
	root.AddCommand(crGroup)

	return root
}

func TestWorkflowCommandNameForCommand(t *testing.T) {
	root := newRootForHints()

	if got := workflowCommandNameForCommand(nil); got != "migrate" {
		t.Errorf("nil command should fall back to migrate, got %q", got)
	}

	if got := workflowCommandNameForCommand(root); got != "migrate" {
		t.Errorf("root command should fall back to migrate, got %q", got)
	}

	copyCmd, _, err := root.Find([]string{"copy"})
	if err != nil {
		t.Fatal(err)
	}

	if got := workflowCommandNameForCommand(copyCmd); got != "copy" {
		t.Errorf("copy command = %q want copy", got)
	}

	podCmd, _, err := root.Find([]string{"migrate-pod"})
	if err != nil {
		t.Fatal(err)
	}

	if got := workflowCommandNameForCommand(podCmd); got != "migrate-pod" {
		t.Errorf("migrate-pod command = %q want migrate-pod", got)
	}

	unknown, _, err := root.Find([]string{"unknown-top"})
	if err != nil {
		t.Fatal(err)
	}

	if got := workflowCommandNameForCommand(unknown); got != "migrate" {
		t.Errorf("unknown top-level = %q want migrate", got)
	}
}

func TestSessionRecordInspectionCommand(t *testing.T) {
	root := newRootForHints()

	out := sessionRecordInspectionCommand(nil, "tenant-ns", "session-1")
	if !strings.Contains(out, "kubectl") {
		t.Errorf("inspection should use kubectl, got %q", out)
	}

	if !strings.Contains(out, "tenant-ns") || !strings.Contains(out, "session-1") {
		t.Errorf("inspection should keep namespace and id, got %q", out)
	}

	copyCmd, _, err := root.Find([]string{"copy"})
	if err != nil {
		t.Fatal(err)
	}

	// Session records persist as ConfigMaps: the inspection hint must point
	// at the session ConfigMap, not at workflow CRDs the mode never writes.
	out = sessionRecordInspectionCommand(copyCmd, "tenant-ns", "session-1")
	if !strings.Contains(out, "get configmap pvc-migrate-session-session-1") {
		t.Errorf("session inspection should target the session ConfigMap, got %q", out)
	}

	if strings.Contains(out, "migrate.sealos.io") {
		t.Errorf("session inspection must not suggest workflow CRDs, got %q", out)
	}

	crCopyCmd, _, err := root.Find([]string{"cr", "copy", "status"})
	if err != nil {
		t.Fatal(err)
	}

	out = sessionRecordInspectionCommand(crCopyCmd, "tenant-ns", "session-1")
	if !strings.Contains(out, "copies.migrate.sealos.io") ||
		!strings.Contains(out, "clustercopies.migrate.sealos.io") {
		t.Errorf("cr copy family resources missing, got %q", out)
	}

	if strings.Contains(out, "migrations.migrate.sealos.io") {
		t.Errorf("cr inspection must resolve the command's family, got %q", out)
	}
}

func TestCopyResumePhase(t *testing.T) {
	cases := []struct {
		name   string
		status v1alpha1.WorkflowStatus
		want   v1alpha1.WorkflowPhase
	}{
		{"empty", v1alpha1.WorkflowStatus{}, ""},
		{"planned", v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned}, domain.PhasePlanned},
		{"failed without checkpoint", v1alpha1.WorkflowStatus{Phase: domain.PhaseFailed}, ""},
		{"failed with checkpoint", v1alpha1.WorkflowStatus{
			Phase: domain.PhaseFailed, ResumeFrom: domain.PhaseReserved,
		}, domain.PhaseReserved},
	}
	for _, tc := range cases {
		if got := copyResumePhase(tc.status); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

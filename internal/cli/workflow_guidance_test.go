package cli

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func TestWriteWorkflowNextStepsCompletedWithRollback(t *testing.T) {
	var output bytes.Buffer

	if err := writeWorkflowNextSteps(
		&output,
		"pvc-migrate --kubeconfig /etc/kc",
		"migrate-pod",
		"mig-1",
		domain.PhaseCompleted,
		true,
	); err != nil {
		t.Fatal(err)
	}

	guidance := output.String()

	if !strings.Contains(
		guidance,
		"\nNext steps for session mig-1 (phase Completed):\n",
	) {
		t.Fatalf("next steps lack the titled header: %q", guidance)
	}

	for _, want := range []string{
		"  Inspect: pvc-migrate --kubeconfig /etc/kc migrate-pod status mig-1\n",
		"  Roll back: pvc-migrate --kubeconfig /etc/kc --yes migrate-pod rollback mig-1 --dry-run=false\n",
		"  Finalize and delete retained resources/session: " +
			"pvc-migrate --kubeconfig /etc/kc --yes migrate-pod cleanup mig-1" +
			" --finalize --delete-session --dry-run=false\n",
	} {
		if !strings.Contains(guidance, want) {
			t.Fatalf("next steps lack %q: %q", want, guidance)
		}
	}
}

func TestWriteWorkflowNextStepsCompletedWithoutRollback(t *testing.T) {
	var output bytes.Buffer

	if err := writeWorkflowNextSteps(
		&output,
		"pvc-migrate",
		"copy",
		"mig-1",
		domain.PhaseCompleted,
		false,
	); err != nil {
		t.Fatal(err)
	}

	guidance := output.String()

	if strings.Contains(guidance, "Roll back:") {
		t.Fatalf("copy guidance must not suggest rollback: %q", guidance)
	}

	if !strings.Contains(
		guidance,
		"  Finalize and delete retained resources/session: "+
			"pvc-migrate --yes copy cleanup mig-1 --finalize --delete-session --dry-run=false\n",
	) {
		t.Fatalf("copy guidance lacks the finalize command: %q", guidance)
	}
}

func TestWriteWorkflowNextStepsTerminalWithoutRollbackWindow(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseAborted, domain.PhaseRolledBack} {
		var output bytes.Buffer

		if err := writeWorkflowNextSteps(
			&output,
			"pvc-migrate",
			"migrate-pod",
			"mig-1",
			phase,
			true,
		); err != nil {
			t.Fatal(err)
		}

		guidance := output.String()

		if strings.Contains(guidance, "Roll back:") {
			t.Fatalf("%s guidance must not suggest rollback: %q", phase, guidance)
		}

		if !strings.Contains(guidance, "Finalize and delete retained resources/session:") {
			t.Fatalf("%s guidance lacks the finalize command: %q", phase, guidance)
		}
	}
}

func TestWriteWorkflowNextStepsSilentOutsideTerminalPhases(t *testing.T) {
	for _, phase := range []domain.Phase{
		domain.PhasePlanned, domain.PhaseReserved, domain.PhaseWarmCopying,
		domain.PhaseWarmCopied, domain.PhaseFinalSyncing, domain.PhaseActivated,
		domain.PhaseFailed,
	} {
		var output bytes.Buffer

		if err := writeWorkflowNextSteps(
			&output,
			"pvc-migrate",
			"migrate-pod",
			"mig-1",
			phase,
			true,
		); err != nil {
			t.Fatal(err)
		}

		if output.Len() != 0 {
			t.Fatalf("%s guidance must stay silent: %q", phase, output.String())
		}
	}

	var empty bytes.Buffer

	if err := writeWorkflowNextSteps(
		&empty,
		"pvc-migrate",
		"migrate-pod",
		"",
		domain.PhaseCompleted,
		true,
	); err != nil {
		t.Fatal(err)
	}

	if empty.Len() != 0 {
		t.Fatalf("empty session must stay silent: %q", empty.String())
	}
}

func TestWriteWorkflowNextStepsQuotesSession(t *testing.T) {
	var output bytes.Buffer

	if err := writeWorkflowNextSteps(
		&output,
		"pvc-migrate",
		"migrate-pod",
		"mig 1",
		domain.PhaseCompleted,
		true,
	); err != nil {
		t.Fatal(err)
	}

	guidance := output.String()

	if !strings.Contains(guidance, "status 'mig 1'") {
		t.Fatalf("status command must quote the session id: %q", guidance)
	}

	if !strings.Contains(guidance, "rollback 'mig 1' --dry-run=false") {
		t.Fatalf("rollback command must quote the session id: %q", guidance)
	}
}

func TestWriteDryRunNotice(t *testing.T) {
	var output bytes.Buffer

	if err := writeDryRunNotice(
		&output,
		"pvc-migrate --yes migrate-pod cleanup mig-1 --finalize --delete-session --dry-run=false",
	); err != nil {
		t.Fatal(err)
	}

	want := "Dry run: validated only, nothing was changed. " +
		"Execute with: pvc-migrate --yes migrate-pod cleanup mig-1" +
		" --finalize --delete-session --dry-run=false\n"

	if output.String() != want {
		t.Fatalf("dry-run notice mismatch: got %q want %q", output.String(), want)
	}
}

func TestCleanupExecuteCommandMirrorsOptions(t *testing.T) {
	command := cleanupExecuteCommand(
		"pvc-migrate",
		"migrate",
		"mig-1",
		"Keep",
		true,
		true,
	)

	want := "pvc-migrate --yes migrate cleanup mig-1" +
		" --unused-storage-policy Keep --finalize --delete-session --dry-run=false"
	if command != want {
		t.Fatalf("cleanup command mismatch: got %q want %q", command, want)
	}

	minimal := cleanupExecuteCommand("pvc-migrate", "copy", "mig-1", "", false, false)
	if minimal != "pvc-migrate --yes copy cleanup mig-1 --dry-run=false" {
		t.Fatalf("minimal cleanup command mismatch: %q", minimal)
	}
}

func TestWriteControllerWorkflowNextSteps(t *testing.T) {
	var output bytes.Buffer

	if err := writeControllerWorkflowNextSteps(
		&output,
		"kubectl",
		"migrations.migrate.sealos.io",
		"sealos",
		"mig 1",
		domain.PhaseCompleted,
	); err != nil {
		t.Fatal(err)
	}

	guidance := output.String()

	if !strings.Contains(
		guidance,
		"\nNext steps for workflow migrations.migrate.sealos.io/mig 1 (phase Completed):\n",
	) {
		t.Fatalf("controller guidance lacks the titled header: %q", guidance)
	}

	for _, want := range []string{
		"  Inspect: kubectl -n sealos get migrations.migrate.sealos.io 'mig 1'\n",
		"  Finalize: kubectl -n sealos delete migrations.migrate.sealos.io 'mig 1'" +
			" (the finalizer converges storage per the spec reclaim policies)\n",
	} {
		if !strings.Contains(guidance, want) {
			t.Fatalf("controller guidance lacks %q: %q", want, guidance)
		}
	}

	var cluster bytes.Buffer

	if err := writeControllerWorkflowNextSteps(
		&cluster,
		"kubectl",
		"clustermigrations.migrate.sealos.io",
		"",
		"mig-1",
		domain.PhaseCompleted,
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(
		cluster.String(),
		"  Inspect: kubectl get clustermigrations.migrate.sealos.io mig-1\n",
	) {
		t.Fatalf("cluster-scoped guidance must omit the namespace flag: %q", cluster.String())
	}
}

func TestWriteControllerWorkflowNextStepsSilentOutsideTerminalPhases(t *testing.T) {
	for _, phase := range []domain.Phase{
		domain.PhasePlanned, domain.PhaseReserved, domain.PhaseWarmCopying, domain.PhaseFailed,
	} {
		var output bytes.Buffer

		if err := writeControllerWorkflowNextSteps(
			&output,
			"kubectl",
			"migrations.migrate.sealos.io",
			"sealos",
			"mig-1",
			phase,
		); err != nil {
			t.Fatal(err)
		}

		if output.Len() != 0 {
			t.Fatalf("%s controller guidance must stay silent: %q", phase, output.String())
		}
	}
}

func TestWriteDryRunApprovalNotice(t *testing.T) {
	var output bytes.Buffer

	if err := writeDryRunApprovalNotice(&output); err != nil {
		t.Fatal(err)
	}

	want := "Dry run: nothing was submitted or changed. " +
		"Execute with the same command plus --yes and --dry-run=false.\n"

	if output.String() != want {
		t.Fatalf("approval notice mismatch: got %q want %q", output.String(), want)
	}
}

func TestCrossClusterExecuteCommandCarriesChangedFlags(t *testing.T) {
	command := cobra.Command{}
	flags := command.Flags()
	flags.String("source-kubeconfig", "/etc/source", "source")
	flags.String("destination-kubeconfig", "", "destination")
	flags.String("unused-storage-policy", "", "policy")
	flags.Bool("delete-session", false, "delete")

	if err := flags.Set("source-kubeconfig", "/etc/alt source"); err != nil {
		t.Fatal(err)
	}

	if err := flags.Set("delete-session", "true"); err != nil {
		t.Fatal(err)
	}

	got := crossClusterExecuteCommand(&command, "copy cross-cluster cleanup", "mig-1")

	want := "pvc-migrate --yes copy cross-cluster cleanup mig-1" +
		" --source-kubeconfig='/etc/alt source' --delete-session=true --dry-run=false"
	if got != want {
		t.Fatalf("cross-cluster command mismatch: got %q want %q", got, want)
	}
}

func TestControllerWorkflowGuidanceColorization(t *testing.T) {
	var output bytes.Buffer

	if err := writeControllerWorkflowNextSteps(
		&output,
		"kubectl",
		"migrations.migrate.sealos.io",
		"sealos",
		"mig-1",
		domain.PhaseCompleted,
	); err != nil {
		t.Fatal(err)
	}

	colored := string(colorizeLogText(output.Bytes()))

	for _, want := range []string{
		"\x1b[1;36mNext steps for workflow migrations.migrate.sealos.io/mig-1 (phase \x1b[0m",
		"\x1b[1;32mCompleted\x1b[0m",
		"\x1b[36mInspect:\x1b[0m kubectl",
		"\x1b[1;31mFinalize:\x1b[0m kubectl",
	} {
		if !strings.Contains(colored, want) {
			t.Fatalf("colored controller guidance lacks %q: %q", want, colored)
		}
	}

	plain := regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAllString(colored, "")
	if plain != output.String() {
		t.Fatalf("colorization changed controller guidance: got %q want %q", plain, output.String())
	}
}

func TestWorkflowGuidanceColorization(t *testing.T) {
	var output bytes.Buffer

	if err := writeWorkflowNextSteps(
		&output,
		"pvc-migrate",
		"migrate-pod",
		"mig-1",
		domain.PhaseCompleted,
		true,
	); err != nil {
		t.Fatal(err)
	}

	if err := writeDryRunNotice(
		&output,
		"pvc-migrate --yes migrate-pod cleanup mig-1 --dry-run=false",
	); err != nil {
		t.Fatal(err)
	}

	colored := string(colorizeLogText(output.Bytes()))

	for _, want := range []string{
		"\x1b[1;36mNext steps for session mig-1 (phase \x1b[0m",
		"\x1b[1;32mCompleted\x1b[0m",
		"\x1b[36mInspect:\x1b[0m pvc-migrate migrate-pod status mig-1",
		"\x1b[1;31mRoll back:\x1b[0m pvc-migrate --yes migrate-pod rollback mig-1",
		"\x1b[1;31mFinalize and delete retained resources/session:\x1b[0m pvc-migrate",
		"\x1b[1;33mDry run:\x1b[0m validated only, nothing was changed.",
	} {
		if !strings.Contains(colored, want) {
			t.Fatalf("colored guidance lacks %q: %q", want, colored)
		}
	}

	plain := regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAllString(colored, "")
	if plain != output.String() {
		t.Fatalf("colorization changed guidance content: got %q want %q", plain, output.String())
	}
}

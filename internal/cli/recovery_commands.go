package cli

import (
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newRecoveryCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "recovery",
		Short: "Recover ownership when a workflow session record is missing",
	}
	command.AddCommand(r.newOrphanCleanupCommand())

	return command
}

func (r *rootState) newOrphanCleanupCommand() *cobra.Command {
	var (
		sourceNamespace, sourcePVC string
		dryRun                     bool
	)

	command := &cobra.Command{
		Use:   "cleanup-orphan SESSION",
		Short: "Inspect and safely clear ownership left after a session record was lost",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			options := app.OrphanCleanupOptions{
				SessionID:        args[0],
				SessionNamespace: r.global.sessionNamespace,
				SourceNamespace:  sourceNamespace,
				SourcePVC:        sourcePVC,
			}
			prefix := sessionCommandPrefixForCommand(cmd, r.global.sessionNamespace)
			validateCommand := fmt.Sprintf(
				"%s recovery cleanup-orphan %s --source-namespace %s --source-pvc %s",
				prefix,
				args[0],
				sourceNamespace,
				sourcePVC,
			)
			executeCommand := fmt.Sprintf(
				"%s --yes recovery cleanup-orphan %s --source-namespace %s --source-pvc %s --dry-run=false",
				prefix,
				args[0],
				sourceNamespace,
				sourcePVC,
			)

			plan, err := runtime.service.PlanOrphanCleanup(ctx, options)
			if err != nil {
				_, _ = fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan ownership inspection failed. Retry validation:",
					validateCommand,
				)

				return err
			}

			if !plan.Ready {
				if printErr := runtime.printer.Print(plan); printErr != nil {
					return printErr
				}

				_, _ = fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan cleanup is blocked by failed checks. Resolve them, then retry:",
					validateCommand,
				)

				return domain.NewError(
					domain.ErrorPrecondition,
					"cleanup orphan",
					"orphan cleanup plan contains failed checks",
				)
			}

			if dryRun {
				if err := runtime.printer.Print(plan); err != nil {
					return err
				}

				_, err := fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan cleanup validation passed. Execute:",
					executeCommand,
				)

				return err
			}

			if err := r.confirm(ctx, cmd, args[0]); err != nil {
				return reportApprovalError(cmd, err)
			}

			plan, err = runtime.service.CleanupOrphan(ctx, options)
			if err != nil {
				if plan != nil {
					_ = runtime.printer.Print(plan)
				}

				_, _ = fmt.Fprintln(
					cmd.ErrOrStderr(),
					"\nOrphan cleanup stopped before confirmed completion. Revalidate current resource state:",
					validateCommand,
				)

				return err
			}

			if err := runtime.printer.Print(plan); err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				cmd.ErrOrStderr(),
				"orphan ownership for session %s was cleared; the session record was already absent\n",
				args[0],
			)

			return err
		},
	}
	command.Flags().
		StringVarP(&sourceNamespace, "source-namespace", "n", "default", "Namespace of the owned source PVC")
	command.Flags().StringVar(&sourcePVC, "source-pvc", "", "Name of the owned source PVC")

	if err := command.MarkFlagRequired("source-pvc"); err != nil {
		panic(err)
	}

	bindDryRun(command, &dryRun)

	return command
}

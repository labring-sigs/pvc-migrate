package cli

import (
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

func (r *rootState) newRenameCommand() *cobra.Command {
	var (
		sessionID       string
		sourceNamespace string
		sourcePVC       string
		destinationPVC  string
		dryRun          bool
	)

	command := &cobra.Command{
		Use:   "rename",
		Short: "Rebind an offline PVC name within its namespace",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sourcePVC == "" {
				return domain.NewError(domain.ErrorValidation, "rename", "--source-pvc is required")
			}

			if destinationPVC == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"rename",
					"--destination-pvc is required",
				)
			}

			if sessionID == "" {
				generated, err := domain.NewSessionID(time.Now())
				if err != nil {
					return err
				}

				sessionID = generated
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			plan, err := runtime.planner.PlanRenamePVC(ctx, planner.RenamePlanOptions{
				SessionID: sessionID, SourceNamespace: sourceNamespace, SourcePVC: sourcePVC,
				DestinationPVC: destinationPVC, SessionNamespace: r.global.sessionNamespace,
			})
			if err != nil {
				return reportPlanningError(cmd, err)
			}

			if err := requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr()); err != nil {
				return err
			}

			if dryRun {
				return printPlanResult(cmd, runtime, plan)
			}

			if err := r.confirm(ctx, cmd, sourcePVC); err != nil {
				return reportApprovalError(cmd, err)
			}

			session, err := runtime.service.CreateSession(ctx, plan, false)
			if err != nil {
				return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
			}

			if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
				return err
			}

			if err := runtime.service.Rename(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	flags := command.Flags()
	flags.StringVar(&sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(&sourcePVC, "source-pvc", "", "Existing offline PVC name")
	flags.StringVar(&destinationPVC, "destination-pvc", "", "New PVC name in the source namespace")
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newRenamePlanCommand())
	r.addRenameLifecycle(command)

	return command
}

func (r *rootState) newRenamePlanCommand() *cobra.Command {
	var (
		sessionID       string
		sourceNamespace string
		sourcePVC       string
		destinationPVC  string
	)

	command := &cobra.Command{
		Use:   "plan",
		Short: "Inspect PVC rename checks without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sourcePVC == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"rename plan",
					"--source-pvc is required",
				)
			}

			if destinationPVC == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"rename plan",
					"--destination-pvc is required",
				)
			}

			if sessionID == "" {
				generated, err := domain.NewSessionID(time.Now())
				if err != nil {
					return err
				}

				sessionID = generated
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			plan, err := runtime.planner.PlanRenamePVC(ctx, planner.RenamePlanOptions{
				SessionID: sessionID, SourceNamespace: sourceNamespace, SourcePVC: sourcePVC,
				DestinationPVC: destinationPVC, SessionNamespace: r.global.sessionNamespace,
			})
			if err != nil {
				return reportPlanningError(cmd, err)
			}

			if err := printPlanResult(cmd, runtime, plan); err != nil {
				return err
			}

			return requireReady(plan)
		},
	}
	flags := command.Flags()
	flags.StringVar(&sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(&sourcePVC, "source-pvc", "", "Existing offline PVC name")
	flags.StringVar(&destinationPVC, "destination-pvc", "", "New PVC name in the source namespace")

	return command
}

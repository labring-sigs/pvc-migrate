package cli

import (
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

func (r *rootState) newMoveCommand() *cobra.Command {
	var (
		sessionID            string
		sourceNamespace      string
		sourcePVC            string
		destinationNamespace string
		destinationPVC       string
		dryRun               bool
	)

	command := &cobra.Command{
		Use:   "move",
		Short: "Move an offline PVC identity within or across namespaces",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sourcePVC == "" {
				return domain.NewError(domain.ErrorValidation, "move", "--source-pvc is required")
			}

			if destinationNamespace == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"move",
					"--destination-namespace is required",
				)
			}

			if destinationPVC == "" {
				destinationPVC = sourcePVC
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

			plan, err := runtime.planner.PlanMovePVC(ctx, planner.MovePlanOptions{
				SessionID: sessionID, SourceNamespace: sourceNamespace, SourcePVC: sourcePVC,
				DestinationNamespace: destinationNamespace, DestinationPVC: destinationPVC,
				SessionNamespace: r.global.sessionNamespace,
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

			if err := runtime.service.Move(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	flags := command.Flags()
	flags.StringVar(&sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(&sourcePVC, "source-pvc", "", "Existing offline PVC name")
	flags.StringVar(&destinationNamespace, "destination-namespace", "", "Destination namespace")
	flags.StringVar(
		&destinationPVC,
		"destination-pvc",
		"",
		"Destination PVC name; defaults to the source name",
	)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newMovePlanCommand())
	r.addMoveLifecycle(command)

	return command
}

func (r *rootState) newMovePlanCommand() *cobra.Command {
	var (
		sessionID            string
		sourceNamespace      string
		sourcePVC            string
		destinationNamespace string
		destinationPVC       string
	)

	command := &cobra.Command{
		Use:   "plan",
		Short: "Inspect PVC move checks without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sourcePVC == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"move plan",
					"--source-pvc is required",
				)
			}

			if destinationNamespace == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"move plan",
					"--destination-namespace is required",
				)
			}

			if destinationPVC == "" {
				destinationPVC = sourcePVC
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

			plan, err := runtime.planner.PlanMovePVC(ctx, planner.MovePlanOptions{
				SessionID: sessionID, SourceNamespace: sourceNamespace, SourcePVC: sourcePVC,
				DestinationNamespace: destinationNamespace, DestinationPVC: destinationPVC,
				SessionNamespace: r.global.sessionNamespace,
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
	flags.StringVar(&destinationNamespace, "destination-namespace", "", "Destination namespace")
	flags.StringVar(
		&destinationPVC,
		"destination-pvc",
		"",
		"Destination PVC name; defaults to the source name",
	)

	return command
}

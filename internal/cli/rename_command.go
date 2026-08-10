package cli

import (
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

func (r *rootState) newRenameCommand() *cobra.Command {
	return r.newPVCIdentityCommand(false)
}

func (r *rootState) newMoveCommand() *cobra.Command {
	return r.newPVCIdentityCommand(true)
}

func (r *rootState) newPVCIdentityCommand(move bool) *cobra.Command {
	var sessionID string
	var sourceNamespace string
	var sourcePVC string
	var destinationNamespace string
	var destinationPVC string
	var dryRun bool
	operation := domain.OperationRename
	use := "rename"
	short := "Rebind an offline PVC name within its namespace"
	if move {
		operation = domain.OperationMove
		use = "move"
		short = "Move an offline PVC identity to another namespace"
	}
	command := &cobra.Command{
		Use: use,
		Aliases: func() []string {
			if move {
				return []string{"mv"}
			}
			return nil
		}(),
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sourcePVC == "" {
				return domain.NewError(domain.ErrorValidation, use, "--source-pvc is required")
			}
			if move {
				if destinationNamespace == "" {
					return domain.NewError(domain.ErrorValidation, use, "--destination-namespace is required")
				}
				if destinationPVC == "" {
					destinationPVC = sourcePVC
				}
			} else {
				destinationNamespace = sourceNamespace
				if destinationPVC == "" {
					return domain.NewError(domain.ErrorValidation, use, "--destination-pvc is required")
				}
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
			plan, err := runtime.planner.PlanRename(ctx, planner.RenameOptions{
				Operation:            operation,
				SessionID:            sessionID,
				SourceNamespace:      sourceNamespace,
				SourcePVC:            sourcePVC,
				DestinationNamespace: destinationNamespace,
				DestinationPVC:       destinationPVC,
				SessionNamespace:     r.global.sessionNamespace,
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
			if err := r.confirm(cmd, sourcePVC); err != nil {
				return reportPreSessionError(cmd, err)
			}
			session, err := runtime.service.CreateSession(ctx, plan, false)
			if err != nil {
				return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
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
	if move {
		flags.StringVar(&destinationNamespace, "destination-namespace", "", "Destination namespace")
		flags.StringVar(&destinationPVC, "destination-pvc", "", "Destination PVC name; defaults to the source name")
	} else {
		flags.StringVar(&destinationPVC, "destination-pvc", "", "New PVC name in the source namespace")
	}
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newPVCIdentityPlanCommand(move))
	return command
}

func (r *rootState) newPVCIdentityPlanCommand(move bool) *cobra.Command {
	var sessionID string
	var sourceNamespace string
	var sourcePVC string
	var destinationNamespace string
	var destinationPVC string
	operation := domain.OperationRename
	use := "rename"
	if move {
		operation = domain.OperationMove
		use = "move"
	}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Inspect PVC identity checks without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sourcePVC == "" {
				return domain.NewError(domain.ErrorValidation, use, "--source-pvc is required")
			}
			if move {
				if destinationNamespace == "" {
					return domain.NewError(domain.ErrorValidation, use, "--destination-namespace is required")
				}
				if destinationPVC == "" {
					destinationPVC = sourcePVC
				}
			} else {
				destinationNamespace = sourceNamespace
				if destinationPVC == "" {
					return domain.NewError(domain.ErrorValidation, use, "--destination-pvc is required")
				}
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
			plan, err := runtime.planner.PlanRename(ctx, planner.RenameOptions{
				Operation:            operation,
				SessionID:            sessionID,
				SourceNamespace:      sourceNamespace,
				SourcePVC:            sourcePVC,
				DestinationNamespace: destinationNamespace,
				DestinationPVC:       destinationPVC,
				SessionNamespace:     r.global.sessionNamespace,
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
	if move {
		flags.StringVar(&destinationNamespace, "destination-namespace", "", "Destination namespace")
		flags.StringVar(&destinationPVC, "destination-pvc", "", "Destination PVC name; defaults to the source name")
	} else {
		flags.StringVar(&destinationPVC, "destination-pvc", "", "New PVC name in the source namespace")
	}
	return command
}

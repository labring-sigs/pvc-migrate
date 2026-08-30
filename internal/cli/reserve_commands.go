package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newReserveCommand() *cobra.Command {
	flags := &reserveFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "reserve",
		Short: "Provision and retain staged destination PVCs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			existing := targetsExistingSession(flags.sessionID, flags.sourcePVCs, flags.podName)
			if err := validateDestinationCapacityFlags(
				domain.OperationReserve,
				existing,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return reportPreSessionError(cmd, err)
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if existing {
				session, err := runtime.store.Get(ctx, r.global.sessionNamespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(
						cmd,
						r.global.sessionNamespace,
						flags.sessionID,
						err,
					)
				}

				if err := requireCLISessionType(
					session,
					domain.SessionTypeReserve,
					"reserve",
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				if dryRun {
					if err := runtime.service.ValidateReservation(ctx, session); err != nil {
						return reportSessionError(cmd, session, err)
					}
					return printSessionResult(cmd, runtime, session)
				}

				if err := runtime.service.Reserve(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			options, err := flags.planOptions(r)
			if err != nil {
				return err
			}

			plan, err := runtime.planner.PlanReserve(ctx, options)
			if err != nil {
				return reportPlanningError(cmd, err)
			}

			if err := requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr()); err != nil {
				return err
			}

			session, err := runtime.service.CreateSession(ctx, plan, dryRun)
			if err != nil {
				return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
			}

			if dryRun {
				return printPlanResult(cmd, runtime, plan)
			}

			if err := runtime.service.Reserve(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	flags.bind(command)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newReservePlanCommand())
	r.addReserveLifecycle(command)

	return command
}

func (r *rootState) newReservePlanCommand() *cobra.Command {
	flags := &reserveFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Inventory resources and validate this reservation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			existing := targetsExistingSession(flags.sessionID, flags.sourcePVCs, flags.podName)
			if err := validateDestinationCapacityFlags(
				domain.OperationReserve,
				existing,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return err
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if existing {
				session, err := runtime.store.Get(ctx, r.global.sessionNamespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(
						cmd,
						r.global.sessionNamespace,
						flags.sessionID,
						err,
					)
				}

				if err := requireCLISessionType(
					session,
					domain.SessionTypeReserve,
					"reserve plan",
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				if err := runtime.service.ValidateReservation(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			options, err := flags.planOptions(r)
			if err != nil {
				return err
			}

			plan, err := runtime.planner.PlanReserve(ctx, options)
			if err != nil {
				return reportPlanningError(cmd, err)
			}

			if err := printPlanResult(cmd, runtime, plan); err != nil {
				return err
			}

			return requireReady(plan)
		},
	}
	flags.bind(command)

	return command
}

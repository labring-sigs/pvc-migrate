package cli

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

// getCopySession loads an existing Copy session and accepts the documented
// Reservation-to-Copy hand-off. CRD storage keeps each workflow type in its
// own resource, so the fallback is required only when the Copy lookup reports
// that the session does not exist.
func getCopySession(
	ctx context.Context,
	store kube.SessionStore,
	namespace, id string,
) (*domain.Session, error) {
	session, err := kube.GetSessionByType(ctx, store, namespace, id, domain.SessionTypeCopy)
	if err == nil || !kube.IsSessionNotFound(err) {
		return session, err
	}

	return kube.GetSessionByType(ctx, store, namespace, id, domain.SessionTypeReserve)
}

// adoptReservedSessionForCopy is the explicit hand-off from the standalone
// reserve command to copy. The conversion lives in copy's command module so
// reserve and copy do not share a mixed command/flag implementation.
func adoptReservedSessionForCopy(session *domain.Session, flags *copyFlags) error {
	if session.Spec.Type == domain.SessionTypeReserve {
		options := session.Spec.WorkflowOptions()
		options.SourceNode = flags.sourceNode
		options.Strategies = planner.ResolveStrategies(
			session.Spec.SourceNamespace,
			session.Spec.DestinationNamespace,
			flags.strategies,
		)
		options.VerifyChecksum = flags.verifyChecksum
		options.DeleteExtraneous = flags.deleteExtraneous

		session.Spec = domain.NewSessionSpec(
			domain.OperationCopy,
			session.Spec.SessionCommon,
			flags.online,
			options,
		)
	}

	if flags.online {
		if session.Spec.Type != domain.SessionTypeCopy || session.Spec.Copy == nil {
			return domain.NewError(
				domain.ErrorPrecondition,
				"copy",
				"--online requires a copy session",
			)
		}

		session.Spec.Copy.Online = true
	}

	if flags.sourceNode != "" {
		options := session.Spec.WorkflowOptionsPtr()
		if options == nil {
			return domain.NewError(
				domain.ErrorValidation,
				"copy",
				"session workflow options are missing",
			)
		}

		options.SourceNode = flags.sourceNode
	}

	return nil
}

func (r *rootState) newCopyCommand() *cobra.Command {
	flags := &copyFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "copy",
		Short: "Run an idempotent offline or online warm copy without workload cutover",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			existing := targetsExistingSession(flags.sessionID, flags.sourcePVCs, flags.podName)
			if err := validateDestinationCapacityFlags(
				domain.OperationCopy,
				existing,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return reportPreSessionError(cmd, err)
			}

			if flags.podName != "" && len(flags.sourcePVCs) > 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"copy",
					"--source-pvc cannot be combined with --pod; the Pod PVC set is copied as one unit",
				)
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			var (
				session *domain.Session
				plan    *domain.MigrationPlan
			)
			if existing {
				namespace := workflowNamespaceForCommand(r, cmd)

				session, err = getCopySession(ctx, runtime.store, namespace, flags.sessionID)
				if err == nil {
					err = adoptReservedSessionForCopy(session, flags)
				}

				if err == nil {
					err = requireCLISessionType(session, domain.SessionTypeCopy, "copy")
				}
			} else {
				var options planner.CopyOptions

				options, err = flags.planOptions(r, false)
				if err == nil {
					options.SessionNamespace, options.TemporaryNamespace = r.controllerPlanNamespaces(
						runtime,
						domain.SessionTypeCopy,
						options.SourceNamespace,
						options.DestinationNamespace,
						options.TemporaryNamespace,
						false,
					)
				}

				if err == nil {
					plan, err = runtime.planner.PlanCopy(ctx, options)
				}

				if err == nil {
					err = requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr())
				}

				if err == nil {
					session, err = runtime.service.CreateSession(ctx, plan, dryRun)
				}
			}

			if err != nil {
				if session != nil {
					return reportSessionError(cmd, session, err)
				}
				return err
			}

			if dryRun {
				if existing {
					if err := runtime.service.ValidateWarmCopy(ctx, session); err != nil {
						return reportSessionError(cmd, session, err)
					}
					return printCopyDryRunResult(cmd, runtime, session, flags)
				}

				return printPlanResult(cmd, runtime, plan)
			}

			if existing && runtime.mode == executionModeController &&
				session.Backend == kube.SessionBackendCRD {
				if err := runtime.store.Update(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
			}

			if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
				return err
			}

			if err := runtime.service.ValidateWarmCopy(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if err := runtime.service.Reserve(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			if err := runtime.service.WarmCopy(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}

			return printSessionResult(cmd, runtime, session)
		},
	}
	flags.bind(command)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newCopyPlanCommand())
	r.addCopyLifecycle(command)

	return command
}

func (r *rootState) newCopyPlanCommand() *cobra.Command {
	flags := &copyFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Inventory resources and validate this copy",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			existing := targetsExistingSession(flags.sessionID, flags.sourcePVCs, flags.podName)
			if err := validateDestinationCapacityFlags(
				domain.OperationCopy,
				existing,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return err
			}

			if flags.podName != "" && len(flags.sourcePVCs) > 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"copy plan",
					"--source-pvc cannot be combined with --pod; the Pod PVC set is copied as one unit",
				)
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if existing {
				namespace := workflowNamespaceForCommand(r, cmd)

				session, err := getCopySession(ctx, runtime.store, namespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(
						cmd,
						namespace,
						flags.sessionID,
						err,
					)
				}

				if err := adoptReservedSessionForCopy(session, flags); err != nil {
					return reportSessionError(cmd, session, err)
				}

				if err := requireCLISessionType(
					session,
					domain.SessionTypeCopy,
					"copy plan",
				); err != nil {
					return reportSessionError(cmd, session, err)
				}

				if err := runtime.service.ValidateWarmCopy(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			options, err := flags.planOptions(r, false)
			if err != nil {
				return err
			}

			options.SessionNamespace, options.TemporaryNamespace = r.controllerPlanNamespaces(
				runtime,
				domain.SessionTypeCopy,
				options.SourceNamespace,
				options.DestinationNamespace,
				options.TemporaryNamespace,
				false,
			)

			plan, err := runtime.planner.PlanCopy(ctx, options)
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

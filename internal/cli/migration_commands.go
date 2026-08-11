package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

type migrationFlags struct {
	sessionID            string
	sourceNamespace      string
	temporaryNamespace   string
	destinationNamespace string
	sourcePVCs           []string
	destinationPVCs      []string
	podName              string
	sourceNode           string
	targetNode           string
	destinationClass     string
	capacityAwareness    string
	strategies           []string
	verifyChecksum       bool
	deleteExtraneous     bool
	switchoverCandidate  string
	allowLeaderDowntime  bool
	precopyPasses        int
	online               bool
}

func (f *migrationFlags) bind(command *cobra.Command, includePod, includeSourceNode, includeController, includePrecopy bool) {
	flags := command.Flags()
	flags.StringVar(&f.sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&f.sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(&f.temporaryNamespace, "temporary-namespace", "pvc-migrate-system", "Namespace for staged destination PVCs")
	flags.StringVar(&f.destinationNamespace, "destination-namespace", "", "Destination namespace; defaults to source namespace")
	flags.StringSliceVar(&f.sourcePVCs, "source-pvc", nil, "Source PVC name; repeat for multiple claims")
	flags.StringSliceVar(&f.destinationPVCs, "destination-pvc", nil, "Destination PVC name; repeat in source PVC order")
	if includeSourceNode {
		flags.StringVar(&f.sourceNode, "source-node", "", "Source tool node; inferred from active consumers when possible")
	}
	flags.StringVar(&f.targetNode, "target-node", domain.AutoValue, "Target node for provisioning and copy tools; auto selects a compatible Ready node")
	flags.StringVar(&f.destinationClass, "destination-storage-class", "", "Destination StorageClass; defaults to each source class")
	flags.StringVar(&f.capacityAwareness, "capacity-awareness", string(domain.CapacityAwarenessAuto), "CSIStorageCapacity policy: auto, require, or off")
	flags.StringSliceVar(&f.strategies, "strategy", []string{domain.StrategyAuto}, "pv-migrate strategy order; auto selects a topology-compatible order")
	flags.BoolVar(&f.verifyChecksum, "verify-checksum", true, "Use rsync checksum comparison during final sync")
	flags.BoolVar(&f.deleteExtraneous, "delete-extraneous", true, "Delete destination files absent from the source")
	if includePrecopy {
		flags.IntVar(&f.precopyPasses, "precopy-passes", 1, "Warm-copy passes before workload pause")
	}
	if includePod {
		podDescription := "Pod whose PVCs define the operation set"
		if includeController {
			podDescription = "Stateful Pod migration unit"
		}
		flags.StringVar(&f.podName, "pod", "", podDescription)
	}
	if includePod && includeController {
		flags.StringVar(&f.switchoverCandidate, "kubeblocks-candidate", "", "KubeBlocks switchover target when the selected instance is primary")
		flags.BoolVar(&f.allowLeaderDowntime, "allow-leader-downtime", false, "Acknowledge downtime for a leader, primary, or master instance")
	}
}

func (f *migrationFlags) planOptions(state *rootState, operation domain.Operation, useTemporary bool) (planner.Options, error) {
	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return planner.Options{}, err
		}
		id = generated
		f.sessionID = id
	}
	destinationNamespace := f.destinationNamespace
	if destinationNamespace == "" {
		destinationNamespace = f.sourceNamespace
	}
	stagingNamespace := destinationNamespace
	temporaryNamespace := destinationNamespace
	if useTemporary {
		stagingNamespace = f.temporaryNamespace
		temporaryNamespace = f.temporaryNamespace
	}
	return planner.Options{
		SessionID:            id,
		Operation:            operation,
		SourceNamespace:      f.sourceNamespace,
		TemporaryNamespace:   temporaryNamespace,
		DestinationNamespace: destinationNamespace,
		SessionNamespace:     state.global.sessionNamespace,
		StagingNamespace:     stagingNamespace,
		SourcePVCs:           append([]string(nil), f.sourcePVCs...),
		DestinationPVCs:      append([]string(nil), f.destinationPVCs...),
		PodName:              f.podName,
		SourceNode:           f.sourceNode,
		TargetNode:           f.targetNode,
		ToolImage:            state.global.toolImage,
		DestinationClass:     f.destinationClass,
		CapacityAwareness:    domain.CapacityAwareness(f.capacityAwareness),
		Strategies:           append([]string(nil), f.strategies...),
		Online:               f.online,
		VerifyChecksum:       f.verifyChecksum,
		DeleteExtraneous:     f.deleteExtraneous,
		SwitchoverCandidate:  f.switchoverCandidate,
		AllowLeaderDowntime:  f.allowLeaderDowntime,
	}, nil
}

func (r *rootState) newMigrationPlanCommand(operation domain.Operation, useTemporary, includePod, includeSourceNode, includeController, includePrecopy bool, includeOnline bool) *cobra.Command {
	flags := &migrationFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Inventory resources and validate this operation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if operation == domain.OperationMigratePod && flags.podName == "" {
				return domain.NewError(domain.ErrorValidation, "migrate-pod plan", "--pod is required")
			}
			if flags.podName != "" && len(flags.sourcePVCs) > 0 {
				return domain.NewError(domain.ErrorValidation, "plan", "--source-pvc cannot be combined with --pod; the Pod PVC set is migrated as one unit")
			}
			if (operation == domain.OperationMigrate || operation == domain.OperationMigratePod) && flags.destinationNamespace != "" && flags.destinationNamespace != flags.sourceNamespace {
				return domain.NewError(domain.ErrorPrecondition, "plan", "an orchestrated migration keeps application PVCs in the source namespace")
			}
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			if targetsExistingSession(flags) && (operation == domain.OperationReserve || operation == domain.OperationCopy) {
				session, err := runtime.store.Get(ctx, r.global.sessionNamespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(cmd, r.global.sessionNamespace, flags.sessionID, err)
				}
				if operation == domain.OperationCopy {
					if err := prepareCopySession(session, flags); err != nil {
						return reportSessionError(cmd, session, err)
					}
				}
				if err := runtime.service.ValidateReservation(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}
			options, err := flags.planOptions(r, operation, useTemporary)
			if err != nil {
				return err
			}
			plan, err := runtime.planner.Plan(ctx, options)
			if err != nil {
				return reportPlanningError(cmd, err)
			}
			if err := printPlanResult(cmd, runtime, plan); err != nil {
				return err
			}
			return requireReady(plan)
		},
	}
	flags.bind(command, includePod, includeSourceNode, includeController, includePrecopy)
	if includeOnline {
		command.Flags().BoolVar(&flags.online, "online", false, "Allow active PVC consumers for one finite warm-copy pass")
	}
	return command
}

func (r *rootState) newReserveCommand() *cobra.Command {
	flags := &migrationFlags{}
	var dryRun bool
	command := &cobra.Command{
		Use:   "reserve",
		Short: "Provision and retain staged destination PVCs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			if targetsExistingSession(flags) {
				session, err := runtime.store.Get(ctx, r.global.sessionNamespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(cmd, r.global.sessionNamespace, flags.sessionID, err)
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
			options, err := flags.planOptions(r, domain.OperationReserve, true)
			if err != nil {
				return err
			}
			plan, err := runtime.planner.Plan(ctx, options)
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
	flags.bind(command, true, false, false, false)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newMigrationPlanCommand(domain.OperationReserve, true, true, false, false, false, false))
	return command
}

func (r *rootState) newCopyCommand() *cobra.Command {
	flags := &migrationFlags{}
	var dryRun bool
	command := &cobra.Command{
		Use:   "copy",
		Short: "Run an idempotent offline or online warm copy without workload cutover",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			var session *domain.Session
			var plan *domain.MigrationPlan
			if targetsExistingSession(flags) {
				session, err = runtime.store.Get(ctx, r.global.sessionNamespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(cmd, r.global.sessionNamespace, flags.sessionID, err)
				}
				err = prepareCopySession(session, flags)
			} else {
				var options planner.Options
				options, err = flags.planOptions(r, domain.OperationCopy, false)
				if err == nil {
					plan, err = runtime.planner.Plan(ctx, options)
					if err != nil {
						return reportPlanningError(cmd, err)
					}
					if err == nil {
						err = requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr())
					}
					if err == nil {
						session, err = runtime.service.CreateSession(ctx, plan, dryRun)
						if err != nil {
							return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
						}
					}
				}
			}
			if err != nil {
				if session != nil {
					return reportSessionError(cmd, session, err)
				}
				return err
			}
			if dryRun {
				if targetsExistingSession(flags) {
					if err := runtime.service.ValidateReservation(ctx, session); err != nil {
						return reportSessionError(cmd, session, err)
					}
					return printSessionResult(cmd, runtime, session)
				}
				return printPlanResult(cmd, runtime, plan)
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
	flags.bind(command, true, true, false, false)
	command.Flags().BoolVar(&flags.online, "online", false, "Allow active PVC consumers for one finite warm-copy pass")
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newMigrationPlanCommand(domain.OperationCopy, false, true, true, false, false, true))
	return command
}

func targetsExistingSession(flags *migrationFlags) bool {
	return flags.sessionID != "" && len(flags.sourcePVCs) == 0 && flags.podName == ""
}

func prepareCopySession(session *domain.Session, flags *migrationFlags) error {
	if session.Spec.Type == domain.SessionTypeReserve {
		session.Spec = domain.NewSessionSpec(domain.OperationCopy, session.Spec.SessionCommon, session.Spec.Workload(), flags.online, session.Spec.WorkflowOptions())
	}
	if flags.online {
		if session.Spec.Type != domain.SessionTypeCopy || session.Spec.Copy == nil {
			return domain.NewError(domain.ErrorPrecondition, "copy", "--online requires a copy session")
		}
		session.Spec.Copy.Online = true
	}
	if flags.sourceNode != "" {
		options := session.Spec.WorkflowOptionsPtr()
		if options == nil {
			return domain.NewError(domain.ErrorValidation, "copy", "session workflow options are missing")
		}
		options.SourceNode = flags.sourceNode
	}
	return nil
}

func (r *rootState) newFinalSyncCommand() *cobra.Command {
	var sessionID string
	var dryRun bool
	command := &cobra.Command{
		Use:   "final-sync",
		Short: "Pause the workload and run the final offline sync",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sessionID == "" {
				return domain.NewError(domain.ErrorValidation, "final-sync", "--session is required")
			}
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, sessionID)
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, sessionID, err)
			}
			if !session.Spec.Orchestrated() {
				return reportSessionError(cmd, session, domain.NewError(domain.ErrorPrecondition, "final-sync", "final sync requires an orchestrated migration session"))
			}
			if dryRun {
				if err := runtime.service.ValidateFinalSync(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}
			if err := r.confirm(cmd, sessionID); err != nil {
				return reportSessionError(cmd, session, err)
			}
			alreadyPaused := session.Status.Phase == domain.PhasePaused || session.Status.Phase == domain.PhaseFinalSyncing || session.Status.Phase == domain.PhaseFinalSynced || (session.Status.Phase == domain.PhaseFailed && session.Status.ResumeFrom == domain.PhaseFinalSyncing)
			if !alreadyPaused {
				if err := runtime.service.Pause(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
			}
			if err := runtime.service.FinalSync(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	command.Flags().StringVar(&sessionID, "session", "", "Migration session ID")
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newSessionFlagPlanCommand("final-sync", func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
		if !session.Spec.Orchestrated() {
			return domain.NewError(domain.ErrorPrecondition, "final-sync", "final sync requires an orchestrated migration session")
		}
		return runtime.service.ValidateFinalSync(ctx, session)
	}))
	return command
}

func (r *rootState) newActivateCommand() *cobra.Command {
	var sessionID string
	var dryRun bool
	command := &cobra.Command{
		Use:   "activate",
		Short: "Bind staged PVs to application PVC identities",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sessionID == "" {
				return domain.NewError(domain.ErrorValidation, "activate", "--session is required")
			}
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, sessionID)
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, sessionID, err)
			}
			if dryRun {
				if err := runtime.service.ValidateActivation(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}
				return printSessionResult(cmd, runtime, session)
			}
			if err := r.confirm(cmd, sessionID); err != nil {
				return reportSessionError(cmd, session, err)
			}
			if err := runtime.service.Activate(ctx, session); err != nil {
				return reportSessionError(cmd, session, err)
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	command.Flags().StringVar(&sessionID, "session", "", "Migration session ID")
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newSessionFlagPlanCommand("activate", func(ctx context.Context, runtime *commandRuntime, session *domain.Session) error {
		return runtime.service.ValidateActivation(ctx, session)
	}))
	return command
}

func (r *rootState) newSessionFlagPlanCommand(action string, validate func(context.Context, *commandRuntime, *domain.Session) error) *cobra.Command {
	var sessionID string
	command := &cobra.Command{
		Use:   "plan",
		Short: "Validate the persisted session stage without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if sessionID == "" {
				return domain.NewError(domain.ErrorValidation, action+" plan", "--session is required")
			}
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			session, err := runtime.store.Get(ctx, r.global.sessionNamespace, sessionID)
			if err != nil {
				return reportSessionLookupError(cmd, r.global.sessionNamespace, sessionID, err)
			}
			if err := validate(ctx, runtime, session); err != nil {
				return reportSessionError(cmd, session, err)
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	command.Flags().StringVar(&sessionID, "session", "", "Migration session ID")
	return command
}

func (r *rootState) newMigrateCommand(podMode bool) *cobra.Command {
	flags := &migrationFlags{}
	var dryRun bool
	use := "migrate"
	short := "Run reserve, warm copy, pause, final sync, activate, and resume"
	operation := domain.OperationMigrate
	if podMode {
		use = "migrate-pod"
		short = "Migrate every PVC of one supported Pod as a single unit"
		operation = domain.OperationMigratePod
	}
	command := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if podMode && flags.podName == "" {
				return domain.NewError(domain.ErrorValidation, use, "--pod is required")
			}
			if flags.podName != "" && len(flags.sourcePVCs) > 0 {
				return domain.NewError(domain.ErrorValidation, use, "--source-pvc cannot be combined with --pod; the Pod PVC set is migrated as one unit")
			}
			if flags.precopyPasses < 0 {
				return domain.NewError(domain.ErrorValidation, use, "--precopy-passes cannot be negative")
			}
			if flags.destinationNamespace != "" && flags.destinationNamespace != flags.sourceNamespace {
				return domain.NewError(domain.ErrorPrecondition, use, "an orchestrated migration keeps application PVCs in the source namespace")
			}
			runtime, err := r.runtime()
			if err != nil {
				return err
			}
			ctx, cancel := r.context(cmd.Context())
			defer cancel()
			options, err := flags.planOptions(r, operation, true)
			if err != nil {
				return err
			}
			plan, err := runtime.planner.Plan(ctx, options)
			if err != nil {
				return reportPlanningError(cmd, err)
			}
			if err := requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr()); err != nil {
				return err
			}
			if dryRun {
				return printPlanResult(cmd, runtime, plan)
			}
			if err := r.confirm(cmd, approvalIdentity(flags)); err != nil {
				return reportPreSessionError(cmd, err)
			}
			session, err := runtime.service.CreateSession(ctx, plan, false)
			if err != nil {
				return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
			}
			if err := runtime.service.Migrate(ctx, session, flags.precopyPasses); err != nil {
				return reportSessionError(cmd, session, err)
			}
			return printSessionResult(cmd, runtime, session)
		},
	}
	flags.bind(command, true, true, true, true)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newMigrationPlanCommand(operation, true, true, true, true, false, false))
	return command
}

func (r *rootState) confirm(command *cobra.Command, expected string) error {
	if r.global.assumeYes {
		return nil
	}
	if expected == "" {
		return domain.NewError(domain.ErrorValidation, "approval", "approval identity is empty")
	}
	if _, err := fmt.Fprintf(command.ErrOrStderr(), "Type %s to approve: ", expected); err != nil {
		return err
	}
	var actual string
	if _, err := fmt.Fscan(command.InOrStdin(), &actual); err != nil {
		return domain.WrapError(domain.ErrorPrecondition, "approval", "typed approval or --yes is required", err)
	}
	if actual != expected {
		return domain.NewError(domain.ErrorPrecondition, "approval", "typed approval did not match")
	}
	return nil
}

func approvalIdentity(flags *migrationFlags) string {
	if flags.podName != "" {
		return flags.podName
	}
	if len(flags.sourcePVCs) > 0 {
		return flags.sourcePVCs[0]
	}
	return flags.sessionID
}

func requireReady(plan *domain.MigrationPlan) error {
	if plan.Ready {
		return nil
	}
	return domain.NewError(domain.ErrorPrecondition, "plan", "migration plan contains failed checks")
}

func requireReadyWithOutput(runtime *commandRuntime, plan *domain.MigrationPlan, guidance ...io.Writer) error {
	if plan.Ready {
		return nil
	}
	if err := runtime.printer.Print(plan); err != nil {
		return err
	}
	for _, writer := range guidance {
		if writer == nil {
			continue
		}
		if _, err := fmt.Fprintln(writer, "\nNo session or migration resources were created. Resolve the failed plan checks, then rerun the command."); err != nil {
			return err
		}
	}
	return requireReady(plan)
}

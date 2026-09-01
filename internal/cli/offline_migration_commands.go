package cli

import (
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

type offlineMigrationFlags struct {
	sessionID             string
	sourceNamespace       string
	temporaryNamespace    string
	destinationNamespace  string
	sourcePVCs            []string
	destinationPVCs       []string
	destinationCapacities []string
	sourcePaths           []string
	destinationPaths      []string
	allowVolumeShrink     bool
	skipSourceUsageCheck  bool
	sourceNode            string
	targetNode            string
	destinationClass      string
	capacityAwareness     string
	strategies            []string
	verifyChecksum        bool
	deleteExtraneous      bool
}

func (f *offlineMigrationFlags) bind(command *cobra.Command) {
	flags := command.Flags()
	flags.StringVar(&f.sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&f.sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(
		&f.temporaryNamespace,
		"temporary-namespace",
		"pvc-migrate-system",
		"Namespace for staged destination PVCs",
	)
	flags.StringVar(
		&f.destinationNamespace,
		"destination-namespace",
		"",
		"Destination namespace; defaults to source namespace",
	)
	flags.StringSliceVar(
		&f.sourcePVCs,
		"source-pvc",
		nil,
		"Source PVC name; repeat for multiple claims",
	)
	flags.StringSliceVar(
		&f.destinationPVCs,
		"destination-pvc",
		nil,
		"Destination PVC name; for multiple PVCs use source-pvc-name=destination-pvc-name",
	)
	flags.StringSliceVar(
		&f.destinationCapacities,
		"destination-capacity",
		nil,
		"Destination PVC storage capacity; one value applies to all PVCs, or use source-pvc-name=capacity for explicit mappings",
	)
	flags.StringArrayVar(
		&f.sourcePaths,
		"source-path",
		nil,
		"Source directory inside a PVC; repeat and use source-pvc-name=relative-path for multiple PVCs",
	)
	flags.StringArrayVar(
		&f.destinationPaths,
		"destination-path",
		nil,
		"Destination directory inside a PVC; repeat and use source-pvc-name=relative-path for multiple PVCs",
	)
	flags.BoolVar(
		&f.allowVolumeShrink,
		"allow-volume-shrink",
		false,
		"Allow destination capacity below the source PV capacity; only use when copied data is known to fit",
	)
	flags.BoolVar(
		&f.skipSourceUsageCheck,
		"skip-source-usage-check",
		false,
		"Skip the storage-backend CRD usage check for a smaller destination",
	)
	flags.StringVar(
		&f.sourceNode,
		"source-node",
		"",
		"Source tool node; inferred from active consumers when possible",
	)
	flags.StringVar(
		&f.targetNode,
		"target-node",
		domain.AutoValue,
		"Target node for provisioning and copy tools; auto selects a compatible Ready node",
	)
	flags.StringVar(
		&f.destinationClass,
		"destination-storage-class",
		"",
		"Destination StorageClass; defaults to each source class",
	)
	flags.StringVar(
		&f.capacityAwareness,
		"capacity-awareness",
		string(domain.CapacityAwarenessAuto),
		"CSIStorageCapacity policy: auto, require, or off",
	)
	flags.StringSliceVar(
		&f.strategies,
		"strategy",
		[]string{domain.StrategyAuto},
		"pv-migrate strategy order; auto selects a topology-compatible order",
	)
	flags.BoolVar(
		&f.verifyChecksum,
		"verify-checksum",
		true,
		"Use rsync checksum comparison during final sync",
	)
	flags.BoolVar(
		&f.deleteExtraneous,
		"delete-extraneous",
		true,
		"Delete destination files absent from the source",
	)
}

func (f *offlineMigrationFlags) planOptions(
	state *rootState,
	useTemporary bool,
) (planner.OfflineMigrationOptions, error) {
	if err := validateDestinationCapacityFlags(
		domain.OperationMigrate,
		false,
		f.destinationCapacities,
		f.allowVolumeShrink,
		f.skipSourceUsageCheck,
		f.sourcePaths,
		f.destinationPaths,
	); err != nil {
		return planner.OfflineMigrationOptions{}, err
	}

	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return planner.OfflineMigrationOptions{}, err
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

	return planner.OfflineMigrationOptions{
		SessionID:             id,
		SourceNamespace:       f.sourceNamespace,
		TemporaryNamespace:    temporaryNamespace,
		DestinationNamespace:  destinationNamespace,
		SessionNamespace:      state.global.sessionNamespace,
		StagingNamespace:      stagingNamespace,
		ToolImage:             state.global.toolImage,
		SourcePVCs:            append([]string(nil), f.sourcePVCs...),
		DestinationPVCs:       append([]string(nil), f.destinationPVCs...),
		DestinationCapacities: append([]string(nil), f.destinationCapacities...),
		SourcePaths:           append([]string(nil), f.sourcePaths...),
		DestinationPaths:      append([]string(nil), f.destinationPaths...),
		AllowVolumeShrink:     f.allowVolumeShrink,
		SkipSourceUsageCheck:  f.skipSourceUsageCheck,
		SourceNode:            f.sourceNode,
		TargetNode:            f.targetNode,
		DestinationClass:      f.destinationClass,
		CapacityAwareness:     domain.CapacityAwareness(f.capacityAwareness),
		Strategies:            append([]string(nil), f.strategies...),
		VerifyChecksum:        f.verifyChecksum,
		DeleteExtraneous:      f.deleteExtraneous,
	}, nil
}

func (r *rootState) newMigrateCommand() *cobra.Command {
	flags := &offlineMigrationFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "migrate",
		Short: "Run a complete offline PVC migration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateDestinationCapacityFlags(
				domain.OperationMigrate,
				false,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return reportPreSessionError(cmd, err)
			}

			return r.runOfflineMigrateCommand(cmd, flags, dryRun)
		},
	}
	flags.bind(command)
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newOfflineMigrationPlanCommand(),
		r.newOfflineMigrationStatusCommand(),
		r.newOfflineMigrationResumeCommand(),
		r.newOfflineMigrationAbortCommand(),
		r.newOfflineMigrationRollbackCommand(),
		r.newOfflineMigrationCleanupCommand(),
	)

	return command
}

func (r *rootState) newOfflineMigrationPlanCommand() *cobra.Command {
	flags := &offlineMigrationFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Inventory resources and validate this offline migration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			existing := flags.sessionID != "" && len(flags.sourcePVCs) == 0
			if err := validateDestinationCapacityFlags(
				domain.OperationMigrate,
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
				namespace := workflowNamespaceForCommand(r, cmd)

				session, err := runtime.store.Get(ctx, namespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(
						cmd,
						namespace,
						flags.sessionID,
						err,
					)
				}

				if session.Spec.Operation() != domain.OperationMigrate {
					return reportSessionError(
						cmd,
						session,
						domain.NewError(
							domain.ErrorPrecondition,
							"migrate plan",
							"offline migrate plan requires an offline Migrate session",
						),
					)
				}

				if err := runtime.service.ValidateReservation(ctx, session); err != nil {
					return reportSessionError(cmd, session, err)
				}

				return printSessionResult(cmd, runtime, session)
			}

			options, err := flags.planOptions(r, true)
			if err != nil {
				return err
			}

			options.SessionNamespace, options.TemporaryNamespace = r.controllerPlanNamespaces(
				runtime,
				domain.SessionTypeMigrate,
				options.SourceNamespace,
				options.DestinationNamespace,
				options.TemporaryNamespace,
				cmd.Flags().Changed("temporary-namespace"),
			)
			options.StagingNamespace = options.TemporaryNamespace

			plan, err := runtime.planner.PlanOfflineMigration(ctx, options)
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

func (r *rootState) runOfflineMigrateCommand(
	cmd *cobra.Command,
	flags *offlineMigrationFlags,
	dryRun bool,
) error {
	runtime, err := r.runtime()
	if err != nil {
		return err
	}

	ctx, cancel := r.context(cmd.Context())
	defer cancel()

	options, err := flags.planOptions(r, true)
	if err != nil {
		return err
	}

	options.SessionNamespace, options.TemporaryNamespace = r.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigrate,
		options.SourceNamespace,
		options.DestinationNamespace,
		options.TemporaryNamespace,
		cmd.Flags().Changed("temporary-namespace"),
	)
	options.StagingNamespace = options.TemporaryNamespace

	plan, err := runtime.planner.PlanOfflineMigration(ctx, options)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if err := requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr()); err != nil {
		return err
	}

	if dryRun {
		return printPlanResult(cmd, runtime, plan)
	}

	if err := r.confirm(ctx, cmd, offlineApprovalIdentity(flags)); err != nil {
		return reportApprovalError(cmd, err)
	}

	session, err := runtime.service.CreateSession(ctx, plan, false)
	if err != nil {
		return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
	}

	if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
		return err
	}

	if err := runtime.service.OfflineMigrate(ctx, session); err != nil {
		return reportSessionError(cmd, session, err)
	}

	return printSessionResult(cmd, runtime, session)
}

func offlineApprovalIdentity(flags *offlineMigrationFlags) string {
	if len(flags.sourcePVCs) > 0 {
		return flags.sourcePVCs[0]
	}
	return flags.sessionID
}

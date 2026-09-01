package cli

import (
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
)

type podMigrationFlags struct {
	sessionID              string
	sourceNamespace        string
	temporaryNamespace     string
	destinationCapacities  []string
	sourcePaths            []string
	destinationPaths       []string
	allowVolumeShrink      bool
	skipSourceUsageCheck   bool
	sourceNode             string
	targetNode             string
	destinationClass       string
	capacityAwareness      string
	strategies             []string
	verifyChecksum         bool
	deleteExtraneous       bool
	podName                string
	switchoverCandidate    string
	allowLeaderDowntime    bool
	forceReprovision       bool
	precopyPasses          int
	openEBSLVMEnableShared bool
}

func (f *podMigrationFlags) bind(command *cobra.Command) {
	flags := command.Flags()
	flags.StringVar(&f.sessionID, "session", "", "Migration session ID")
	flags.StringVarP(&f.sourceNamespace, "source-namespace", "n", "default", "Pod namespace")
	flags.StringVar(
		&f.temporaryNamespace,
		"temporary-namespace",
		"pvc-migrate-system",
		"Namespace for staged destination PVCs",
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
		"Source tool node; inferred from the selected Pod",
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
	flags.IntVar(&f.precopyPasses, "precopy-passes", 1, "Warm-copy passes before workload pause")
	flags.BoolVar(
		&f.openEBSLVMEnableShared,
		"openebs-lvm-enable-shared",
		false,
		"Enable same-node shared mounts for OpenEBS LVM warm copy and multi-consumer RWO destinations",
	)
	flags.StringVar(&f.podName, "pod", "", "Stateful Pod migration unit")
	flags.StringVar(
		&f.switchoverCandidate,
		"kubeblocks-candidate",
		"",
		"Switchover target for a supported InstanceSet-backed KubeBlocks primary",
	)
	flags.BoolVar(
		&f.allowLeaderDowntime,
		"allow-leader-downtime",
		false,
		"Acknowledge selected leader downtime for InstanceSet-backed KubeBlocks or native StatefulSet scale-down",
	)
}

func (f *podMigrationFlags) bindForceReprovision(command *cobra.Command) {
	command.Flags().
		BoolVar(&f.forceReprovision, "force-reprovision", false, "Replace backing PVs when the Pod already uses the target node and StorageClass")
}

func (f *podMigrationFlags) planOptions(
	state *rootState,
	useTemporary bool,
) (planner.PodMigrationOptions, error) {
	if err := validateDestinationCapacityFlags(
		domain.OperationMigratePod,
		false,
		f.destinationCapacities,
		f.allowVolumeShrink,
		f.skipSourceUsageCheck,
		f.sourcePaths,
		f.destinationPaths,
	); err != nil {
		return planner.PodMigrationOptions{}, err
	}

	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return planner.PodMigrationOptions{}, err
		}

		id = generated
		f.sessionID = id
	}

	stagingNamespace := f.sourceNamespace

	temporaryNamespace := f.sourceNamespace
	if useTemporary {
		stagingNamespace = f.temporaryNamespace
		temporaryNamespace = f.temporaryNamespace
	}

	return planner.PodMigrationOptions{
		SessionID:              id,
		SourceNamespace:        f.sourceNamespace,
		TemporaryNamespace:     temporaryNamespace,
		SessionNamespace:       state.global.sessionNamespace,
		StagingNamespace:       stagingNamespace,
		ToolImage:              state.global.toolImage,
		DestinationCapacities:  append([]string(nil), f.destinationCapacities...),
		SourcePaths:            append([]string(nil), f.sourcePaths...),
		DestinationPaths:       append([]string(nil), f.destinationPaths...),
		AllowVolumeShrink:      f.allowVolumeShrink,
		SkipSourceUsageCheck:   f.skipSourceUsageCheck,
		PodName:                f.podName,
		SourceNode:             f.sourceNode,
		TargetNode:             f.targetNode,
		DestinationClass:       f.destinationClass,
		CapacityAwareness:      domain.CapacityAwareness(f.capacityAwareness),
		Strategies:             append([]string(nil), f.strategies...),
		VerifyChecksum:         f.verifyChecksum,
		DeleteExtraneous:       f.deleteExtraneous,
		SwitchoverCandidate:    f.switchoverCandidate,
		AllowLeaderDowntime:    f.allowLeaderDowntime,
		ForceReprovision:       f.forceReprovision,
		PrecopyPasses:          f.precopyPasses,
		OpenEBSLVMEnableShared: f.openEBSLVMEnableShared,
	}, nil
}

func (r *rootState) newMigratePodCommand() *cobra.Command {
	flags := &podMigrationFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "migrate-pod",
		Short: "Run a real-time Pod migration with warm copy and workload cutover",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateDestinationCapacityFlags(
				domain.OperationMigratePod,
				false,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return reportPreSessionError(cmd, err)
			}

			if flags.podName == "" {
				return domain.NewError(domain.ErrorValidation, "migrate-pod", "--pod is required")
			}

			if flags.precopyPasses < 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"migrate-pod",
					"--precopy-passes cannot be negative",
				)
			}

			return r.runPodMigrateCommand(cmd, flags, dryRun)
		},
	}
	flags.bind(command)
	flags.bindForceReprovision(command)
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newPodMigrationPlanCommand(),
		r.newPodMigrationStatusCommand(),
		r.newPodMigrationResumeCommand(),
		r.newPodMigrationAbortCommand(),
		r.newPodMigrationRollbackCommand(),
		r.newPodMigrationCleanupCommand(),
	)

	return command
}

func (r *rootState) newPodMigrationPlanCommand() *cobra.Command {
	flags := &podMigrationFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Inventory resources and validate this real-time Pod migration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			existing := flags.sessionID != "" && flags.podName == ""
			if err := validateDestinationCapacityFlags(
				domain.OperationMigratePod,
				existing,
				flags.destinationCapacities,
				flags.allowVolumeShrink,
				flags.skipSourceUsageCheck,
				flags.sourcePaths,
				flags.destinationPaths,
			); err != nil {
				return err
			}

			if flags.podName == "" && !existing {
				return domain.NewError(
					domain.ErrorValidation,
					"migrate-pod plan",
					"--pod is required",
				)
			}

			if flags.precopyPasses < 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"migrate-pod plan",
					"--precopy-passes cannot be negative",
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
				session, err := runtime.store.Get(ctx, namespace, flags.sessionID)
				if err != nil {
					return reportSessionLookupError(
						cmd,
						namespace,
						flags.sessionID,
						err,
					)
				}

				if session.Spec.Operation() != domain.OperationMigratePod {
					return reportSessionError(
						cmd,
						session,
						domain.NewError(
							domain.ErrorPrecondition,
							"migrate-pod plan",
							"migrate-pod plan requires a MigratePod session",
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
				domain.SessionTypeMigratePod,
				options.SourceNamespace,
				options.SourceNamespace,
				options.TemporaryNamespace,
				cmd.Flags().Changed("temporary-namespace"),
			)
			options.StagingNamespace = options.TemporaryNamespace

			plan, err := runtime.planner.PlanPodMigration(ctx, options)
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
	flags.bindForceReprovision(command)

	return command
}

func (r *rootState) runPodMigrateCommand(
	cmd *cobra.Command,
	flags *podMigrationFlags,
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
		domain.SessionTypeMigratePod,
		options.SourceNamespace,
		options.SourceNamespace,
		options.TemporaryNamespace,
		cmd.Flags().Changed("temporary-namespace"),
	)
	options.StagingNamespace = options.TemporaryNamespace

	plan, err := runtime.planner.PlanPodMigration(ctx, options)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if err := requireReadyWithOutput(runtime, plan, cmd.ErrOrStderr()); err != nil {
		return err
	}

	if dryRun {
		return printPlanResult(cmd, runtime, plan)
	}

	if err := r.confirm(ctx, cmd, podApprovalIdentity(flags)); err != nil {
		return reportApprovalError(cmd, err)
	}

	session, err := runtime.service.CreateSession(ctx, plan, false)
	if err != nil {
		return reportSessionCreationError(cmd, plan.SessionNamespace, plan.SessionID, err)
	}

	if deferred, err := deferControllerExecution(ctx, cmd, runtime, session); deferred {
		return err
	}

	if err := runtime.service.MigratePod(ctx, session); err != nil {
		return reportSessionError(cmd, session, err)
	}

	return printSessionResult(cmd, runtime, session)
}

func podApprovalIdentity(flags *podMigrationFlags) string {
	if flags.podName != "" {
		return flags.podName
	}
	return flags.sessionID
}

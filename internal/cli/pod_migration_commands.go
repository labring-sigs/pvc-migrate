package cli

import (
	"errors"
	"fmt"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type podMigrationFlags struct {
	sessionID               string
	sourceNamespace         string
	temporaryNamespace      string
	destinationCapacities   []string
	sourcePaths             []string
	destinationPaths        []string
	allowVolumeShrink       bool
	skipSourceUsageCheck    bool
	sourceNode              string
	targetNode              string
	destinationClass        string
	capacityAwareness       string
	strategies              []string
	verifyChecksum          bool
	deleteExtraneous        bool
	podName                 string
	switchoverCandidate     string
	allowLeaderDowntime     bool
	forceReprovision        bool
	allowPlacementViolation bool
	precopyPasses           int
	openEBSLVMEnableShared  bool
	unusedStoragePolicy     string
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
	flags.StringVar(
		&f.unusedStoragePolicy,
		"unused-storage-policy",
		string(v1alpha1.UnusedStorageKeep),
		"Keep or Delete replaced storage: Delete removes the old source PV after a completed cutover, or the staged destination after a rollback or abort; the PVC the workload runs on is always kept (default Keep)",
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
		false,
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
	flags.BoolVar(
		&f.allowPlacementViolation,
		"allow-placement-violation",
		false,
		"Proceed when the recreated Pod may violate required podAntiAffinity or topologySpread constraints on the target node",
	)
	flags.StringVar(&f.podName, "pod", "", "Stateful Pod migration unit")
	flags.StringVar(
		&f.switchoverCandidate,
		"switchover-candidate",
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

func (f *podMigrationFlags) workflow(
	state *rootState,
	runtime *commandRuntime,
	temporaryExplicit bool,
	submit bool,
) (*v1alpha1.ClusterPodMigration, error) {
	if err := validateDestinationCapacityFlags(
		domain.OperationMigratePod,
		false,
		f.destinationCapacities,
		f.allowVolumeShrink,
		f.skipSourceUsageCheck,
		f.sourcePaths,
		f.destinationPaths,
	); err != nil {
		return nil, err
	}

	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return nil, err
		}

		id = generated
		f.sessionID = id
	}

	sessionNamespace, temporaryNamespace := state.controllerPlanNamespaces(
		runtime,
		domain.SessionTypeMigratePod,
		f.sourceNamespace,
		f.sourceNamespace,
		f.temporaryNamespace,
		temporaryExplicit,
		submit,
	)

	object := &v1alpha1.ClusterPodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec: v1alpha1.ClusterPodMigrationSpec{
			SourceNamespace:    v1alpha1.NamespaceName(f.sourceNamespace),
			TemporaryNamespace: v1alpha1.NamespaceName(temporaryNamespace),
			SessionNamespace:   v1alpha1.NamespaceName(sessionNamespace),
			PodMigrationSpec: v1alpha1.PodMigrationSpec{
				Pod:                     v1alpha1.LocalResourceReference{Name: f.podName},
				PrecopyPasses:           f.precopyPasses,
				ForceReprovision:        f.forceReprovision,
				OpenEBSLVMEnableShared:  f.openEBSLVMEnableShared,
				SwitchoverCandidate:     f.switchoverCandidate,
				AllowLeaderDowntime:     f.allowLeaderDowntime,
				AllowPlacementViolation: f.allowPlacementViolation,
				TransferOptions: v1alpha1.TransferOptions{
					UnusedStoragePolicy: v1alpha1.UnusedStoragePolicy(
						f.unusedStoragePolicy,
					),
					DestinationStorageClass: f.destinationClass,
					CapacityAwareness:       f.capacityAwareness,
					SourceNode:              f.sourceNode,
					TargetNode:              f.targetNode,
					Strategies:              append([]string(nil), f.strategies...),
					VerifyChecksum:          f.verifyChecksum,
					DeleteExtraneous:        new(f.deleteExtraneous),
					AllowVolumeShrink:       f.allowVolumeShrink,
					SkipSourceUsageCheck:    f.skipSourceUsageCheck,
				},
			},
		},
	}
	if err := applyVolumeMappings(&object.Spec.TransferOptions, &object.Spec.Volumes, true,
		f.destinationCapacities, nil, f.sourcePaths, f.destinationPaths); err != nil {
		return nil, err
	}

	return object, nil
}

func (r *rootState) newMigratePodCommand() *cobra.Command {
	flags := &podMigrationFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "migrate-pod",
		Short: "Run a real-time Pod migration with warm copy and workload cutover in this session",
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

			return r.runPodMigrateCommand(cmd, flags, dryRun, false, false)
		},
	}
	flags.bind(command)
	flags.bindForceReprovision(command)
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newPodMigrationPlanCommand(),
		r.newPodMigrationCreateCommand(),
		r.newPodMigrationStatusCommand(),
		r.newPodMigrationResumeCommand(),
		r.newPodMigrationAbortCommand(),
		r.newPodMigrationRollbackCommand(),
		r.newPodMigrationCleanupCommand(),
	)

	return command
}

// newPodMigrationCreateCommand submits a declarative PodMigration workflow
// for controller reconciliation.
// newPodMigrationPlanCommand validates a Pod migration without mutations.
// Planning lives in the main dry-run path; this subcommand keeps the
// subcommand symmetry with the other operations.
func (r *rootState) newPodMigrationPlanCommand() *cobra.Command {
	flags := &podMigrationFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "plan",
		Short: "Inventory resources and validate this Pod migration",
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

			return r.runPodMigrateCommand(cmd, flags, true, false, false)
		},
	}
	flags.bind(command)
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newPodMigrationCreateCommand() *cobra.Command {
	flags := &podMigrationFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a PodMigration workflow for controller reconciliation",
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
				return domain.NewError(
					domain.ErrorValidation,
					"migrate-pod create",
					"--pod is required",
				)
			}

			if flags.precopyPasses < 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"migrate-pod create",
					"--precopy-passes cannot be negative",
				)
			}

			return r.runPodMigrateCommand(cmd, flags, dryRun, true, wait)
		},
	}
	flags.bind(command)
	flags.bindForceReprovision(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

func (r *rootState) runPodMigrateCommand(
	cmd *cobra.Command,
	flags *podMigrationFlags,
	dryRun bool,
	submit bool,
	wait bool,
) error {
	runtime, err := r.runtime()
	if err != nil {
		return err
	}

	ctx, cancel := r.context(cmd.Context())
	defer cancel()

	object, err := flags.workflow(r, runtime, cmd.Flags().Changed("temporary-namespace"), submit)
	if err != nil {
		return err
	}

	// Submission previews plan with controller semantics: submission RBAC and
	// CR-backed estimates, not the data-plane permissions local runs need.
	if submit && runtime.planner != nil {
		runtime.planner = runtime.planner.ForController()
	}

	// Only controller submission depends on the workflow CRDs being served.
	if submit {
		if err := requireControllerWorkflow(runtime, domain.SessionTypeMigratePod); err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	if submit && !dryRun {
		if err := r.confirm(ctx, cmd, podApprovalIdentity(flags)); err != nil {
			return reportApprovalError(cmd, err)
		}

		runtime.waitForController = wait

		return submitPodMigration(ctx, cmd, runtime, object)
	}

	plan, err := runtime.planner.PlanPodMigration(ctx, object, r.global.toolImage)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if err := requireReadyWithOutput(
		runtime,
		plan,
		cmd.ErrOrStderr(),
		podMigrationPlanAdvice(plan.Workload.KubeBlocks),
	); err != nil {
		return err
	}

	if dryRun {
		return printPlanResult(cmd, runtime, plan, podMigrationPlanAdvice(plan.Workload.KubeBlocks))
	}

	if err := r.confirm(ctx, cmd, podApprovalIdentity(flags)); err != nil {
		return reportApprovalError(cmd, err)
	}

	object.Status.Phase = domain.PhasePlanned
	if err := runtime.clusterPodMigrationSessionStore.Create(ctx, object); err != nil {
		return reportPlanningError(cmd, err)
	}

	if err := runtime.clusterPodMigrationSessionExecutor.Run(ctx, object); err != nil {
		return reportPodMigrationError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func reportPodMigrationError(
	cmd *cobra.Command,
	name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Pod migration %s stopped in phase %s. Inspect migrate-pod status %s before resume, abort or cleanup.\n",
		name,
		phase,
		name,
	)

	return errors.Join(cause, err)
}

func podApprovalIdentity(flags *podMigrationFlags) string {
	if flags.podName != "" {
		return flags.podName
	}
	return flags.sessionID
}

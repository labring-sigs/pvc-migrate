package cli

import (
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type offlineMigrationFlags struct {
	sessionID                   string
	sourceNamespace             string
	temporaryNamespace          string
	sourcePVCs                  []string
	destinationPVCs             []string
	destinationCapacities       []string
	sourcePaths                 []string
	destinationPaths            []string
	allowVolumeShrink           bool
	skipSourceUsageCheck        bool
	sourceNode                  string
	targetNode                  string
	destinationClass            string
	capacityAwareness           string
	strategies                  []string
	verifyChecksum              bool
	deleteExtraneous            bool
	sourcePVReclaimPolicy       string
	destinationPVCReclaimPolicy string
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
		"Temporary PVC name before switching the source PVC; for multiple PVCs use source-pvc-name=destination-pvc-name",
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
		&f.sourcePVReclaimPolicy,
		"source-pv-reclaim-policy",
		string(domain.SourcePVReclaimRetain),
		"Policy for the old source PV after migration: Retain or Delete",
	)
	flags.StringVar(
		&f.destinationPVCReclaimPolicy,
		"destination-pvc-reclaim-policy",
		string(domain.DestinationPVCReclaimRetain),
		"Destination storage policy on cleanup, including after rollback: Retain or Delete",
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
		false,
		"Use rsync checksum comparison during final sync",
	)
	flags.BoolVar(
		&f.deleteExtraneous,
		"delete-extraneous",
		true,
		"Delete destination files absent from the source",
	)
}

func (f *offlineMigrationFlags) workflow(
	state *rootState,
	runtime *commandRuntime,
	temporaryExplicit bool,
	submit bool,
) (*v1alpha1.ClusterMigration, error) {
	if err := validateDestinationCapacityFlags(
		domain.OperationMigrate,
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
		domain.SessionTypeMigrate,
		f.sourceNamespace,
		f.sourceNamespace,
		f.temporaryNamespace,
		temporaryExplicit,
		submit,
	)

	object := &v1alpha1.ClusterMigration{
		ObjectMeta: metav1.ObjectMeta{Name: id},
		Spec: v1alpha1.ClusterMigrationSpec{
			SourceNamespace:    v1alpha1.NamespaceName(f.sourceNamespace),
			TemporaryNamespace: v1alpha1.NamespaceName(temporaryNamespace),
			SessionNamespace:   v1alpha1.NamespaceName(sessionNamespace),
			MigrationSpec: v1alpha1.MigrationSpec{
				SourcePVReclaimPolicy: f.sourcePVReclaimPolicy,
				TransferOptions: v1alpha1.TransferOptions{
					DestinationPVCReclaimPolicy: f.destinationPVCReclaimPolicy,
					DestinationStorageClass:     f.destinationClass,
					CapacityAwareness:           f.capacityAwareness,
					SourceNode:                  f.sourceNode,
					TargetNode:                  f.targetNode,
					Strategies:                  append([]string(nil), f.strategies...),
					VerifyChecksum:              f.verifyChecksum,
					DeleteExtraneous:            f.deleteExtraneous,
					AllowVolumeShrink:           f.allowVolumeShrink,
					SkipSourceUsageCheck:        f.skipSourceUsageCheck,
				},
			},
		},
	}
	for _, name := range f.sourcePVCs {
		object.Spec.Volumes = append(object.Spec.Volumes,
			v1alpha1.VolumeRequest{SourcePVC: v1alpha1.LocalResourceReference{Name: name}})
	}

	if err := applyVolumeMappings(&object.Spec.TransferOptions, &object.Spec.Volumes, false,
		f.destinationCapacities, f.destinationPVCs, f.sourcePaths, f.destinationPaths); err != nil {
		return nil, err
	}

	return object, nil
}

func (r *rootState) newMigrateCommand() *cobra.Command {
	flags := &offlineMigrationFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "migrate",
		Short: "Run a complete offline PVC migration in this session",
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

			return r.runOfflineMigrateCommand(cmd, flags, dryRun, false, false)
		},
	}
	flags.bind(command)
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newOfflineMigrationCreateCommand(),
		r.newOfflineMigrationPlanCommand(),
		r.newOfflineMigrationStatusCommand(),
		r.newOfflineMigrationResumeCommand(),
		r.newOfflineMigrationAbortCommand(),
		r.newOfflineMigrationRollbackCommand(),
		r.newOfflineMigrationCleanupCommand(),
	)

	return command
}

// newOfflineMigrationCreateCommand submits a declarative Migration workflow
// for controller reconciliation.
func (r *rootState) newOfflineMigrationCreateCommand() *cobra.Command {
	flags := &offlineMigrationFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a Migration workflow for controller reconciliation",
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

			return r.runOfflineMigrateCommand(cmd, flags, dryRun, true, wait)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

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
				return r.validateMigrationReservation(ctx, cmd, runtime, flags.sessionID)
			}

			object, err := flags.workflow(
				r,
				runtime,
				cmd.Flags().Changed("temporary-namespace"),
				false,
			)
			if err != nil {
				return err
			}

			plan, err := runtime.planner.PlanOfflineMigration(ctx, object, r.global.toolImage)
			if err != nil {
				return reportPlanningError(cmd, err)
			}

			if err := printPlanResult(
				cmd,
				runtime,
				plan,
				offlineMigrationPlanFailureAdvice,
			); err != nil {
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

	// Only controller submission depends on the workflow CRDs being served;
	// session execution is CRD-independent.
	if submit {
		if err := requireControllerWorkflow(runtime, domain.SessionTypeMigrate); err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	if submit && !dryRun {
		if err := r.confirm(ctx, cmd, offlineApprovalIdentity(flags)); err != nil {
			return reportApprovalError(cmd, err)
		}

		runtime.waitForController = wait

		return submitMigration(ctx, cmd, runtime, object)
	}

	plan, err := runtime.planner.PlanOfflineMigration(ctx, object, r.global.toolImage)
	if err != nil {
		return reportPlanningError(cmd, err)
	}

	if err := requireReadyWithOutput(
		runtime,
		plan,
		cmd.ErrOrStderr(),
		offlineMigrationPlanFailureAdvice,
	); err != nil {
		return err
	}

	if dryRun {
		return printPlanResult(cmd, runtime, plan, offlineMigrationPlanFailureAdvice)
	}

	if err := r.confirm(ctx, cmd, offlineApprovalIdentity(flags)); err != nil {
		return reportApprovalError(cmd, err)
	}

	now := metav1.Now()
	object.Status.WorkflowStatus = v1alpha1.WorkflowStatus{
		Phase: domain.PhasePlanned, StartedAt: now, UpdatedAt: now,
	}
	namespace := string(object.Spec.SessionNamespace)

	store, err := cliWorkflowStore(runtime, namespace,
		func() *v1alpha1.ClusterMigration { return &v1alpha1.ClusterMigration{} })
	if err != nil {
		return err
	}

	if err := store.Create(ctx, object); err != nil {
		return reportSessionCreationError(cmd, namespace, object.Name, err)
	}

	executor := app.NewClusterMigrationExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLocker(
			runtime,
		),
		namespace,
		copyengine.NewPVMigrate(),
		r.migrationConfig(runtime),
	)
	if err := executor.Run(ctx, object); err != nil {
		return reportMigrationError(cmd, object.Name, object.Status.Phase, err)
	}

	return runtime.printer.Print(object)
}

func offlineApprovalIdentity(flags *offlineMigrationFlags) string {
	if len(flags.sourcePVCs) > 0 {
		return flags.sourcePVCs[0]
	}
	return flags.sessionID
}

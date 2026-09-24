package cli

import (
	"context"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// crSubmission describes one cr create flow: the workflow object the operator
// described and the submit call that hands the CR to the controller.
type crSubmission struct {
	sessionType domain.SessionType
	object      crclient.Object
	submit      func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error
}

// runCRSubmission drives the shared submission contract: controller-semantics
// planning, CRD availability, a preview that prints exactly the CR that would
// be submitted, explicit approval, and the family's submit call.
func (r *rootState) runCRSubmission(
	cmd *cobra.Command,
	submission crSubmission,
	dryRun, wait *bool,
) error {
	runtime, err := r.runtime()
	if err != nil {
		return err
	}

	if runtime.planner != nil {
		runtime.planner = runtime.planner.ForController()
	}

	if err := requireControllerWorkflow(runtime, submission.sessionType); err != nil {
		return err
	}

	ctx, cancel := r.context(cmd.Context())
	defer cancel()

	if *dryRun {
		if err := runtime.printer.Print(submission.object); err != nil {
			return err
		}

		return writeDryRunApprovalNotice(cmd.ErrOrStderr())
	}

	if err := r.confirm(ctx, cmd, submission.object.GetName()); err != nil {
		return reportApprovalError(cmd, err)
	}

	runtime.waitForController = *wait

	return submission.submit(ctx, cmd, runtime)
}

// checkCRSubmissionIdentity rechecks identity collisions at submission time;
// cached discovery can lag an API server that already accepted a create.
func checkCRSubmissionIdentity(
	ctx context.Context,
	runtime *commandRuntime,
	kind domain.ControllerKind,
	name string,
	namespaces []string,
) error {
	return kube.CheckWorkflowIdentityCollision(
		ctx,
		runtime.clients.Runtime,
		runtime.clients.Kubernetes,
		runtime.controllerKinds,
		name,
		kind,
		namespaces,
		true,
	)
}

func (r *rootState) newCRPodMigrationCreateCommand() *cobra.Command {
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
					"cr migrate-pod create",
					"--pod is required",
				)
			}

			if flags.precopyPasses < 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"cr migrate-pod create",
					"--precopy-passes cannot be negative",
				)
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			object, err := flags.workflow(r, runtime, false, true)
			if err != nil {
				return err
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeMigratePod,
					object:      object,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitPodMigration(ctx, cmd, runtime, object)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	flags.bind(command)
	flags.bindForceReprovision(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

// buildMigrationSpec assembles the offline migration spec shared by the
// namespaced and cluster submission commands.
func (r *rootState) buildMigrationSpec(
	cmd *cobra.Command,
	flags *offlineMigrationFlags,
) (*v1alpha1.ClusterMigration, error) {
	runtime, err := r.runtime()
	if err != nil {
		return nil, err
	}

	return flags.workflow(r, runtime, cmd.Flags().Changed("temporary-namespace"), true)
}

func (r *rootState) newCRMigrationCreateCommand() *cobra.Command {
	flags := &offlineMigrationFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a namespaced Migration workflow for controller reconciliation",
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

			object, err := r.buildMigrationSpec(cmd, flags)
			if err != nil {
				return err
			}

			applyClusterMigrationDefaults(&object.Spec)

			if err := requireSameNamespaceSpec(
				string(object.Spec.SourceNamespace),
				string(object.Spec.DestinationNamespace),
				string(object.Spec.SessionNamespace),
				"cr migrate create",
			); err != nil {
				return err
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeMigrate,
					object:      object,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitMigration(ctx, cmd, runtime, object)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

func (r *rootState) newCRClusterMigrationCreateCommand() *cobra.Command {
	flags := &offlineMigrationFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a ClusterMigration workflow for controller reconciliation",
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

			object, err := r.buildMigrationSpec(cmd, flags)
			if err != nil {
				return err
			}

			spec := object.Spec.DeepCopy()
			applyClusterMigrationDefaults(spec)

			workflow := &v1alpha1.ClusterMigration{
				ObjectMeta: metav1.ObjectMeta{Name: object.Name},
				Spec:       *spec,
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeMigrate,
					object:      workflow,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitClusterMigration(ctx, cmd, runtime, workflow)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

func (r *rootState) newCRCopyCreateCommand() *cobra.Command {
	flags := &copyFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a namespaced Copy workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			submission, object, err := r.buildCopySubmission(cmd, flags)
			if err != nil {
				return err
			}

			if err := requireSameNamespaceSpec(
				string(object.Spec.SourceNamespace),
				string(object.Spec.DestinationNamespace),
				string(object.Spec.SessionNamespace),
				"cr copy create",
			); err != nil {
				return err
			}

			return r.runCRSubmission(cmd, *submission, &dryRun, &wait)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

func (r *rootState) newCRClusterCopyCreateCommand() *cobra.Command {
	flags := &copyFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a ClusterCopy workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, object, err := r.buildCopySubmission(cmd, flags)
			if err != nil {
				return err
			}

			spec := object.Spec.DeepCopy()
			applyClusterCopyDefaults(spec)

			workflow := &v1alpha1.ClusterCopy{
				ObjectMeta: metav1.ObjectMeta{Name: object.Name},
				Spec:       *spec,
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeCopy,
					object:      workflow,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitClusterCopy(ctx, cmd, runtime, workflow)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

// buildCopySubmission assembles the copy workflow the flags describe and the
// identity namespaces its submission must check.
func (r *rootState) buildCopySubmission(
	cmd *cobra.Command,
	flags *copyFlags,
) (*crSubmission, *v1alpha1.ClusterCopy, error) {
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
		return nil, nil, reportPreSessionError(cmd, err)
	}

	if flags.podName != "" && len(flags.sourcePVCs) != 0 {
		return nil, nil, domain.NewError(
			domain.ErrorValidation,
			"copy",
			"--source-pvc cannot be combined with --pod; the Pod PVC set is copied as one unit",
		)
	}

	runtime, err := r.runtime()
	if err != nil {
		return nil, nil, err
	}

	if existing {
		// A reservation session graduates into a submitted Copy through the
		// existing-session handoff; only a re-submitted Copy is refused.
		object, err := flags.workflow(r, runtime, true)
		if err != nil {
			return nil, nil, err
		}

		return &crSubmission{
			sessionType: domain.SessionTypeCopy,
			object:      object,
			submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
				return r.copyExisting(ctx, cmd, runtime, flags, false, true)
			},
		}, object, nil
	}

	object, err := flags.workflow(r, runtime, true)
	if err != nil {
		return nil, nil, err
	}

	return &crSubmission{
		sessionType: domain.SessionTypeCopy,
		object:      object,
		submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
			return submitCopy(ctx, cmd, runtime, object)
		},
	}, object, nil
}

func (r *rootState) newCRReserveCreateCommand() *cobra.Command {
	flags := &reserveFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a namespaced Reservation workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			submission, object, err := r.buildReserveSubmission(cmd, flags)
			if err != nil {
				return err
			}

			if err := requireSameNamespaceSpec(
				string(object.Spec.SourceNamespace),
				string(object.Spec.DestinationNamespace),
				string(object.Spec.SessionNamespace),
				"cr reserve create",
			); err != nil {
				return err
			}

			return r.runCRSubmission(cmd, *submission, &dryRun, &wait)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

func (r *rootState) newCRClusterReserveCreateCommand() *cobra.Command {
	flags := &reserveFlags{}

	var (
		dryRun bool
		wait   bool
	)

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a ClusterReservation workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, object, err := r.buildReserveSubmission(cmd, flags)
			if err != nil {
				return err
			}

			spec := object.Spec.DeepCopy()
			applyClusterReservationDefaults(spec)

			workflow := &v1alpha1.ClusterReservation{
				ObjectMeta: metav1.ObjectMeta{Name: object.Name},
				Spec:       *spec,
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeReserve,
					object:      workflow,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitClusterReservation(ctx, cmd, runtime, workflow)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	flags.bind(command)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

// buildReserveSubmission assembles the reservation workflow the flags
// describe. Existing sessions cannot be re-submitted: the controller
// reconciles submitted workflows automatically.
func (r *rootState) buildReserveSubmission(
	cmd *cobra.Command,
	flags *reserveFlags,
) (*crSubmission, *v1alpha1.ClusterReservation, error) {
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
		return nil, nil, reportPreSessionError(cmd, err)
	}

	if existing {
		return nil, nil, domain.NewError(
			domain.ErrorValidation,
			"cr reserve create",
			"an existing session cannot be re-submitted; continue it with the session lifecycle commands",
		)
	}

	runtime, err := r.runtime()
	if err != nil {
		return nil, nil, err
	}

	object, err := flags.workflow(r, runtime, true)
	if err != nil {
		return nil, nil, err
	}

	return &crSubmission{
		sessionType: domain.SessionTypeReserve,
		object:      object,
		submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
			return submitReservation(ctx, cmd, runtime, object)
		},
	}, object, nil
}

func (r *rootState) newCRRenameCreateCommand() *cobra.Command {
	object := &v1alpha1.Rename{}
	dryRun := false
	wait := true

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a Rename workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if object.Spec.SourcePVC.Name == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"cr rename create",
					"--source-pvc is required",
				)
			}

			if object.Spec.DestinationPVC.Name == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"cr rename create",
					"--destination-pvc is required",
				)
			}

			current := object.DeepCopy()
			if current.Name == "" {
				id, err := domain.NewSessionID(time.Now())
				if err != nil {
					return err
				}

				current.Name = id
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if err := checkCRSubmissionIdentity(
				ctx,
				runtime,
				domain.ControllerKindRename,
				current.Name,
				[]string{current.Namespace},
			); err != nil {
				return err
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeRename,
					object:      current,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitRename(ctx, cmd, runtime, current)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVarP(
		&object.Namespace,
		"namespace",
		"n",
		"default",
		"Source and destination PVC namespace",
	)
	f.StringVar(&object.Spec.SourcePVC.Name, "source-pvc", "", "Existing offline PVC name")
	f.StringVar(
		&object.Spec.DestinationPVC.Name,
		"destination-pvc",
		"",
		"New PVC name in the source namespace",
	)

	return command
}

func (r *rootState) newCRMoveCreateCommand() *cobra.Command {
	object := &v1alpha1.Move{
		Spec: v1alpha1.MoveSpec{DestinationPVC: &v1alpha1.LocalResourceReference{}},
	}

	var (
		sourceNamespace      string
		destinationNamespace string
	)

	dryRun := false
	wait := true

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a Move workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			object.Spec.SourceNamespace = v1alpha1.NamespaceName(sourceNamespace)
			object.Spec.DestinationNamespace = v1alpha1.NamespaceName(destinationNamespace)

			if object.Spec.SourcePVC.Name == "" || object.Spec.DestinationNamespace == "" {
				return domain.NewError(
					domain.ErrorValidation,
					"cr move create",
					"--source-pvc and --destination-namespace are required",
				)
			}

			current := object.DeepCopy()

			current.Spec.SessionNamespace = v1alpha1.NamespaceName(r.global.sessionNamespace)
			if current.Spec.DestinationPVC.Name == "" {
				current.Spec.DestinationPVC = nil
			}

			if current.Name == "" {
				id, err := domain.NewSessionID(time.Now())
				if err != nil {
					return err
				}

				current.Name = id
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if err := checkCRSubmissionIdentity(
				ctx,
				runtime,
				domain.ControllerKindMove,
				current.Name,
				[]string{
					string(current.Spec.SourceNamespace),
					string(current.Spec.DestinationNamespace),
					r.global.sessionNamespace,
				},
			); err != nil {
				return err
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeMove,
					object:      current,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return submitMove(ctx, cmd, runtime, current)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVar(
		&sourceNamespace,
		"source-namespace",
		"default",
		"Source PVC namespace",
	)
	f.StringVar(
		&destinationNamespace,
		"destination-namespace",
		"",
		"Destination namespace for the PVC identity",
	)
	f.StringVar(&object.Spec.SourcePVC.Name, "source-pvc", "", "Existing offline PVC name")
	f.StringVar(
		&object.Spec.DestinationPVC.Name,
		"destination-pvc",
		"",
		"New PVC name in the destination namespace; defaults to the source name",
	)

	return command
}

func (r *rootState) newCRBackupCreateCommand() *cobra.Command {
	object := &v1alpha1.Backup{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Backup"},
	}
	flags := &s3RepositoryFlags{}
	dryRun := false
	wait := true

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a Backup workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := r.validateBackupInput(cmd, object, flags, true); err != nil {
				return err
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeBackup,
					object:      object,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return r.submitBackup(ctx, cmd, runtime, object)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVarP(
		&object.Namespace,
		"namespace",
		"n",
		"default",
		"Source PVC and repository namespace",
	)
	f.StringVar(&object.Spec.SourcePVC.Name, "source-pvc", "", "Source PVC name")
	f.StringVar(&object.Spec.Name, "name", "", "Backup recovery point name")
	f.StringVar(&object.Spec.Path, "path", "", "PVC subdirectory")
	f.BoolVar(
		&object.Spec.Online,
		"online",
		false,
		"Copy from an active source without pausing consumers",
	)
	f.BoolVar(
		&object.Spec.OpenEBSLVMEnableShared,
		"openebs-lvm-enable-shared",
		false,
		"Temporarily enable shared mounts for online OpenEBS LVM backup",
	)
	bindRepositoryFlags(command, flags, &object.Spec.RepositoryRef.Name, true)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

func (r *rootState) newCRRestoreCreateCommand() *cobra.Command {
	object := &v1alpha1.Restore{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Restore"},
	}
	flags := &s3RepositoryFlags{}
	dryRun := false
	wait := true

	command := &cobra.Command{
		Use:   "create",
		Short: "Submit a Restore workflow for controller reconciliation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := r.validateRestoreInput(cmd, object, flags, true); err != nil {
				return err
			}

			return r.runCRSubmission(
				cmd,
				crSubmission{
					sessionType: domain.SessionTypeRestore,
					object:      object,
					submit: func(ctx context.Context, cmd *cobra.Command, runtime *commandRuntime) error {
						return r.submitRestore(ctx, cmd, runtime, object)
					},
				},
				&dryRun,
				&wait,
			)
		},
	}
	f := command.Flags()
	f.StringVar(&object.Name, "id", "", "Workflow ID; generated when omitted")
	f.StringVarP(
		&object.Namespace,
		"namespace",
		"n",
		"default",
		"Destination PVC and repository namespace",
	)
	f.StringVar(&object.Spec.DestinationPVC.Name, "destination-pvc", "", "Destination PVC name")
	f.StringVar(&object.Spec.Name, "name", "", "Backup recovery point name")
	f.StringVar(&object.Spec.Path, "path", "", "PVC subdirectory")
	f.BoolVar(&object.Spec.CreatePVC, "create-pvc", false, "Create the destination PVC")
	f.StringVar(
		&object.Spec.DestinationStorageClass,
		"destination-storage-class",
		"",
		"StorageClass for a created PVC",
	)
	f.StringVar(
		&object.Spec.DestinationAccessMode,
		"destination-access-mode",
		"ReadWriteOnce",
		"Access mode for a created PVC",
	)
	f.StringVar(
		&object.Spec.DestinationCapacity,
		"destination-capacity",
		"",
		"Capacity for a created PVC; defaults to backup capacity",
	)
	f.StringVar(
		&object.Spec.TargetNode,
		"target-node",
		"",
		"Node for restore tool scheduling and PVC binding",
	)
	f.BoolVar(
		&object.Spec.AllowMounted,
		"allow-mounted",
		false,
		"Allow restore while the PVC has consumers",
	)
	f.BoolVar(
		&object.Spec.DeleteExtraneous,
		"delete-extraneous",
		false,
		"Delete destination files absent from the recovery point",
	)
	bindRepositoryFlags(command, flags, &object.Spec.RepositoryRef.Name, true)
	bindCreateDryRun(command, &dryRun)
	bindCreateWait(command, &wait)

	return command
}

// requireSameNamespaceSpec rejects cross-namespace specs on the namespaced cr
// create commands: those submissions belong to the cluster-scoped variants.
func requireSameNamespaceSpec(source, destination, session, command string) error {
	if source != destination || destination != session {
		return domain.NewError(
			domain.ErrorValidation,
			command,
			"cross-namespace workflows are cluster-scoped; submit them with "+clusterCreateHint(
				command,
			),
		)
	}

	return nil
}

func clusterCreateHint(command string) string {
	switch command {
	case "cr copy create":
		return "cr cluster-copy create"
	case "cr reserve create":
		return "cr cluster-reserve create"
	default:
		return "cr cluster-migrate create"
	}
}

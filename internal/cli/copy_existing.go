package cli

import (
	"context"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) loadCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
) (crclient.Object, error) {
	object, _, err := r.loadCopyWithBackend(ctx, cmd, runtime, id, false, source)
	return object, err
}

// loadCopyWithBackend resolves one copy/reservation identity from the single
// backend its command family addresses — ConfigMap session records for the
// session commands, workflow CRs for the cr commands. The backend tells the
// caller which stores and handoff callbacks to bind: controller-owned
// reservations hand off through the CRD store and the elected controller
// executes the resulting Copy.
func (r *rootState) loadCopyWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	allowReservation bool,
	source workflowSource,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"copy",
			"Kubernetes clients are required",
		)
	}

	namespace := r.workflowStorageNamespace(cmd)

	candidates := map[domain.ControllerKind]crclient.Object{
		domain.ControllerKindCopy:        &v1alpha1.Copy{},
		domain.ControllerKindClusterCopy: &v1alpha1.ClusterCopy{},
	}
	if allowReservation {
		candidates[domain.ControllerKindReservation] = &v1alpha1.Reservation{}
		candidates[domain.ControllerKindClusterReservation] = &v1alpha1.ClusterReservation{}
	}

	switch source {
	case sourceController:
		delete(candidates, domain.ControllerKindClusterCopy)
		delete(candidates, domain.ControllerKindClusterReservation)
	case sourceClusterController:
		delete(candidates, domain.ControllerKindCopy)
		delete(candidates, domain.ControllerKindReservation)
	}

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		id,
		candidates,
		source,
	)
	if err != nil {
		return nil, "", reportSessionLookupError(cmd, namespace, id, err)
	}

	switch object.(type) {
	case *v1alpha1.Copy, *v1alpha1.ClusterCopy:
		return object, backend, nil
	case *v1alpha1.Reservation, *v1alpha1.ClusterReservation:
		if allowReservation {
			return object, backend, nil
		}
	}

	return nil, "", domain.NewError(domain.ErrorValidation, "copy", "stored workflow is not a copy")
}

// applyCopyOverrides changes only flags explicitly supplied at this entrypoint.
// Storage selection belongs to the original plan and cannot be retargeted here.
func applyCopyOverrides(cmd *cobra.Command, spec *v1alpha1.CopySpec, flags *copyFlags) error {
	for _, name := range []string{"destination-namespace", "destination-pvc", "destination-capacity", "source-path", "destination-path", "allow-volume-shrink", "skip-source-usage-check", "target-node", "destination-storage-class", "capacity-awareness"} {
		if cmd.Flags().Changed(name) {
			return domain.NewError(
				domain.ErrorPrecondition,
				"copy",
				fmt.Sprintf(
					"--%s cannot change persisted storage selection; create a new workflow",
					name,
				),
			)
		}
	}

	if cmd.Flags().Changed("online") {
		spec.Online = flags.online
	}

	if cmd.Flags().Changed("source-node") {
		spec.SourceNode = flags.sourceNode
	}

	if cmd.Flags().Changed("strategy") {
		spec.Strategies = slices.Clone(flags.strategies)
	}

	if cmd.Flags().Changed("verify-checksum") {
		spec.VerifyChecksum = flags.verifyChecksum
	}

	if cmd.Flags().Changed("delete-extraneous") {
		spec.DeleteExtraneous = new(flags.deleteExtraneous)
	}

	if cmd.Flags().Changed("unused-storage-policy") {
		spec.UnusedStoragePolicy = v1alpha1.UnusedStoragePolicy(
			flags.unusedStoragePolicy,
		)
	}

	return domain.ValidateUnusedStoragePolicy(spec.UnusedStoragePolicy)
}

func validateCopyOverrides(cmd *cobra.Command, spec v1alpha1.CopySpec, flags *copyFlags) error {
	candidate := spec.DeepCopy()
	if err := applyCopyOverrides(cmd, candidate, flags); err != nil {
		return err
	}

	if !apiequality.Semantic.DeepEqual(spec, *candidate) {
		return domain.NewError(
			domain.ErrorPrecondition,
			"copy",
			"persisted copy input is immutable; create a new workflow to change its spec",
		)
	}

	return nil
}

// copyExisting drives an already-persisted workflow: the session copy
// commands adopt or resume ConfigMap session records (submit=false), while
// cr create graduates a reservation into an executed copy (submit=true); a
// submitted Copy is never re-submitted. cr create addresses a controller-owned
// Reservation CR in the source namespace first and falls back to the
// ConfigMap session — session commands only ever touch the session record.
func (r *rootState) copyExisting(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	flags *copyFlags,
	dryRun bool,
	submit bool,
) error {
	if submit && flags.sourceNamespace != "" {
		crdObject, crdErr := lookupControllerObjects(
			ctx,
			runtime,
			flags.sourceNamespace,
			flags.sessionID,
			map[domain.ControllerKind]crclient.Object{
				domain.ControllerKindReservation:        &v1alpha1.Reservation{},
				domain.ControllerKindClusterReservation: &v1alpha1.ClusterReservation{},
			},
		)
		switch {
		case crdErr == nil:
			switch current := crdObject.(type) {
			case *v1alpha1.Reservation:
				return r.adoptReservation(ctx, cmd, runtime, current, flags, dryRun, backendCRD)
			case *v1alpha1.ClusterReservation:
				return r.adoptClusterReservation(
					ctx,
					cmd,
					runtime,
					current,
					flags,
					dryRun,
					backendCRD,
				)
			}
		case apierrors.IsNotFound(crdErr):
			// A cr submission graduates controller-owned reservations only.
			// Falling through to ConfigMap session records would execute the
			// copy in-process under the cr command tree, mixing the two
			// execution modes the command groups keep apart.
			return domain.NewError(
				domain.ErrorValidation,
				"cr copy create",
				fmt.Sprintf(
					"no Reservation or ClusterReservation %s exists in namespace %s; a ConfigMap session record is graduated by the session copy command",
					flags.sessionID,
					flags.sourceNamespace,
				),
			)
		default:
			return crdErr
		}
	}

	object, backend, err := r.loadCopyWithBackend(
		ctx,
		cmd,
		runtime,
		flags.sessionID,
		true,
		sourceSession,
	)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Copy:
		if submit {
			return domain.NewError(
				domain.ErrorValidation,
				"copy create",
				"an existing session cannot be re-submitted; the controller reconciles submitted workflows automatically",
			)
		}

		if err := validateCopyOverrides(cmd, current.Spec, flags); err != nil {
			return err
		}

		return r.executeCopy(ctx, cmd, runtime, current, dryRun, true, backend)
	case *v1alpha1.ClusterCopy:
		if submit {
			return domain.NewError(
				domain.ErrorValidation,
				"copy create",
				"an existing session cannot be re-submitted; the controller reconciles submitted workflows automatically",
			)
		}

		if err := validateCopyOverrides(cmd, current.Spec.CopySpec, flags); err != nil {
			return err
		}

		return r.executeClusterCopy(ctx, cmd, runtime, current, dryRun, true, backend)
	case *v1alpha1.Reservation:
		return r.adoptReservation(ctx, cmd, runtime, current, flags, dryRun, backend)
	case *v1alpha1.ClusterReservation:
		return r.adoptClusterReservation(ctx, cmd, runtime, current, flags, dryRun, backend)
	default:
		return domain.NewError(domain.ErrorValidation, "copy", "unsupported copy input")
	}
}

func (r *rootState) resumeCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	dryRun bool,
	source workflowSource,
) error {
	object, backend, err := r.loadCopyWithBackend(ctx, cmd, runtime, id, false, source)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Copy:
		return r.executeCopy(ctx, cmd, runtime, current, dryRun, false, backend)
	case *v1alpha1.ClusterCopy:
		return r.executeClusterCopy(ctx, cmd, runtime, current, dryRun, false, backend)
	default:
		return domain.NewError(domain.ErrorValidation, "copy", "stored workflow is not a copy")
	}
}

func (r *rootState) executeCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.Copy,
	dryRun, repeat bool, backend string,
) error {
	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		r.workflowStorageNamespace(cmd),
		func() *v1alpha1.Copy { return &v1alpha1.Copy{} },
	)
	if err != nil {
		return err
	}

	executor := app.NewCopyExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)
	if dryRun {
		if err := executor.Validate(ctx, object); err != nil {
			return reportCopyError(cmd, object.Name, object.Status.Phase, err)
		}

		namespace := workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)

		if !repeat {
			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"copy",
					"resume",
					namespace,
					object.Name,
				),
			)
		}

		return printCopyDryRunResult(cmd, runtime, object, namespace)
	}

	if !repeat && requiresResumeApproval(copyResumePhase(object.Status.WorkflowStatus)) {
		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}
	}

	if repeat && object.Status.Phase == domain.PhaseWarmCopied {
		err = executor.RequestCopyPass(ctx, object)
	} else {
		err = executor.RequestResume(ctx, object)
	}

	if err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(
			cmd,
			workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
		).pvcMigrate,
		"copy",
		workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object),
		object.Name,
		object.Status.Phase,
		false,
	)
}

func (r *rootState) executeClusterCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	object *v1alpha1.ClusterCopy,
	dryRun, repeat bool, backend string,
) error {
	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		r.workflowStorageNamespace(cmd),
		func() *v1alpha1.ClusterCopy { return &v1alpha1.ClusterCopy{} },
	)
	if err != nil {
		return err
	}

	namespace := string(object.Spec.SessionNamespace)
	if namespace == "" {
		namespace = string(object.Spec.SourceNamespace)
	}

	executor := app.NewClusterCopyExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		namespace,
		copyengine.NewPVMigrate(),
		r.copyConfig(runtime),
	)
	if dryRun {
		if err := executor.Validate(ctx, object); err != nil {
			return reportCopyError(cmd, object.Name, object.Status.Phase, err)
		}

		if !repeat {
			if err := runtime.printer.Print(object); err != nil {
				return err
			}

			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
					"copy",
					"resume",
					namespace,
					object.Name,
				),
			)
		}

		return printCopyDryRunResult(cmd, runtime, object, namespace)
	}

	if !repeat && requiresResumeApproval(copyResumePhase(object.Status.WorkflowStatus)) {
		if err := r.confirm(ctx, cmd, object.Name); err != nil {
			return reportApprovalError(cmd, err)
		}
	}

	if repeat && object.Status.Phase == domain.PhaseWarmCopied {
		err = executor.RequestCopyPass(ctx, object)
	} else {
		err = executor.RequestResume(ctx, object)
	}

	if err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, object.Name, object.Status.Phase, err)
	}

	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
		"copy",
		namespace,
		object.Name,
		object.Status.Phase,
		false,
	)
}

func copyResumePhase(status v1alpha1.WorkflowStatus) v1alpha1.WorkflowPhase {
	if status.Phase == domain.PhaseFailed {
		return status.ResumeFrom
	}
	return status.Phase
}

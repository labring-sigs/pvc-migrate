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

// recordScope selects which API scope — namespaced or cluster-scoped — one
// command family addresses. The split copy and reserve session families carry
// their scope structurally, so a family can never resolve the other scope's
// records; cr commands derive the scope from their workflowSource.
type recordScope int

const (
	namespacedRecords recordScope = iota
	clusterRecords
)

// recordScopeForSource maps a cr command's workflowSource onto the record
// scope its family addresses.
func recordScopeForSource(source workflowSource) recordScope {
	if source == sourceClusterController {
		return clusterRecords
	}

	return namespacedRecords
}

// sessionFamilyCommand renders the root command name of a split session
// family: the namespaced family keeps the plain verb, the cluster family
// carries the cluster- prefix. workflowCommandPath adds the cr segment for
// controller commands, so cr families pass the same value.
func sessionFamilyCommand(scope recordScope, family string) string {
	if scope == clusterRecords {
		return "cluster-" + family
	}

	return family
}

func (r *rootState) loadCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
	scope recordScope,
) (crclient.Object, error) {
	object, _, err := r.loadCopyWithBackend(ctx, cmd, runtime, id, false, source, scope)
	return object, err
}

// loadCopyWithBackend resolves one copy/reservation identity from the single
// backend its command family addresses — ConfigMap session records for the
// session commands, workflow CRs for the cr commands — restricted to the one
// record scope the family owns. The backend tells the caller which stores and
// handoff callbacks to bind: controller-owned reservations hand off through
// the CRD store and the elected controller executes the resulting Copy.
func (r *rootState) loadCopyWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	allowReservation bool,
	source workflowSource,
	scope recordScope,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"copy",
			"Kubernetes clients are required",
		)
	}

	// The scope owns both lookups: the CRD probe set for the cr commands, and
	// — because a ConfigMap record decodes by its stored kind — the acceptance
	// check below, so a family can never resolve the other scope's record.
	candidates := map[domain.ControllerKind]crclient.Object{}
	if scope == namespacedRecords {
		candidates[domain.ControllerKindCopy] = &v1alpha1.Copy{}
		if allowReservation {
			candidates[domain.ControllerKindReservation] = &v1alpha1.Reservation{}
		}
	} else {
		candidates[domain.ControllerKindClusterCopy] = &v1alpha1.ClusterCopy{}
		if allowReservation {
			candidates[domain.ControllerKindClusterReservation] = &v1alpha1.ClusterReservation{}
		}
	}

	object, backend, err := r.loadCopyRecordWithBackend(ctx, cmd, runtime, id, candidates, source)
	if err != nil {
		return nil, "", err
	}

	accepted := false
	switch object.(type) {
	case *v1alpha1.Copy:
		accepted = scope == namespacedRecords
	case *v1alpha1.ClusterCopy:
		accepted = scope == clusterRecords
	case *v1alpha1.Reservation:
		accepted = scope == namespacedRecords && allowReservation
	case *v1alpha1.ClusterReservation:
		accepted = scope == clusterRecords && allowReservation
	}

	if !accepted {
		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"copy",
			"stored workflow is not a copy",
		)
	}

	return object, backend, nil
}

// loadCopyRecordWithBackend resolves one copy/reservation identity against an
// explicit candidate kind set and the backend its source addresses.
func (r *rootState) loadCopyRecordWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	candidates map[domain.ControllerKind]crclient.Object,
	source workflowSource,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"copy",
			"Kubernetes clients are required",
		)
	}

	// Session records persist in the session storage namespace whatever
	// tenant namespace their object carries; cr commands address workflow CRs
	// through their own -n instead.
	namespace := r.workflowStorageNamespace(cmd)
	if source == sourceSession {
		namespace = r.migrationRecordNamespace()
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

	return object, backend, nil
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

// copyExisting drives the cr create graduation: cr copy create and cr
// cluster-copy create graduate a controller-owned Reservation CR (namespaced or
// cluster-scoped) in the source namespace into the Copy the elected controller
// executes; a submitted Copy is never re-submitted. cr create addresses the
// CRD first and never falls back to ConfigMap session records — the session
// families own those through copySessionExisting.
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

	// Only a cr submission that skipped the CRD lookup (an explicitly empty
	// source namespace) reaches the session records below; the candidate set
	// keeps today's both-scope shape for that defensive path.
	object, backend, err := r.loadCopyRecordWithBackend(
		ctx,
		cmd,
		runtime,
		flags.sessionID,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindCopy:               &v1alpha1.Copy{},
			domain.ControllerKindClusterCopy:        &v1alpha1.ClusterCopy{},
			domain.ControllerKindReservation:        &v1alpha1.Reservation{},
			domain.ControllerKindClusterReservation: &v1alpha1.ClusterReservation{},
		},
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

// copySessionExisting drives an already-persisted ConfigMap session record for
// one copy family: the copy command adopts or resumes namespaced records
// (Copy and Reservation), the cluster-copy command cluster-scoped ones
// (ClusterCopy and ClusterReservation). Controller-owned CR graduation belongs
// to the cr tree and is unreachable here.
func (r *rootState) copySessionExisting(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	flags *copyFlags,
	dryRun bool,
	scope recordScope,
) error {
	object, backend, err := r.loadCopyWithBackend(
		ctx,
		cmd,
		runtime,
		flags.sessionID,
		true,
		sourceSession,
		scope,
	)
	if err != nil {
		return err
	}

	switch current := object.(type) {
	case *v1alpha1.Copy:
		if err := validateCopyOverrides(cmd, current.Spec, flags); err != nil {
			return err
		}

		return r.executeCopy(ctx, cmd, runtime, current, dryRun, true, backend)
	case *v1alpha1.ClusterCopy:
		if err := validateCopyOverrides(cmd, current.Spec.CopySpec, flags); err != nil {
			return err
		}

		return r.executeClusterCopy(ctx, cmd, runtime, current, dryRun, true, backend)
	case *v1alpha1.Reservation:
		return r.adoptReservation(ctx, cmd, runtime, current, flags, dryRun, backend)
	case *v1alpha1.ClusterReservation:
		return r.adoptClusterReservation(ctx, cmd, runtime, current, flags, dryRun, backend)
	default:
		return domain.NewError(
			domain.ErrorValidation,
			sessionFamilyCommand(scope, "copy"),
			"stored workflow is not a copy session record",
		)
	}
}

func (r *rootState) resumeCopy(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	dryRun bool,
	source workflowSource,
	scope recordScope,
) error {
	object, backend, err := r.loadCopyWithBackend(ctx, cmd, runtime, id, false, source, scope)
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
	// Session records persist in the session storage namespace whatever tenant
	// namespace their object carries; a CRD record ignores the store namespace
	// entirely, so one resolution serves both backends.
	store, err := cliWorkflowStoreForBackend(
		runtime,
		backend,
		r.migrationRecordNamespace(),
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
			return reportCopyError(cmd, "copy", object.Name, object.Status.Phase, err)
		}

		namespace := workflowLeaseNamespace(backend, r.migrationRecordNamespace(), object)

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
		return reportCopyError(cmd, "copy", object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, "copy", object.Name, object.Status.Phase, err)
	}

	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(
			cmd,
			workflowLeaseNamespace(backend, r.migrationRecordNamespace(), object),
		).pvcMigrate,
		"copy",
		workflowLeaseNamespace(backend, r.migrationRecordNamespace(), object),
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
			return reportCopyError(cmd, "cluster-copy", object.Name, object.Status.Phase, err)
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
					"cluster-copy",
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
		return reportCopyError(cmd, "cluster-copy", object.Name, object.Status.Phase, err)
	}

	if err := executor.Run(ctx, object); err != nil {
		return reportCopyError(cmd, "cluster-copy", object.Name, object.Status.Phase, err)
	}

	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	return writeWorkflowNextSteps(
		cmd.ErrOrStderr(),
		cmd,
		guidancePrefixesForCommand(cmd, namespace).pvcMigrate,
		"cluster-copy",
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

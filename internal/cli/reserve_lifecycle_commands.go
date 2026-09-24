package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *rootState) addReserveLifecycle(parent *cobra.Command) {
	parent.AddCommand(
		r.newReserveStatusCommand(sourceSession),
		r.newReserveResumeCommand(sourceSession),
		r.newReserveAbortCommand(sourceSession),
		r.newReserveCleanupCommand(sourceSession),
	)
}

func (r *rootState) loadReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
) (crclient.Object, error) {
	object, _, err := r.loadReservationWithBackend(ctx, cmd, runtime, id, source)
	return object, err
}

func (r *rootState) loadReservationWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
	source workflowSource,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"reserve",
			"Kubernetes clients are required",
		)
	}

	namespace := r.workflowStorageNamespace(cmd)

	candidates := map[domain.ControllerKind]crclient.Object{
		domain.ControllerKindReservation:        &v1alpha1.Reservation{},
		domain.ControllerKindClusterReservation: &v1alpha1.ClusterReservation{},
	}
	switch source {
	case sourceController:
		delete(candidates, domain.ControllerKindClusterReservation)
	case sourceClusterController:
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
	case *v1alpha1.Reservation, *v1alpha1.ClusterReservation:
		return object, backend, nil
	default:
		return nil, "", domain.NewError(
			domain.ErrorValidation,
			"reserve",
			"stored workflow is not a reservation",
		)
	}
}

func (r *rootState) newReserveStatusCommand(source workflowSource) *cobra.Command {
	return &cobra.Command{
		Use:   "status [" + workflowArgLabel(source) + "]",
		Short: "Show one reservation or list reservations",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if len(args) == 1 {
				object, err := r.loadReservation(ctx, cmd, runtime, args[0], source)
				if err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writeWorkflowNextSteps(
					cmd.ErrOrStderr(),
					cmd,
					guidancePrefixesForCommand(
						cmd,
						workflowHintNamespace("", r, cmd, object),
					).pvcMigrate,
					"reserve",
					workflowHintNamespace("", r, cmd, object),
					object.GetName(),
					workflowObjectPhase(object),
					false,
				)
			}

			objects := []crclient.Object{}

			if source != sourceSession {
				if !crdListable(runtime) {
					return runtime.printer.Print(objects)
				}

				kind := domain.ControllerKindReservation
				if source == sourceClusterController {
					kind = domain.ControllerKindClusterReservation
				}

				if len(runtime.controllerKinds) != 0 &&
					!slices.Contains(runtime.controllerKinds, kind) {
					return runtime.printer.Print(objects)
				}

				items, err := listControllerWorkflows(ctx, runtime, kind)
				if err != nil {
					return err
				}

				objects = append(objects, items...)

				return runtime.printer.Print(objects)
			}

			namespace := r.workflowStorageNamespace(cmd)

			if len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindReservation) {
				store, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
				)
				if err != nil {
					return err
				}

				items, err := store.List(ctx, namespace)
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			if len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindClusterReservation) {
				store, err := cliWorkflowStore(
					runtime,
					namespace,
					func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
				)
				if err != nil {
					return err
				}

				items, err := store.List(ctx, "")
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			return runtime.printer.Print(objects)
		},
	}
}

func (r *rootState) newReserveResumeCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume " + workflowArgLabel(source),
		Short: "Continue a reservation from its persisted checkpoint",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		return r.reserveExisting(ctx, cmd, runtime, args[0], dryRun, source)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newReserveAbortCommand(source workflowSource) *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort " + workflowArgLabel(source),
		Short: "Abort a reservation",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, backend, err := r.loadReservationWithBackend(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		if !dryRun {
			if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
				return reportApprovalError(cmd, err)
			}
		}

		switch current := object.(type) {
		case *v1alpha1.Reservation:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
			)
			if err != nil {
				return err
			}

			executor := app.NewReservationExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				r.reservationConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateAbort(current)
			} else {
				err = executor.Abort(ctx, current)
			}

			if err != nil {
				return reportReservationError(cmd, current.Name, current.Status.Phase, err)
			}
		case *v1alpha1.ClusterReservation:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
			)
			if err != nil {
				return err
			}

			namespace := string(current.Spec.SessionNamespace)
			if namespace == "" {
				namespace = string(current.Spec.SourceNamespace)
			}

			executor := app.NewClusterReservationExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				namespace,
				r.reservationConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateAbort(current)
			} else {
				err = executor.Abort(ctx, current)
			}

			if err != nil {
				return reportReservationError(cmd, current.Name, current.Status.Phase, err)
			}
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		namespace := workflowHintNamespace(backend, r, cmd, object)

		if dryRun {
			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					cmd,
					guidancePrefixesForCommand(
						cmd,
						namespace,
					).pvcMigrate,
					"reserve",
					"abort",
					namespace,
					object.GetName(),
				),
			)
		}

		return writeWorkflowNextSteps(
			cmd.ErrOrStderr(),
			cmd,
			guidancePrefixesForCommand(
				cmd,
				namespace,
			).pvcMigrate,
			"reserve",
			namespace,
			object.GetName(),
			workflowObjectPhase(object),
			false,
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newReserveCleanupCommand(source workflowSource) *cobra.Command {
	var (
		options app.ReservationCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup " + workflowArgLabel(source),
		Short: "Finalize reservation storage and clean up its workflow",
		Args:  cobra.ExactArgs(1),
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		runtime, err := r.runtime()
		if err != nil {
			return err
		}

		ctx, cancel := r.context(cmd.Context())
		defer cancel()

		object, backend, err := r.loadReservationWithBackend(ctx, cmd, runtime, args[0], source)
		if err != nil {
			return err
		}

		if !dryRun {
			if err := r.confirm(ctx, cmd, object.GetName()); err != nil {
				return reportApprovalError(cmd, err)
			}
		}

		switch current := object.(type) {
		case *v1alpha1.Reservation:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
			)
			if err != nil {
				return err
			}

			executor := app.NewReservationExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				r.reservationConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateCleanup(ctx, current, options)
			} else {
				err = executor.Cleanup(ctx, current, options)
			}

			if err != nil {
				return reportReservationCleanupError(
					cmd,
					workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), current),
					current.Name,
					options,
					err,
				)
			}
		case *v1alpha1.ClusterReservation:
			store, err := cliWorkflowStoreForBackend(
				runtime,
				backend,
				r.workflowStorageNamespace(cmd),
				func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
			)
			if err != nil {
				return err
			}

			namespace := string(current.Spec.SessionNamespace)
			if namespace == "" {
				namespace = string(current.Spec.SourceNamespace)
			}

			executor := app.NewClusterReservationExecutor(
				runtime.clients.Kubernetes,
				store,
				cliWorkflowLockerForBackend(runtime, backend),
				namespace,
				r.reservationConfig(runtime),
			)
			if dryRun {
				err = executor.ValidateCleanup(ctx, current, options)
			} else {
				err = executor.Cleanup(ctx, current, options)
			}

			if err != nil {
				return reportReservationCleanupError(
					cmd,
					namespace,
					current.Name,
					options,
					err,
				)
			}
		}

		if options.DeleteSession && !dryRun {
			_, err := fmt.Fprintf(
				cmd.OutOrStdout(),
				"Deleted reservation workflow %s.\n",
				object.GetName(),
			)

			return err
		}

		if err := runtime.printer.Print(object); err != nil {
			return err
		}

		if dryRun {
			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				cleanupExecuteCommand(
					cmd,
					guidancePrefixesForCommand(
						cmd,
						workflowHintNamespace(backend, r, cmd, object),
					).pvcMigrate,
					"reserve",
					workflowHintNamespace(backend, r, cmd, object),
					object.GetName(),
					options.UnusedStoragePolicy,
					options.Finalize,
					options.DeleteSession,
				),
			)
		}

		return nil
	}
	command.Flags().
		StringVar(&options.UnusedStoragePolicy, "unused-storage-policy", "", "Keep or Delete reserved storage; defaults to the recorded policy. Delete removes destination PVCs this reservation created and never promoted to a copy; the source is always kept")
	command.Flags().
		BoolVar(&options.Finalize, "finalize", false, "Release ownership of retained storage and close the recovery window")
	command.Flags().
		BoolVar(&options.DeleteSession, "delete-session", false, "Delete the workflow record after cleanup")
	bindDryRun(command, &dryRun)

	return command
}

func reportReservationError(
	cmd *cobra.Command,
	name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Reservation %s stopped in phase %s. Inspect reserve status %s before resume or cleanup.\n",
		name,
		phase,
		name,
	)

	return errors.Join(cause, err)
}

func reportReservationCleanupError(
	cmd *cobra.Command,
	namespace, name string,
	options app.ReservationCleanupOptions,
	cause error,
) error {
	if blocker, ok := errors.AsType[*app.CleanupPodBlockerError](cause); ok {
		if err := writeCleanupPodBlockerGuidance(cmd.ErrOrStderr(), cmd, blocker); err != nil {
			cause = errors.Join(cause, err)
		}
	}

	prefix := guidancePrefixesForCommand(cmd, namespace).pvcMigrate

	retry := "reserve cleanup " + shellQuote(name)
	if options.UnusedStoragePolicy != "" {
		retry += " --unused-storage-policy " + shellQuote(
			options.UnusedStoragePolicy,
		)
	}

	if options.Finalize {
		retry += " --finalize"
	}

	if options.DeleteSession {
		retry += " --delete-session"
	}

	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Cleanup stopped before confirmed completion. Inspect current state: %s reserve status %s\nRevalidate cleanup before retrying: %s %s --dry-run\n",
		prefix,
		shellQuote(name),
		prefix,
		retry,
	)

	return errors.Join(cause, err)
}

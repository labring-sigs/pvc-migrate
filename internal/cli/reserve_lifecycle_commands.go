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
		r.newReserveStatusCommand(),
		r.newReserveResumeCommand(),
		r.newReserveAbortCommand(),
		r.newReserveCleanupCommand(),
	)
}

func (r *rootState) loadReservation(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
) (crclient.Object, error) {
	object, _, err := r.loadReservationWithBackend(ctx, cmd, runtime, id)
	return object, err
}

func (r *rootState) loadReservationWithBackend(
	ctx context.Context,
	cmd *cobra.Command,
	runtime *commandRuntime,
	id string,
) (crclient.Object, string, error) {
	if runtime.clients == nil {
		return nil, "", domain.NewError(
			domain.ErrorInternal,
			"reserve",
			"Kubernetes clients are required",
		)
	}

	namespace := r.workflowStorageNamespace(cmd)

	object, backend, err := r.loadWorkflowWithBackend(
		ctx,
		cmd,
		runtime,
		namespace,
		id,
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindReservation:        &v1alpha1.Reservation{},
			domain.ControllerKindClusterReservation: &v1alpha1.ClusterReservation{},
		},
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

// reservationHintNamespace resolves the namespace a suggested reserve
// command needs to find this workflow again, across both object shapes.
func reservationHintNamespace(
	backend string,
	r *rootState,
	cmd *cobra.Command,
	object crclient.Object,
) string {
	if current, ok := object.(*v1alpha1.ClusterReservation); ok {
		namespace := string(current.Spec.SessionNamespace)
		if namespace == "" {
			namespace = string(current.Spec.SourceNamespace)
		}

		return namespace
	}

	return workflowLeaseNamespace(backend, r.workflowStorageNamespace(cmd), object)
}

func reservationPhase(object crclient.Object) domain.Phase {
	switch current := object.(type) {
	case *v1alpha1.Reservation:
		return current.Status.Phase
	case *v1alpha1.ClusterReservation:
		return current.Status.Phase
	default:
		return ""
	}
}

func (r *rootState) newReserveStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status [SESSION]",
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
				object, err := r.loadReservation(ctx, cmd, runtime, args[0])
				if err != nil {
					return err
				}

				if err := runtime.printer.Print(object); err != nil {
					return err
				}

				return writeWorkflowNextSteps(
					cmd.ErrOrStderr(),
					guidancePrefixesForCommand(
						cmd,
						reservationHintNamespace("", r, cmd, object),
					).pvcMigrate,
					"reserve",
					object.GetName(),
					reservationPhase(object),
					false,
				)
			}

			namespace := r.workflowStorageNamespace(cmd)

			objects := []crclient.Object{}
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

			if crdListable(runtime) && (len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindReservation)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.Reservation { return &v1alpha1.Reservation{} },
				)
				if err != nil {
					return err
				}

				items, err := crdStore.List(ctx, namespace)
				if err != nil {
					return err
				}

				for _, object := range items {
					objects = append(objects, object)
				}
			}

			if crdListable(runtime) && (len(runtime.controllerKinds) == 0 ||
				slices.Contains(runtime.controllerKinds, domain.ControllerKindClusterReservation)) {
				crdStore, err := cliCRDWorkflowStore(
					runtime,
					func() *v1alpha1.ClusterReservation { return &v1alpha1.ClusterReservation{} },
				)
				if err != nil {
					return err
				}

				items, err := crdStore.List(ctx, "")
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

func (r *rootState) newReserveResumeCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
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

		return r.reserveExisting(ctx, cmd, runtime, args[0], dryRun)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newReserveAbortCommand() *cobra.Command {
	var dryRun bool

	command := &cobra.Command{
		Use:   "abort SESSION",
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

		object, backend, err := r.loadReservationWithBackend(ctx, cmd, runtime, args[0])
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

		if dryRun {
			return writeDryRunNotice(
				cmd.ErrOrStderr(),
				lifecycleExecuteCommand(
					guidancePrefixesForCommand(
						cmd,
						reservationHintNamespace(backend, r, cmd, object),
					).pvcMigrate,
					"reserve",
					"abort",
					object.GetName(),
				),
			)
		}

		return writeWorkflowNextSteps(
			cmd.ErrOrStderr(),
			guidancePrefixesForCommand(
				cmd,
				reservationHintNamespace(backend, r, cmd, object),
			).pvcMigrate,
			"reserve",
			object.GetName(),
			reservationPhase(object),
			false,
		)
	}
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newReserveCleanupCommand() *cobra.Command {
	var (
		options app.ReservationCleanupOptions
		dryRun  bool
	)

	command := &cobra.Command{
		Use:   "cleanup SESSION",
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

		object, backend, err := r.loadReservationWithBackend(ctx, cmd, runtime, args[0])
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
					guidancePrefixesForCommand(
						cmd,
						reservationHintNamespace(backend, r, cmd, object),
					).pvcMigrate,
					"reserve",
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

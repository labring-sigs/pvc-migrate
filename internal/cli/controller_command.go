package cli

import (
	"context"
	"errors"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func (r *rootState) newControllerCommand() *cobra.Command {
	var (
		once         bool
		pollInterval time.Duration
	)

	command := &cobra.Command{
		Use:   "controller",
		Short: "Run the workflow CRD reconciliation loop",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			if runtime.mode != executionModeController {
				return domain.NewError(
					domain.ErrorPrecondition,
					"controller",
					"workflow CRDs are not installed; use --mode=controller after installing deploy/crd.yaml",
				)
			}

			if runtime.controllerStore == nil {
				return domain.NewError(
					domain.ErrorInternal,
					"controller",
					"controller session store is not configured",
				)
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			if once {
				return controller.NewRunner(runtime.service, runtime.controllerStore, r.global.sessionNamespace).
					WithKubernetesClient(runtime.clients.Kubernetes).
					WithOpenEBSLVMSharedVolumeManager(runtime.openEBSLVMSharedVolumeManager).
					WithPollInterval(pollInterval).
					WithLogger(runtime.logger.With("component", "migration-controller")).
					ReconcileOnce(ctx)
			}

			err = controller.StartManagerWithKinds(
				ctx,
				runtime.clients.RESTConfig,
				runtime.service,
				runtime.controllerStore,
				r.global.sessionNamespace,
				runtime.clients.Kubernetes,
				runtime.openEBSLVMSharedVolumeManager,
				runtime.controllerKinds,
			)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			return err
		},
	}
	command.Flags().BoolVar(&once, "once", false, "Run one reconciliation pass and exit")
	command.Flags().
		DurationVar(&pollInterval, "poll-interval", 5*time.Second, "Interval between reconciliation passes")

	return command
}

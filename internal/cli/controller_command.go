package cli

import (
	"context"
	"errors"

	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
)

func (r *rootState) newControllerCommand() *cobra.Command {
	var once bool

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

			if once {
				ctx, cancel := r.context(cmd.Context())
				defer cancel()

				if err := controller.ValidateTrustedToolImage(r.global.toolImage); err != nil {
					return err
				}

				cluster, err := kube.Identity(ctx, runtime.clients)
				if err != nil {
					return domain.WrapError(
						domain.ErrorPrecondition,
						"controller",
						"resolve cluster identity",
						err,
					)
				}
				// A one-shot controller pass is an operator operation and must
				// inspect every tenant namespace. The normal manager path receives
				// namespace/name directly from controller-runtime events.
				return controller.NewRunner(runtime.service, runtime.controllerStore, "").
					WithKubernetesClient(runtime.clients.Kubernetes).
					WithControllerClient(runtime.clients.Runtime).
					WithClusterIdentity(cluster.ID).
					WithTrustedToolImage(r.global.toolImage).
					WithKubeconfig(r.global.kubeconfig, r.global.kubeContext).
					WithOpenEBSLVMSharedVolumeManager(runtime.openEBSLVMSharedVolumeManager).
					WithLogger(runtime.logger.With("component", "workflow-controller")).
					ReconcileOnce(ctx)
			}

			err = controller.StartManagerWithKinds(
				cmd.Context(),
				runtime.clients.RESTConfig,
				runtime.service,
				runtime.controllerStore,
				r.global.controllerNamespace,
				runtime.clients.Kubernetes,
				runtime.openEBSLVMSharedVolumeManager,
				r.global.kubeconfig,
				r.global.kubeContext,
				runtime.controllerKinds,
				r.global.toolImage,
			)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			return err
		},
	}
	command.Flags().BoolVar(&once, "once", false, "Run one reconciliation pass and exit")

	return command
}

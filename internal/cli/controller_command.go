package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
)

func (r *rootState) newControllerCommand() *cobra.Command {
	var (
		once                   bool
		controllerNamespace    string
		healthProbeBindAddress string
	)

	command := &cobra.Command{
		Use:   "controller",
		Short: "Run the workflow CRD reconciliation loop",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// --timeout bounds one command run; the daemon runs until its
			// process signal and never consumes it. Refusing the explicit
			// flag keeps an accepted flag effective (it also made the
			// per-attempt --copy-timeout validation compare against a
			// deadline nothing enforces).
			if !once && cmd.Flags().Changed("timeout") {
				return domain.NewError(
					domain.ErrorValidation,
					"flags",
					"--timeout bounds a single command run and is consumed by --once; the daemon runs until its process signal",
				)
			}

			if problems := validation.IsDNS1123Label(controllerNamespace); len(problems) > 0 {
				return domain.NewError(
					domain.ErrorValidation,
					"flags",
					fmt.Sprintf(
						"--controller-namespace %q is invalid: %s",
						controllerNamespace,
						strings.Join(problems, "; "),
					),
				)
			}

			runtime, err := r.runtime()
			if err != nil {
				return err
			}

			if runtime.controllerDiscoveryComplete && len(runtime.controllerKinds) == 0 {
				return domain.NewError(
					domain.ErrorPrecondition,
					"controller mode",
					"controller mode requires at least one migrate.sealos.io/v1alpha1 workflow CRD; install the controller with its Helm chart (charts/pvc-migrate, or the published OCI chart)",
				)
			}

			options := controller.ManagerOptions{
				BackupPlanner:                 runtime.planner.ForController().PlanBackup,
				RestorePlanner:                runtime.planner.ForController().PlanRestore,
				RenamePlanner:                 runtime.planner.ForController().PlanRename,
				MovePlanner:                   runtime.planner.ForController().PlanMove,
				ReservationPlanner:            runtime.planner.ForController().PlanReserve,
				NamespacedReservationPlanner:  runtime.planner.ForController().PlanReservation,
				CopyPlanner:                   runtime.planner.ForController().PlanCopy,
				MigrationPlanner:              runtime.planner.ForController().PlanOfflineMigration,
				NamespacedMigrationPlanner:    runtime.planner.ForController().PlanNamespacedMigration,
				PodMigrationPlanner:           runtime.planner.ForController().PlanPodMigration,
				NamespacedPodMigrationPlanner: runtime.planner.ForController().PlanNamespacedPodMigration,
				WorkloadManager:               runtime.controllers,
				NamespacedCopyPlanner:         runtime.planner.ForController().PlanNamespacedCopy,
				TransferExecution: app.VolumeCopyConfig{
					Retries: r.global.retries, RetryBackoff: r.global.retryBackoff,
					HelmTimeout: r.global.helmTimeout, Compress: r.global.compress,
					BandwidthLimit: r.global.copyBandwidth,
					CopyTimeout:    r.global.copyTimeout,
				},
				Namespace:                     controllerNamespace,
				KubernetesClient:              runtime.clients.Kubernetes,
				OpenEBSLVMSharedVolumeManager: runtime.openEBSLVMSharedVolumeManager,
				KubeconfigPath:                r.global.kubeconfig,
				KubeContext:                   r.global.kubeContext,
				SupportedKinds:                runtime.controllerKinds,
				TrustedToolImage:              r.global.toolImage,
				Logger:                        runtime.controllerLogger,
				HealthProbeBindAddress:        healthProbeBindAddress,
			}
			if once {
				ctx, cancel := r.context(cmd.Context())
				defer cancel()

				return controller.ReconcileWorkflowsOnce(
					ctx,
					runtime.clients.Runtime,
					options,
				)
			}

			err = controller.StartManager(
				cmd.Context(),
				runtime.clients.RESTConfig,
				options,
			)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			return err
		},
	}
	command.Flags().StringVar(
		&controllerNamespace,
		"controller-namespace",
		"pvc-migrate-system",
		"Namespace where the controller runs and holds its leader Lease",
	)
	command.Flags().
		BoolVar(&once, "once", false, "Reconcile current workflows until stable and exit")
	command.Flags().StringVar(
		&healthProbeBindAddress,
		"health-probe-bind-address",
		":8081",
		"Address for controller health and readiness probes",
	)

	return command
}

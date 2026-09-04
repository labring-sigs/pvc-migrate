package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// crossClusterConnectionFlags contains only two-cluster client settings.
// Workflow modules embed it so copy and reserve keep independent business
// flags and option conversion.
type crossClusterConnectionFlags struct {
	sourceKubeconfig      string
	sourceContext         string
	destinationKubeconfig string
	destinationContext    string
	sessionNamespace      string
}

type crossClusterCopyFlags struct {
	crossClusterConnectionFlags
	sessionID               string
	sourceNamespace         string
	destinationNamespace    string
	sourcePVCs              []string
	destinationPVCs         []string
	destinationCapacities   []string
	sourcePaths             []string
	destinationPaths        []string
	destinationStorageClass string
	allowVolumeShrink       bool
	skipSourceUsageCheck    bool
	online                  bool
	verifyChecksum          bool
	deleteExtraneous        bool
	targetNode              string
	toolImage               string
	strategies              []string
	deleteDestination       bool
	deleteSession           bool
}

func (r *rootState) newCrossClusterCopyCommand() *cobra.Command {
	command := r.newCrossClusterCopyRunCommand()
	command.Use = "cross-cluster"
	command.Short = "Copy PVC data between two Kubernetes clusters"
	command.AddCommand(
		r.newCrossClusterCopyPlanCommand(),
		r.newCrossClusterCopyStatusCommand(),
		r.newCrossClusterCopyResumeCommand(),
		r.newCrossClusterCopyCleanupCommand(),
	)

	return command
}

func (r *rootState) newCrossClusterCopyStatusCommand() *cobra.Command {
	flags := &crossClusterCopyFlags{}
	command := &cobra.Command{
		Use: "status SESSION", Short: "Show a cross-cluster copy session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.sessionID = args[0]

			service, err := r.crossClusterService(&flags.crossClusterConnectionFlags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := service.Get(ctx, flags.sessionNamespace, args[0])
			if err != nil {
				return err
			}

			return r.crossPrinter().Print(session)
		},
	}
	flags.bindConnections(command, r)

	return command
}

func (r *rootState) newCrossClusterCopyResumeCommand() *cobra.Command {
	flags := &crossClusterCopyFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Resume a cross-cluster copy session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.sessionID = args[0]

			service, err := r.crossClusterService(&flags.crossClusterConnectionFlags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := service.Get(ctx, flags.sessionNamespace, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				return r.crossPrinter().Print(session)
			}

			if err := service.Copy(
				ctx,
				session,
				r.global.retries,
				r.global.noCompress,
			); err != nil {
				return err
			}

			return r.crossPrinter().Print(session)
		},
	}
	flags.bindConnections(command, r)
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newCrossClusterCopyPlanCommand() *cobra.Command {
	flags := &crossClusterCopyFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Validate a cross-cluster copy without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			service, err := r.crossClusterService(&flags.crossClusterConnectionFlags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			options, err := flags.options(r)
			if err != nil {
				return err
			}

			plan, err := service.Plan(ctx, options)
			if err != nil {
				return err
			}

			if err := r.crossPrinter().Print(plan); err != nil {
				return err
			}

			if !plan.Ready {
				return domain.NewError(
					domain.ErrorPrecondition,
					"copy cross-cluster plan",
					"plan contains failed checks",
				)
			}

			_, err = fmt.Fprintf(
				cmd.ErrOrStderr(),
				"\nCross-cluster copy validation passed. Execute with --dry-run=false.\n",
			)

			return err
		},
	}
	flags.bind(command, r)

	return command
}

func (r *rootState) newCrossClusterCopyRunCommand() *cobra.Command {
	flags := &crossClusterCopyFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "run",
		Short: "Reserve and copy PVC data between clusters",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			service, err := r.crossClusterService(&flags.crossClusterConnectionFlags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			options, err := flags.options(r)
			if err != nil {
				return err
			}

			var session *crosscluster.Session
			if flags.sessionID != "" {
				session, err = service.Get(ctx, options.SessionNamespace, flags.sessionID)
				if apierrors.IsNotFound(err) {
					session, err = nil, nil
				}
			}

			if session == nil && err == nil {
				plan, planErr := service.Plan(ctx, options)
				if planErr != nil {
					return planErr
				}

				if printErr := r.crossPrinter().Print(plan); printErr != nil {
					return printErr
				}

				if !plan.Ready {
					return domain.NewError(
						domain.ErrorPrecondition,
						"copy cross-cluster",
						"plan contains failed checks",
					)
				}

				if dryRun {
					return nil
				}

				session, err = service.CreateSession(ctx, options, plan)
			}

			if err != nil {
				return err
			}

			if dryRun {
				return r.crossPrinter().Print(session)
			}

			if err := service.Copy(
				ctx,
				session,
				r.global.retries,
				r.global.noCompress,
			); err != nil {
				return err
			}

			if err := r.crossPrinter().Print(session); err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				cmd.ErrOrStderr(),
				"\nCross-cluster copy completed. Verify destination data before deleting the session-owned destination PVC.\nCleanup when ready with: %s\n",
				crossClusterCopyCleanupCommand(flags, session.ID),
			)

			return err
		},
	}
	flags.bind(command, r)
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newCrossClusterCopyCleanupCommand() *cobra.Command {
	flags := &crossClusterCopyFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Clean up a cross-cluster copy session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.sessionID = args[0]

			service, err := r.crossClusterService(&flags.crossClusterConnectionFlags)
			if err != nil {
				return err
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			session, err := service.Get(ctx, flags.sessionNamespace, args[0])
			if err != nil {
				return err
			}

			if dryRun {
				return r.crossPrinter().Print(session)
			}

			if !r.global.assumeYes {
				return domain.NewError(
					domain.ErrorPrecondition,
					"copy cross-cluster cleanup",
					"re-run with --yes after reviewing the session",
				)
			}

			if err := service.Cleanup(
				ctx,
				session,
				flags.deleteDestination,
				flags.deleteSession,
			); err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				cmd.ErrOrStderr(),
				"cross-cluster session %s cleanup completed\n",
				args[0],
			)

			return err
		},
	}
	flags.bindConnections(command, r)
	command.Flags().
		BoolVar(&flags.deleteDestination, "delete-destination", false, "Delete session-owned destination PVCs and their released PVs")
	command.Flags().
		BoolVar(&flags.deleteSession, "delete-session", false, "Delete the source-cluster session record")
	bindDryRun(command, &dryRun)

	return command
}

func (f *crossClusterCopyFlags) bind(command *cobra.Command, r *rootState) {
	f.bindConnections(command, r)
	flags := command.Flags()
	flags.StringVarP(&f.sourceNamespace, "source-namespace", "n", "default", "Source PVC namespace")
	flags.StringVar(
		&f.destinationNamespace,
		"destination-namespace",
		"",
		"Destination PVC namespace; defaults to source namespace",
	)
	flags.StringVar(&f.sessionID, "session", "", "Cross-cluster session ID")
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
		"Destination PVC name; use source=destination for explicit multiple-PVC mappings",
	)
	flags.StringSliceVar(
		&f.destinationCapacities,
		"destination-capacity",
		nil,
		"Destination capacity; use source=capacity for explicit mappings",
	)
	flags.StringArrayVar(
		&f.sourcePaths,
		"source-path",
		nil,
		"Source path; use source=path for explicit mappings",
	)
	flags.StringArrayVar(
		&f.destinationPaths,
		"destination-path",
		nil,
		"Destination path; use source=path for explicit mappings",
	)
	flags.StringVar(
		&f.destinationStorageClass,
		"destination-storage-class",
		"",
		"Destination StorageClass (required)",
	)
	flags.BoolVar(
		&f.allowVolumeShrink,
		"allow-volume-shrink",
		false,
		"Allow destination capacity below source capacity",
	)
	flags.BoolVar(
		&f.skipSourceUsageCheck,
		"skip-source-usage-check",
		false,
		"Proceed with shrink after independently verifying source data fits",
	)
	flags.BoolVar(
		&f.online,
		"online",
		false,
		"Allow active source consumers during a best-effort copy",
	)
	flags.BoolVar(
		&f.verifyChecksum,
		"verify-checksum",
		false,
		"Verify copied files with rsync checksums",
	)
	flags.BoolVar(
		&f.deleteExtraneous,
		"delete-extraneous",
		true,
		"Delete destination files absent from source",
	)
	flags.StringVar(&f.targetNode, "target-node", domain.AutoValue, "Destination reservation node")
	flags.StringVar(&f.toolImage, "tool-image", r.global.toolImage, "Tool image")
	flags.StringSliceVar(
		&f.strategies,
		"strategy",
		[]string{domain.StrategyLocal},
		"Cross-cluster strategy: local, loadbalancer, or nodeport",
	)
}

func (f *crossClusterConnectionFlags) bindConnections(command *cobra.Command, r *rootState) {
	flags := command.Flags()
	flags.StringVar(
		&f.sourceKubeconfig,
		"source-kubeconfig",
		r.global.kubeconfig,
		"Source kubeconfig path",
	)
	flags.StringVar(
		&f.sourceContext,
		"source-context",
		r.global.kubeContext,
		"Source Kubernetes context",
	)
	flags.StringVar(
		&f.destinationKubeconfig,
		"destination-kubeconfig",
		"",
		"Destination kubeconfig path (required)",
	)
	flags.StringVar(
		&f.destinationContext,
		"destination-context",
		"",
		"Destination Kubernetes context",
	)
	flags.StringVar(
		&f.sessionNamespace,
		"session-namespace",
		r.global.sessionNamespace,
		"Source-cluster namespace for cross-cluster sessions",
	)
}

func (f *crossClusterCopyFlags) options(r *rootState) (crosscluster.Options, error) {
	if f.destinationKubeconfig == "" {
		return crosscluster.Options{}, domain.NewError(
			domain.ErrorValidation,
			"cross-cluster flags",
			"--destination-kubeconfig is required",
		)
	}

	if f.destinationNamespace == "" {
		f.destinationNamespace = f.sourceNamespace
	}

	if f.sessionNamespace == "" {
		f.sessionNamespace = r.global.sessionNamespace
	}

	if f.toolImage == "" {
		f.toolImage = r.global.toolImage
	}

	if f.sourceNamespace == "" || f.destinationNamespace == "" {
		return crosscluster.Options{}, domain.NewError(
			domain.ErrorValidation,
			"cross-cluster flags",
			"source and destination namespaces are required",
		)
	}

	id := f.sessionID
	if id == "" {
		generated, err := domain.NewSessionID(time.Now())
		if err != nil {
			return crosscluster.Options{}, err
		}

		id = generated
		f.sessionID = id
	}

	if err := crosscluster.ValidateSessionID(id); err != nil {
		return crosscluster.Options{}, domain.NewError(
			domain.ErrorValidation,
			"cross-cluster flags",
			err.Error(),
		)
	}

	return crosscluster.Options{
		SessionID:               id,
		SessionNamespace:        f.sessionNamespace,
		SourceNamespace:         f.sourceNamespace,
		DestinationNamespace:    f.destinationNamespace,
		SourcePVCs:              f.sourcePVCs,
		DestinationPVCs:         f.destinationPVCs,
		DestinationCapacities:   f.destinationCapacities,
		SourcePaths:             f.sourcePaths,
		DestinationPaths:        f.destinationPaths,
		DestinationStorageClass: f.destinationStorageClass,
		AllowVolumeShrink:       f.allowVolumeShrink,
		SkipSourceUsageCheck:    f.skipSourceUsageCheck,
		Online:                  f.online,
		VerifyChecksum:          f.verifyChecksum,
		DeleteExtraneous:        f.deleteExtraneous,
		TargetNode:              f.targetNode,
		ToolImage:               f.toolImage,
		Strategies:              f.strategies,
	}, nil
}

func (r *rootState) crossClusterService(
	flags *crossClusterConnectionFlags,
) (*crosscluster.Service, error) {
	if err := r.validateGlobalFlags(); err != nil {
		return nil, err
	}

	mode, err := parseExecutionMode(r.global.mode)
	if err != nil {
		return nil, err
	}

	if mode == executionModeController {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"cross-cluster mode",
			"cross-cluster workflows require two explicit API-server connections and use the session backend; use --mode=session",
		)
	}

	if flags.destinationKubeconfig == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"cross-cluster flags",
			"--destination-kubeconfig is required",
		)
	}

	if flags.sourceKubeconfig == "" {
		flags.sourceKubeconfig = r.global.kubeconfig
	}

	if flags.sourceContext == "" {
		flags.sourceContext = r.global.kubeContext
	}

	logger, err := loggerFor(r)
	if err != nil {
		return nil, err
	}

	configureKubernetesLogger(logger)

	source, dest, err := r.crossClusterClients(
		flags.sourceKubeconfig,
		flags.sourceContext,
		flags.destinationKubeconfig,
		flags.destinationContext,
	)
	if err != nil {
		return nil, err
	}

	return crosscluster.NewService(source, dest, copyengine.NewPVMigrate()).
			WithConnections(flags.sourceKubeconfig, flags.sourceContext, flags.destinationKubeconfig, flags.destinationContext).
			WithRuntime(r.errWriter(), logger.With("component", "cross-cluster"), r.global.helmTimeout),
		nil
}

func (r *rootState) crossClusterClients(
	sourceKubeconfig, sourceContext, destinationKubeconfig, destinationContext string,
) (*kube.Clients, *kube.Clients, error) {
	source, err := kube.NewClients(sourceKubeconfig, sourceContext)
	if err != nil {
		return nil, nil, err
	}

	dest, err := kube.NewClients(destinationKubeconfig, destinationContext)
	if err != nil {
		return nil, nil, err
	}

	return source, dest, nil
}

func (r *rootState) crossPrinter() output.Printer {
	return output.Printer{Writer: r.options.Out, Format: output.Format(r.global.output)}
}

func crossClusterCopyCleanupCommand(flags *crossClusterCopyFlags, sessionID string) string {
	args := []string{"pvc-migrate", "copy", "cross-cluster", "cleanup", shellQuote(sessionID)}
	for _, connection := range []struct {
		name  string
		value string
	}{
		{name: "source-kubeconfig", value: flags.sourceKubeconfig},
		{name: "source-context", value: flags.sourceContext},
		{name: "destination-kubeconfig", value: flags.destinationKubeconfig},
		{name: "destination-context", value: flags.destinationContext},
		{name: "session-namespace", value: flags.sessionNamespace},
	} {
		if connection.value != "" {
			args = append(args, "--"+connection.name, shellQuote(connection.value))
		}
	}

	args = append(args, "--delete-destination", "--delete-session", "--yes", "--dry-run=false")

	return strings.Join(args, " ")
}

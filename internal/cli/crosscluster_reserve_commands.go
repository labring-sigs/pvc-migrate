package cli

import (
	"fmt"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type crossClusterReserveFlags struct {
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
	targetNode              string
	toolImage               string
	strategies              []string
	deleteDestination       bool
	deleteSession           bool
}

// bind exposes only the connection, identity, capacity, and
// provisioning controls needed to create a cross-cluster reservation. Copy
// consistency and online controls belong to copy's bind method.
func (f *crossClusterReserveFlags) bind(command *cobra.Command, r *rootState) {
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
		"Destination PVC name; use source=destination for explicit mappings",
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
	flags.StringVar(&f.targetNode, "target-node", domain.AutoValue, "Destination reservation node")
	flags.StringVar(&f.toolImage, "tool-image", r.global.toolImage, "Tool image")
	flags.StringSliceVar(
		&f.strategies,
		"strategy",
		[]string{"local"},
		"Cross-cluster strategy: local, loadbalancer, or nodeport",
	)
}

func (f *crossClusterReserveFlags) options(r *rootState) (crosscluster.Options, error) {
	if f.destinationKubeconfig == "" {
		return crosscluster.Options{}, domain.NewError(
			domain.ErrorValidation,
			"cross-cluster reserve flags",
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
			"cross-cluster reserve flags",
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
			"cross-cluster reserve flags",
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
		TargetNode:              f.targetNode,
		ToolImage:               f.toolImage,
		Strategies:              f.strategies,
	}, nil
}

// newCrossClusterReserveCommand owns the cross-cluster reservation command
// tree. Copy-specific commands remain in crosscluster_commands.go.
func (r *rootState) newCrossClusterReserveCommand() *cobra.Command {
	command := r.newCrossClusterReserveRunCommand()
	command.Use = "cross-cluster"
	command.Short = "Reserve destination PVCs in another Kubernetes cluster"
	command.AddCommand(
		r.newCrossClusterReservePlanCommand(),
		r.newCrossClusterReserveStatusCommand(),
		r.newCrossClusterReserveResumeCommand(),
		r.newCrossClusterReserveCleanupCommand(),
	)

	return command
}

func (r *rootState) newCrossClusterReserveResumeCommand() *cobra.Command {
	flags := &crossClusterReserveFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "resume SESSION",
		Short: "Resume a cross-cluster reservation session",
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

			if err := service.Reserve(ctx, session); err != nil {
				return err
			}

			return r.crossPrinter().Print(session)
		},
	}
	flags.bindConnections(command, r)
	bindDryRun(command, &dryRun)

	return command
}

func (r *rootState) newCrossClusterReserveStatusCommand() *cobra.Command {
	flags := &crossClusterReserveFlags{}
	command := &cobra.Command{
		Use:   "status SESSION",
		Short: "Show a cross-cluster reserve session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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

func (r *rootState) newCrossClusterReserveCleanupCommand() *cobra.Command {
	flags := &crossClusterReserveFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "cleanup SESSION",
		Short: "Clean up a cross-cluster reserve session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
					"reserve cross-cluster cleanup",
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
				"cross-cluster reserve session %s cleanup completed\n",
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

func (r *rootState) newCrossClusterReservePlanCommand() *cobra.Command {
	flags := &crossClusterReserveFlags{}
	command := &cobra.Command{
		Use:   "plan",
		Short: "Validate destination reservation without mutations",
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
					"reserve cross-cluster plan",
					"plan contains failed checks",
				)
			}

			return nil
		},
	}
	flags.bind(command, r)

	return command
}

func (r *rootState) newCrossClusterReserveRunCommand() *cobra.Command {
	flags := &crossClusterReserveFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "run",
		Short: "Create and reserve destination PVCs in another cluster",
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
						"reserve cross-cluster",
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

			if err := service.Reserve(ctx, session); err != nil {
				return err
			}

			if err := r.crossPrinter().Print(session); err != nil {
				return err
			}

			_, err = fmt.Fprintf(
				cmd.ErrOrStderr(),
				"\nDestination PVC reservation completed. Continue with: pvc-migrate copy cross-cluster --session %s --dry-run=false (reuse the same source/destination connection flags).\n",
				session.ID,
			)

			return err
		},
	}
	flags.bind(command, r)
	bindDryRun(command, &dryRun)

	return command
}

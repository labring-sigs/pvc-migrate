package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
	"github.com/labring-sigs/pvc-migrate/internal/controller"
	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"
)

type Options struct {
	Version             string
	ToolImageRepository string
	In                  io.Reader
	Out                 io.Writer
	ErrOut              io.Writer
	runtimeFactory      func(*rootState) (*commandRuntime, error)
	objectStoreFactory  func(context.Context, objectstore.Config) (*objectstore.Store, error)
}

type globals struct {
	kubeconfig        string
	kubeContext       string
	sessionNamespace  string
	workflowNamespace string
	timeout           time.Duration
	retries           int
	retryBackoff      time.Duration
	helmTimeout       time.Duration
	output            string
	logFormat         string
	logLevel          string
	color             string
	streamToolLogs    bool
	noCompress        bool
	assumeYes         bool
	toolImage         string
}

type logFormat string

const (
	logFormatText logFormat = "text"
	logFormatJSON logFormat = "json"
)

type rootState struct {
	options Options
	global  globals
	errOut  io.Writer
}

type commandRuntime struct {
	clients                            *kube.Clients
	planner                            *planner.Planner
	printer                            output.Printer
	logger                             *slog.Logger
	controllerLogger                   *slog.Logger
	controllers                        *controller.Manager
	openEBSLVMSharedVolumeManager      kube.OpenEBSLVMSharedVolumeManager
	controllerKinds                    []domain.ControllerKind
	waitForController                  bool
	clusterPodMigrationStore           kube.WorkflowStore[*v1alpha1.ClusterPodMigration]
	clusterPodMigrationExecutor        *app.ClusterPodMigrationExecutor
	clusterPodMigrationSessionStore    kube.WorkflowStore[*v1alpha1.ClusterPodMigration]
	clusterPodMigrationSessionExecutor *app.ClusterPodMigrationExecutor
	orphanCleaner                      *app.OrphanCleaner
}

func NewRoot(options Options) *cobra.Command {
	if options.In == nil {
		options.In = strings.NewReader("")
	}

	if options.Out == nil {
		options.Out = io.Discard
	}

	if options.ErrOut == nil {
		options.ErrOut = io.Discard
	}

	state := &rootState{options: options}
	coloredErrOut := newColorOutputWriter(options.ErrOut, func() bool {
		return state.global.logFormat != string(logFormatJSON) &&
			colorEnabled(state.global.color, options.ErrOut)
	})
	state.errOut = newLogOutputWriter(
		coloredErrOut,
		func() bool { return state.global.logFormat == string(logFormatJSON) },
	)
	command := &cobra.Command{
		Use:           "pvc-migrate",
		Short:         "Resumable Kubernetes PVC migration",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(*cobra.Command, []string) error {
			_, err := parseColorMode(state.global.color)
			return err
		},
	}
	command.SetIn(options.In)
	command.SetOut(options.Out)
	command.SetErr(state.errWriter())
	flags := command.PersistentFlags()
	flags.StringVar(&state.global.kubeconfig, "kubeconfig", "", "Kubeconfig path")
	flags.StringVar(&state.global.kubeContext, "context", "", "Kubernetes context")
	flags.StringVar(
		&state.global.sessionNamespace,
		"session-namespace",
		"pvc-migrate-system",
		"Namespace for persistent migration sessions",
	)
	flags.StringVar(
		&state.global.workflowNamespace,
		"workflow-namespace",
		"",
		"Tenant namespace containing a controller workflow for lifecycle/status commands",
	)
	flags.DurationVar(&state.global.timeout, "timeout", 30*time.Minute, "Operation timeout")
	flags.IntVar(&state.global.retries, "retries", 3, "Copy retry attempts")
	flags.DurationVar(
		&state.global.retryBackoff,
		"retry-backoff",
		2*time.Second,
		"Initial copy retry backoff",
	)
	flags.DurationVar(
		&state.global.helmTimeout,
		"helm-timeout",
		10*time.Minute,
		"pv-migrate tool deployment timeout",
	)
	flags.StringVarP(
		&state.global.output,
		"output",
		"o",
		string(output.Table),
		"Output format: table, json, yaml",
	)
	flags.StringVar(
		&state.global.logFormat,
		"log-format",
		string(logFormatText),
		"Log format: text, json",
	)
	flags.StringVar(
		&state.global.logLevel,
		"log-level",
		"info",
		"Log level: debug, info, warn, error",
	)
	flags.StringVar(
		&state.global.color,
		"color",
		colorAuto,
		"Colorize text logs: auto, always, never",
	)
	flags.BoolVar(
		&state.global.streamToolLogs,
		"stream-tool-logs",
		true,
		"Stream generated tool Pod logs to stderr",
	)
	flags.BoolVar(&state.global.noCompress, "no-compress", false, "Disable rsync compression")
	flags.BoolVarP(
		&state.global.assumeYes,
		"yes",
		"y",
		false,
		"Approve workload pause and storage identity changes",
	)
	flags.StringVar(
		&state.global.toolImage,
		"tool-image",
		kube.DefaultToolImage(options.ToolImageRepository, options.Version),
		"Tool image used by PVC reservation, copy, SSHD, and backup tools",
	)

	command.AddCommand(
		state.newReserveCommand(),
		state.newCopyCommand(),
		state.newMigrateCommand(),
		state.newMigratePodCommand(),
		state.newRenameCommand(),
		state.newMoveCommand(),
		state.newBackupCommand(),
		state.newRestoreCommand(),
		state.newRecoveryCommand(),
		state.newControllerCommand(),
		newVersionCommand(options.Version),
	)
	command.AddCommand(newCompletionCommand(command))

	// Cross-cluster workflows run against two explicit API-server connections
	// in the submitting process; they hang off copy/reserve as subcommands.
	for _, parent := range command.Commands() {
		switch parent.Name() {
		case "copy":
			parent.AddCommand(state.newCrossClusterCopyCommand())
		case "reserve":
			parent.AddCommand(state.newCrossClusterReserveCommand())
		}
	}

	return command
}

func bindDryRun(command *cobra.Command, target *bool) {
	command.Flags().
		BoolVar(target, "dry-run", true, "Validate and print the plan without mutations; use --dry-run=false to execute")
}

// bindCreateDryRun defaults to preview: every write operation — including
// controller submission — requires an explicit --dry-run=false to execute.
func bindCreateDryRun(command *cobra.Command, target *bool) {
	command.Flags().
		BoolVar(target, "dry-run", true, "Print the workflow that would be submitted without creating it; use --dry-run=false to submit")
}

// bindCreateWait keeps controller-wait semantics on the submission commands
// that can actually observe a controller-backed workflow. Session commands
// execute in-process and must not offer it.
func bindCreateWait(command *cobra.Command, target *bool) {
	command.Flags().
		BoolVar(target, "wait", true, "Wait for the controller to finish the submitted workflow; use --wait=false to return immediately after submission")
}

func (r *rootState) runtime() (*commandRuntime, error) {
	if r.options.runtimeFactory != nil {
		return r.options.runtimeFactory(r)
	}

	if err := r.validateGlobalFlags(); err != nil {
		return nil, err
	}

	format := output.Format(r.global.output)
	if format != output.Table && format != output.JSON && format != output.YAML {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"flags",
			fmt.Sprintf("unsupported output format %q", r.global.output),
		)
	}

	logger, err := loggerFor(r)
	if err != nil {
		return nil, err
	}

	configureKubernetesLogger(logger)

	clients, err := kube.NewClients(r.global.kubeconfig, r.global.kubeContext)
	if err != nil {
		return nil, err
	}

	controllers := controller.NewManager(clients.Kubernetes, clients.Dynamic, clients.Discovery).
		WithRESTConfig(clients.RESTConfig).
		WithLogger(logger.With("component", "controller"))

	if _, err := kube.NormalizeToolImage(r.global.toolImage); err != nil {
		return nil, domain.WrapError(
			domain.ErrorPrecondition,
			"controller mode",
			"controller trusted tool image is invalid",
			err,
		)
	}

	controllerKinds := kube.AvailableControllerWorkflowKinds(clients.Discovery)
	if len(controllerKinds) == 0 {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"controller mode",
			"controller mode requires at least one migrate.sealos.io/v1alpha1 workflow CRD; install deploy/crd.yaml",
		)
	}

	clusterPodMigrationStore, err := kube.NewCRDWorkflowStore(
		clients.Runtime,
		func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
	)
	if err != nil {
		return nil, err
	}

	clusterPodMigrationLocker := kube.NewCRDWorkflowLocker(clients.Kubernetes)

	openEBSLVMSharedVolumeManager := kube.NewOpenEBSLVMSharedVolumeManager(
		clients.Kubernetes,
		clients.Dynamic,
	)

	structuredLogs := r.global.logFormat == string(logFormatJSON)
	serviceWriter := r.errWriter()

	serviceLogger := controller.NewControllerLogger(logger.With("component", "migration"))

	transferConfig := app.VolumeCopyConfig{
		KubeconfigPath: r.global.kubeconfig,
		Context:        r.global.kubeContext,
		Retries:        r.global.retries,
		RetryBackoff:   r.global.retryBackoff,
		HelmTimeout:    r.global.helmTimeout,
		NoCompress:     r.global.noCompress,
		StreamToolLogs: r.global.streamToolLogs,
		StructuredLogs: structuredLogs,
		Writer:         serviceWriter,
		Logger:         serviceLogger,
		// No TrustedToolImage pin on the session side: the plan records the
		// requested tool image and every executor stage must use exactly that
		// image. The controller pins its own trusted image separately.
	}
	clusterPodMigrationExecutor := app.NewClusterPodMigrationExecutor(
		clients.Kubernetes,
		clusterPodMigrationStore,
		clusterPodMigrationLocker,
		r.global.sessionNamespace,
		copyengine.NewPVMigrate(),
		app.PodMigrationExecutorConfig{
			Storage: app.MigrationExecutorConfig{
				Transfer:        transferConfig,
				ToolImageProber: kube.NewToolImageProber(clients.Kubernetes),
				ProbeTimeout:    r.global.helmTimeout,
			},
			SharedVolumes: openEBSLVMSharedVolumeManager,
			Workloads:     controllers,
		},
	)

	// Session-side migrate-pod persists the concrete CRD in a ConfigMap: the
	// CLI session path never creates workflow CRs, so the executor is bound to
	// the ConfigMap store with the matching locker.
	clusterPodMigrationSessionStore, err := kube.NewConfigMapWorkflowStore(
		clients.Kubernetes,
		r.global.sessionNamespace,
		func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
	)
	if err != nil {
		return nil, err
	}

	clusterPodMigrationSessionExecutor := app.NewClusterPodMigrationExecutor(
		clients.Kubernetes,
		clusterPodMigrationSessionStore,
		kube.NewConfigMapWorkflowLocker(clients.Kubernetes),
		r.global.sessionNamespace,
		copyengine.NewPVMigrate(),
		app.PodMigrationExecutorConfig{
			Storage: app.MigrationExecutorConfig{
				Transfer:        transferConfig,
				ToolImageProber: kube.NewToolImageProber(clients.Kubernetes),
				ProbeTimeout:    r.global.helmTimeout,
			},
			SharedVolumes: openEBSLVMSharedVolumeManager,
			Workloads:     controllers,
		},
	)

	orphanCleaner := app.NewOrphanCleaner(
		clients.Kubernetes,
		clusterPodMigrationLocker,
		kube.NewCRDWorkflowLeaseCleaner(clients.Kubernetes),
		kube.NewCompositeWorkflowOwnerFinder(
			kube.NewCRDWorkflowOwnerFinder(clients.Dynamic),
			kube.NewConfigMapWorkflowOwnerFinder(clients.Kubernetes),
		),
		logger.With("component", "recovery"),
	)

	return &commandRuntime{
		clients: clients,
		planner: planner.New(clients.Kubernetes, controllers).
			WithWorkflowOwnerFinder(kube.NewCompositeWorkflowOwnerFinder(
				kube.NewCRDWorkflowOwnerFinder(clients.Dynamic),
				kube.NewConfigMapWorkflowOwnerFinder(clients.Kubernetes),
			)).
			WithControllerSubmission(false).
			WithOpenEBSLVMSharedVolumeManager(openEBSLVMSharedVolumeManager).
			WithLogger(logger.With("component", "planner")),
		printer: output.Printer{Writer: r.options.Out, Format: format},
		logger:  logger.With("component", "backup"),
		controllerLogger: controller.NewControllerLogger(
			logger.With("component", "workflow-controller"),
		),
		controllers:                        controllers,
		openEBSLVMSharedVolumeManager:      openEBSLVMSharedVolumeManager,
		controllerKinds:                    slices.Clone(controllerKinds),
		waitForController:                  true,
		clusterPodMigrationStore:           clusterPodMigrationStore,
		clusterPodMigrationExecutor:        clusterPodMigrationExecutor,
		clusterPodMigrationSessionStore:    clusterPodMigrationSessionStore,
		clusterPodMigrationSessionExecutor: clusterPodMigrationSessionExecutor,
		orphanCleaner:                      orphanCleaner,
	}, nil
}

func (r *rootState) validateGlobalFlags() error {
	switch {
	case r.global.retries < 1:
		return domain.NewError(domain.ErrorValidation, "flags", "--retries must be at least 1")
	case r.global.retryBackoff <= 0:
		return domain.NewError(
			domain.ErrorValidation,
			"flags",
			"--retry-backoff must be greater than 0",
		)
	case r.global.helmTimeout <= 0:
		return domain.NewError(
			domain.ErrorValidation,
			"flags",
			"--helm-timeout must be greater than 0",
		)
	}

	if problems := validation.IsDNS1123Label(r.global.sessionNamespace); len(problems) > 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"flags",
			fmt.Sprintf(
				"--session-namespace %q is invalid: %s",
				r.global.sessionNamespace,
				strings.Join(problems, "; "),
			),
		)
	}

	if r.global.workflowNamespace != "" {
		if problems := validation.IsDNS1123Label(r.global.workflowNamespace); len(problems) > 0 {
			return domain.NewError(
				domain.ErrorValidation,
				"flags",
				fmt.Sprintf(
					"--workflow-namespace %q is invalid: %s",
					r.global.workflowNamespace,
					strings.Join(problems, "; "),
				),
			)
		}
	}

	return nil
}

func configureKubernetesLogger(logger *slog.Logger) {
	handler := &kubernetesLogHandler{next: logger.With("component", "kubernetes").Handler()}
	klog.SetSlogLogger(slog.New(handler))
}

func loggerFor(r *rootState) (*slog.Logger, error) {
	if _, err := parseColorMode(r.global.color); err != nil {
		return nil, err
	}

	level, err := parseLogLevel(r.global.logLevel)
	if err != nil {
		return nil, err
	}

	handlerOptions := localLogHandlerOptions(level)
	switch logFormat(r.global.logFormat) {
	case logFormatText:
		return slog.New(slog.NewTextHandler(r.errWriter(), handlerOptions)), nil
	case logFormatJSON:
		return slog.New(slog.NewJSONHandler(r.errWriter(), handlerOptions)), nil
	default:
		return nil, domain.NewError(
			domain.ErrorValidation,
			"flags",
			fmt.Sprintf("unsupported log format %q", r.global.logFormat),
		)
	}
}

func (r *rootState) errWriter() io.Writer {
	if r.errOut != nil {
		return r.errOut
	}
	return r.options.ErrOut
}

func printerFor(r *rootState) output.Printer {
	return output.Printer{Writer: r.options.Out, Format: output.Format(r.global.output)}
}

func (r *rootState) context(parent context.Context) (context.Context, context.CancelFunc) {
	if r.global.timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, r.global.timeout)
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, domain.NewError(
			domain.ErrorValidation,
			"flags",
			fmt.Sprintf("unsupported log level %q", value),
		)
	}
}

func newVersionCommand(version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), version)
			return err
		},
	}
}

func newCompletionCommand(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:       "completion [bash|zsh|fish|powershell]",
		Short:     "Generate shell completion",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return root.GenBashCompletion(cmd.OutOrStdout())
			case "zsh":
				return root.GenZshCompletion(cmd.OutOrStdout())
			case "fish":
				return root.GenFishCompletion(cmd.OutOrStdout(), true)
			case "powershell":
				return root.GenPowerShellCompletion(cmd.OutOrStdout())
			default:
				return domain.NewError(
					domain.ErrorValidation,
					"completion",
					"supported shells are bash, zsh, fish, and powershell",
				)
			}
		},
	}
}

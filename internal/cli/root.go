package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

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
	kubeconfig          string
	kubeContext         string
	sessionNamespace    string
	controllerNamespace string
	workflowNamespace   string
	timeout             time.Duration
	retries             int
	retryBackoff        time.Duration
	helmTimeout         time.Duration
	output              string
	logFormat           string
	logLevel            string
	color               string
	streamToolLogs      bool
	wait                bool
	noCompress          bool
	assumeYes           bool
	toolImage           string
	mode                string
}

type executionMode string

const (
	executionModeAuto       executionMode = "auto"
	executionModeSession    executionMode = "session"
	executionModeController executionMode = "controller"
)

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
	clients                       *kube.Clients
	store                         kube.SessionStore
	planner                       *planner.Planner
	service                       *app.Service
	printer                       output.Printer
	logger                        *slog.Logger
	controllers                   *controller.Manager
	openEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	mode                          executionMode
	controllerStore               kube.SessionStore
	controllerKinds               []domain.ControllerKind
	controllerModeExplicit        bool
	waitForController             bool
	controllerWaiter              controllerSessionWaiter
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
		&state.global.controllerNamespace,
		"controller-namespace",
		"pvc-migrate-system",
		"Namespace where the controller is installed and its profile credentials are read",
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
	flags.BoolVar(
		&state.global.wait,
		"wait",
		true,
		"Wait for controller-backed workflows to finish",
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
	flags.StringVar(
		&state.global.mode,
		"mode",
		string(executionModeAuto),
		"Persistence mode: auto selects workflow CRDs when installed, controller uses CRDs, session uses ConfigMaps",
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
	// Cross-cluster workflows have an independent session and resource model.
	for _, parent := range command.Commands() {
		switch parent.Name() {
		case "copy":
			parent.AddCommand(state.newCrossClusterCopyCommand())
		case "reserve":
			parent.AddCommand(state.newCrossClusterReserveCommand())
		}
	}

	command.AddCommand(newCompletionCommand(command))

	return command
}

func bindDryRun(command *cobra.Command, target *bool) {
	command.Flags().
		BoolVar(target, "dry-run", true, "Validate and print the plan without mutations; use --dry-run=false to execute")
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

	requestedMode, err := parseExecutionMode(r.global.mode)
	if err != nil {
		return nil, err
	}

	controllerKinds := kube.AvailableControllerWorkflowKinds(clients.Discovery)
	hasCRDs := len(controllerKinds) > 0

	if requestedMode == executionModeController && !hasCRDs {
		return nil, domain.NewError(
			domain.ErrorPrecondition,
			"controller mode",
			"controller mode requires at least one migrate.sealos.io/v1alpha1 workflow CRD; install deploy/crd.yaml or use --mode=session",
		)
	}

	configMapStore := kube.NewConfigMapSessionStore(clients.Kubernetes)
	crdStore := kube.NewCRDSessionStore(clients.Runtime).
		WithLeaseClient(clients.Kubernetes).
		WithSupportedKinds(controllerKinds)

	var (
		store           kube.SessionStore = configMapStore
		controllerStore kube.SessionStore
	)

	selectedMode := executionModeSession
	switch {
	case requestedMode == executionModeController:
		store = crdStore
		controllerStore = crdStore
		selectedMode = executionModeController
	case requestedMode == executionModeAuto && hasCRDs:
		// Auto mode routes unsupported operations to the legacy ConfigMap
		// backend while eligible same-namespace workflows use the CRD.
		store = kube.NewSessionStoreRouter(configMapStore, crdStore).
			WithControllerKinds(controllerKinds)
		controllerStore = crdStore
		selectedMode = executionModeController
	}

	reserver := kube.NewReserver(clients.Kubernetes).
		WithLogger(logger.With("component", "reserver"))
	openEBSLVMSharedVolumeManager := kube.NewOpenEBSLVMSharedVolumeManager(
		clients.Kubernetes,
		clients.Dynamic,
	)

	if r.global.streamToolLogs {
		reserver = reserver.WithToolLogs(kube.ToolLogOptions{
			Writer:     r.errWriter(),
			Logger:     logger.With("component", "tool"),
			Structured: r.global.logFormat == string(logFormatJSON),
		})
	}

	service := app.NewService(
		clients.Kubernetes,
		store,
		reserver,
		copyengine.NewPVMigrate(),
		controllers,
		kube.NewSwitcher(clients.Kubernetes).WithLogger(logger.With("component", "switcher")),
		app.Config{
			KubeconfigPath:                r.global.kubeconfig,
			Context:                       r.global.kubeContext,
			Retries:                       r.global.retries,
			RetryBackoff:                  r.global.retryBackoff,
			HelmTimeout:                   r.global.helmTimeout,
			NoCompress:                    r.global.noCompress,
			StreamToolLogs:                r.global.streamToolLogs,
			StructuredLogs:                r.global.logFormat == string(logFormatJSON),
			Writer:                        r.errWriter(),
			Logger:                        logger.With("component", "migration"),
			ToolImageProber:               kube.NewToolImageProber(clients.Kubernetes),
			OpenEBSLVMSharedVolumeManager: openEBSLVMSharedVolumeManager,
		},
	)

	return &commandRuntime{
		clients: clients,
		store:   store,
		planner: planner.New(clients.Kubernetes, controllers).
			WithOpenEBSLVMSharedVolumeManager(openEBSLVMSharedVolumeManager).
			WithLogger(logger.With("component", "planner")),
		service:                       service,
		printer:                       output.Printer{Writer: r.options.Out, Format: format},
		logger:                        logger.With("component", "backup"),
		controllers:                   controllers,
		openEBSLVMSharedVolumeManager: openEBSLVMSharedVolumeManager,
		mode:                          selectedMode,
		controllerStore:               controllerStore,
		controllerKinds:               slices.Clone(controllerKinds),
		controllerModeExplicit:        requestedMode == executionModeController,
		waitForController:             r.global.wait,
		controllerWaiter:              kube.NewControllerSessionWaiter(clients.Dynamic),
	}, nil
}

func (r *rootState) validateGlobalFlags() error {
	if _, err := parseExecutionMode(r.global.mode); err != nil {
		return err
	}

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
	if problems := validation.IsDNS1123Label(r.global.controllerNamespace); len(problems) > 0 {
		return domain.NewError(
			domain.ErrorValidation,
			"flags",
			fmt.Sprintf(
				"--controller-namespace %q is invalid: %s",
				r.global.controllerNamespace,
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

func parseExecutionMode(value string) (executionMode, error) {
	mode := executionMode(strings.ToLower(strings.TrimSpace(value)))
	switch mode {
	case executionModeAuto, executionModeSession, executionModeController:
		return mode, nil
	default:
		return "", domain.NewError(
			domain.ErrorValidation,
			"flags",
			"--mode must be auto, session, or controller",
		)
	}
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

	handlerOptions := &slog.HandlerOptions{Level: level}
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
	case "warn", "warning":
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

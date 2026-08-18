package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
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
	kubeconfig       string
	kubeContext      string
	sessionNamespace string
	timeout          time.Duration
	retries          int
	retryBackoff     time.Duration
	helmTimeout      time.Duration
	output           string
	logFormat        string
	logLevel         string
	color            string
	streamToolLogs   bool
	noCompress       bool
	assumeYes        bool
	toolImage        string
}

type rootState struct {
	options Options
	global  globals
	errOut  io.Writer
}

type commandRuntime struct {
	clients     *kube.Clients
	store       kube.SessionStore
	planner     *planner.Planner
	service     *app.Service
	printer     output.Printer
	logger      *slog.Logger
	controllers *controller.Manager
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
		return state.global.logFormat != "json" && colorEnabled(state.global.color, options.ErrOut)
	})
	state.errOut = newLogOutputWriter(coloredErrOut, func() bool { return state.global.logFormat == "json" })
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
	flags.StringVar(&state.global.sessionNamespace, "session-namespace", "pvc-migrate-system", "Namespace for persistent migration sessions")
	flags.DurationVar(&state.global.timeout, "timeout", 30*time.Minute, "Operation timeout")
	flags.IntVar(&state.global.retries, "retries", 3, "Copy retry attempts")
	flags.DurationVar(&state.global.retryBackoff, "retry-backoff", 2*time.Second, "Initial copy retry backoff")
	flags.DurationVar(&state.global.helmTimeout, "helm-timeout", 10*time.Minute, "pv-migrate tool deployment timeout")
	flags.StringVarP(&state.global.output, "output", "o", "table", "Output format: table, json, yaml")
	flags.StringVar(&state.global.logFormat, "log-format", "text", "Log format: text, json")
	flags.StringVar(&state.global.logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	flags.StringVar(&state.global.color, "color", colorAuto, "Colorize text logs: auto, always, never")
	flags.BoolVar(&state.global.streamToolLogs, "stream-tool-logs", true, "Stream generated tool Pod logs to stderr")
	flags.BoolVar(&state.global.noCompress, "no-compress", false, "Disable rsync compression")
	flags.BoolVarP(&state.global.assumeYes, "yes", "y", false, "Approve workload pause and storage identity changes")
	flags.StringVar(&state.global.toolImage, "tool-image", kube.DefaultToolImage(options.ToolImageRepository, options.Version), "Tool image used by PVC reservation, copy, SSHD, and backup tools")

	command.AddCommand(
		state.newReserveCommand(),
		state.newCopyCommand(),
		state.newFinalSyncCommand(),
		state.newActivateCommand(),
		state.newMigrateCommand(false),
		state.newMigrateCommand(true),
		state.newRenameCommand(),
		state.newMoveCommand(),
		state.newBackupCommand(false),
		state.newLiveBackupCommand(),
		state.newBackupCommand(true),
		state.newSessionCommand(),
		newVersionCommand(options.Version),
	)
	command.AddCommand(newCompletionCommand(command))
	return command
}

func bindDryRun(command *cobra.Command, target *bool) {
	command.Flags().BoolVar(target, "dry-run", true, "Validate and print the plan without mutations; use --dry-run=false to execute")
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
		return nil, domain.NewError(domain.ErrorValidation, "flags", fmt.Sprintf("unsupported output format %q", r.global.output))
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
	controllers := controller.NewManager(clients.Kubernetes, clients.Dynamic, clients.Discovery).WithRESTConfig(clients.RESTConfig).WithLogger(logger.With("component", "controller"))
	store := kube.NewConfigMapSessionStore(clients.Kubernetes)
	reserver := kube.NewReserver(clients.Kubernetes).WithLogger(logger.With("component", "reserver"))
	openEBSLVMSharedVolumeManager := kube.NewOpenEBSLVMSharedVolumeManager(clients.Kubernetes, clients.Dynamic)
	if r.global.streamToolLogs {
		reserver = reserver.WithToolLogs(kube.ToolLogOptions{
			Writer:     r.errWriter(),
			Logger:     logger.With("component", "tool"),
			Structured: r.global.logFormat == "json",
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
			StructuredLogs:                r.global.logFormat == "json",
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
		service:     service,
		printer:     output.Printer{Writer: r.options.Out, Format: format},
		logger:      logger.With("component", "backup"),
		controllers: controllers,
	}, nil
}

func (r *rootState) validateGlobalFlags() error {
	switch {
	case r.global.retries < 1:
		return domain.NewError(domain.ErrorValidation, "flags", "--retries must be at least 1")
	case r.global.retryBackoff <= 0:
		return domain.NewError(domain.ErrorValidation, "flags", "--retry-backoff must be greater than 0")
	case r.global.helmTimeout <= 0:
		return domain.NewError(domain.ErrorValidation, "flags", "--helm-timeout must be greater than 0")
	}
	if problems := validation.IsDNS1123Label(r.global.sessionNamespace); len(problems) > 0 {
		return domain.NewError(domain.ErrorValidation, "flags", fmt.Sprintf("--session-namespace %q is invalid: %s", r.global.sessionNamespace, strings.Join(problems, "; ")))
	}
	return nil
}

func configureKubernetesLogger(logger *slog.Logger) {
	klog.SetSlogLogger(logger.With("component", "kubernetes"))
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
	switch r.global.logFormat {
	case "text":
		return slog.New(slog.NewTextHandler(r.errWriter(), handlerOptions)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(r.errWriter(), handlerOptions)), nil
	default:
		return nil, domain.NewError(domain.ErrorValidation, "flags", fmt.Sprintf("unsupported log format %q", r.global.logFormat))
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
		return 0, domain.NewError(domain.ErrorValidation, "flags", fmt.Sprintf("unsupported log level %q", value))
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
				return domain.NewError(domain.ErrorValidation, "completion", "supported shells are bash, zsh, fish, and powershell")
			}
		},
	}
}

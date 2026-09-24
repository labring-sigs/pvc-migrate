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
	kubeconfig       string
	kubeContext      string
	sessionNamespace string
	timeout          time.Duration
	copyTimeout      time.Duration
	retries          int
	retryBackoff     time.Duration
	helmTimeout      time.Duration
	output           string
	logFormat        string
	logLevel         string
	color            string
	streamToolLogs   bool
	compress         bool
	copyBandwidth    string
	assumeYes        bool
	toolImage        string
}

type logFormat string

const (
	logFormatText logFormat = "text"
	logFormatJSON logFormat = "json"
)

type rootState struct {
	options         Options
	global          globals
	errOut          io.Writer
	currentCommand  *cobra.Command
	timeoutExplicit bool
}

type commandRuntime struct {
	clients                       *kube.Clients
	planner                       *planner.Planner
	printer                       output.Printer
	logger                        *slog.Logger
	controllerLogger              *slog.Logger
	controllers                   *controller.Manager
	openEBSLVMSharedVolumeManager kube.OpenEBSLVMSharedVolumeManager
	controllerKinds               []domain.ControllerKind
	// controllerDiscoveryComplete distinguishes an actual empty discovery
	// result from test and injected runtimes that do not provide discovery
	// metadata. Session-backed commands remain usable when no workflow CRD is
	// installed; controller-backed commands still require an advertised kind.
	controllerDiscoveryComplete bool
	waitForController           bool
	podMigrationSessionStore    kube.WorkflowStore[*v1alpha1.PodMigration]
	podMigrationSessionExecutor *app.PodMigrationExecutor
	podMigrationStore           kube.WorkflowStore[*v1alpha1.PodMigration]
	podMigrationExecutor        *app.PodMigrationExecutor
	orphanCleaner               *app.OrphanCleaner
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
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			state.currentCommand = cmd
			state.timeoutExplicit = cmd.Flags().Changed("timeout")

			if err := state.validateCopyTimeout(cmd); err != nil {
				return err
			}

			if err := state.validateCopyBandwidth(cmd); err != nil {
				return err
			}

			if err := state.validateTransferTuningFlags(cmd); err != nil {
				return err
			}

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
	flags.DurationVar(
		&state.global.timeout,
		"timeout",
		30*time.Minute,
		"Whole-operation context timeout; data-transfer operations (copy, migrate, migrate-pod, backup, restore) run under 24h when this flag keeps its 30m default",
	)
	flags.DurationVar(
		&state.global.copyTimeout,
		"copy-timeout",
		0,
		"Per-attempt data transfer timeout for copy, migrate, and migrate-pod; 0 disables. Must be shorter than --timeout when both are set",
	)
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
		&state.global.compress,
		"compress",
		false,
		"Compress rsync transfer data; off by default — cross-cluster copy enables it unless set explicitly",
	)
	flags.StringVar(
		&state.global.copyBandwidth,
		"copy-bandwidth-limit",
		"",
		"Cap rsync transfer rate in KiB/s unless a K/M/G suffix is given (for example 10m); empty is unlimited",
	)
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
		state.newCRCommand(),
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

	podMigrationLocker := kube.NewCRDWorkflowLocker(clients.Kubernetes)

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
		// The flag is documented for copy, migrate, and migrate-pod; without
		// this field the direct-session executors silently ran unbounded
		// attempts while the copy command honored the bound.
		CopyTimeout:    r.global.copyTimeout,
		Compress:       r.global.compress,
		BandwidthLimit: r.global.copyBandwidth,
		StreamToolLogs: r.global.streamToolLogs,
		StructuredLogs: structuredLogs,
		Writer:         serviceWriter,
		Logger:         serviceLogger,
		// No TrustedToolImage pin on the session side: the plan records the
		// requested tool image and every executor stage must use exactly that
		// image. The controller pins its own trusted image separately.
	}
	// podMigrationExecutorConfig is the one place the pod-migration executor
	// dependencies are described; the cluster, session, and namespaced
	// executors differ only in store and locker.
	podMigrationExecutorConfig := func() app.PodMigrationExecutorConfig {
		return app.PodMigrationExecutorConfig{
			Storage: app.MigrationExecutorConfig{
				Transfer:        transferConfig,
				ToolImageProber: kube.NewToolImageProber(clients.Kubernetes),
				ProbeTimeout:    r.global.helmTimeout,
			},
			SharedVolumes: openEBSLVMSharedVolumeManager,
			Workloads:     controllers,
		}
	}

	// Session-side migrate-pod persists the concrete CRD type in a ConfigMap:
	// the CLI session path never creates workflow CRs, so the executor is
	// bound to the ConfigMap store with the matching locker. The session's
	// PodMigration carries its tenant namespace in metadata.namespace; leases
	// for local runs live in that namespace next to the workload.
	podMigrationSessionStore, err := kube.NewConfigMapWorkflowStore(
		clients.Kubernetes,
		r.global.sessionNamespace,
		func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
	)
	if err != nil {
		return nil, err
	}

	podMigrationSessionExecutor := app.NewPodMigrationExecutor(
		clients.Kubernetes,
		podMigrationSessionStore,
		kube.NewConfigMapWorkflowLocker(clients.Kubernetes),
		copyengine.NewPVMigrate(),
		podMigrationExecutorConfig(),
	)

	// Controller-submitted namespaced PodMigrations live as CRs in the tenant
	// namespace; lifecycle commands drive them through the namespaced executor.
	podMigrationStore, err := kube.NewCRDWorkflowStore(
		clients.Runtime,
		func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
	)
	if err != nil {
		return nil, err
	}

	podMigrationExecutor := app.NewPodMigrationExecutor(
		clients.Kubernetes,
		podMigrationStore,
		podMigrationLocker,
		copyengine.NewPVMigrate(),
		podMigrationExecutorConfig(),
	)

	orphanCleaner := app.NewOrphanCleaner(
		clients.Kubernetes,
		podMigrationLocker,
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
			WithSessionRecordNamespace(r.global.sessionNamespace).
			WithOpenEBSLVMSharedVolumeManager(openEBSLVMSharedVolumeManager).
			WithLogger(logger.With("component", "planner")),
		printer: output.Printer{Writer: r.options.Out, Format: format},
		logger:  logger.With("component", "backup"),
		controllerLogger: controller.NewControllerLogger(
			logger.With("component", "workflow-controller"),
		),
		controllers:                   controllers,
		openEBSLVMSharedVolumeManager: openEBSLVMSharedVolumeManager,
		controllerKinds:               slices.Clone(controllerKinds),
		controllerDiscoveryComplete:   true,
		waitForController:             true,
		podMigrationSessionStore:      podMigrationSessionStore,
		podMigrationSessionExecutor:   podMigrationSessionExecutor,
		podMigrationStore:             podMigrationStore,
		podMigrationExecutor:          podMigrationExecutor,
		orphanCleaner:                 orphanCleaner,
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

// dataTransferOperationTimeout bounds operations that move payload data.
// Datasets have no natural upper size, so the short metadata-operation
// default would kill legitimate large copies mid-transfer.
const dataTransferOperationTimeout = 24 * time.Hour

// dataTransferRootCommands are root operations that can execute payload data
// transfers (copy, migrate, migrate-pod, backup, restore, controller --once)
// or resume them from a checkpoint. The controller one-shot command must use
// the long bound because its inventory is not known until it lists the CRDs.
// Metadata-only operations (rename, move, reserve) keep the short default.
var dataTransferRootCommands = map[string]bool{
	"copy": true, "migrate": true, "migrate-pod": true,
	"backup": true, "restore": true, "controller": true,
}

// effectiveTimeout resolves the operation timeout: an explicit --timeout
// always wins; otherwise data-transferring operations default to the long
// transfer bound while everything else keeps the metadata default.
func (r *rootState) effectiveTimeout() time.Duration {
	if r.timeoutExplicit {
		return r.global.timeout
	}

	for c := r.currentCommand; c != nil; c = c.Parent() {
		if dataTransferRootCommands[c.Name()] {
			return dataTransferOperationTimeout
		}
	}

	return r.global.timeout
}

// transferCommandPath resolves the root transfer operation a command belongs
// to and the executed subcommand name (for example copy/create).
func transferCommandPath(cmd *cobra.Command) (root, sub string) {
	if cmd == nil {
		return "", ""
	}

	sub = cmd.Name()
	for c := cmd; c != nil; c = c.Parent() {
		if dataTransferRootCommands[c.Name()] {
			return c.Name(), sub
		}
	}

	return "", sub
}

// submissionMessage explains why a transfer-tuning flag cannot apply to a
// controller-submitted workflow: the submitting process never runs rsync.
const submissionMessage = "this command submits the workflow and the controller executes its transfers with its own flags; run the operation directly, or set the flag on the controller process"

// validateCopyBandwidth turns a malformed rate into an admission error
// instead of a failed tool job far into execution. The limit drives rsync
// transfers executed by this process; backup and restore move data through
// rclone instead, and create submissions hand execution to the controller,
// so the flag is refused there rather than accepted and silently ignored.
func (r *rootState) validateCopyBandwidth(cmd *cobra.Command) error {
	if r.global.copyBandwidth == "" {
		return nil
	}

	root, sub := transferCommandPath(cmd)

	if rejectsTransferTuning(root, sub) {
		return domain.NewError(
			domain.ErrorValidation,
			"flags",
			"--copy-bandwidth-limit "+transferTuningRejection(root),
		)
	}

	if root == "copy" || root == "migrate" || root == "migrate-pod" || root == "controller" {
		return copyengine.ValidateBandwidthLimit(r.global.copyBandwidth)
	}

	return nil
}

// validateTransferTuningFlags refuses an explicitly set --compress on paths
// that never consume it: rclone-based operations and controller submissions.
// Compression stays available wherever this process runs the rsync transfer.
func (r *rootState) validateTransferTuningFlags(cmd *cobra.Command) error {
	compress := cmd.Flags().Lookup("compress")
	if compress == nil || !cmd.Flags().Changed("compress") {
		return nil
	}

	root, sub := transferCommandPath(cmd)

	if rejectsTransferTuning(root, sub) {
		return domain.NewError(
			domain.ErrorValidation,
			"flags",
			"--compress "+transferTuningRejection(root),
		)
	}

	return nil
}

func rejectsTransferTuning(root, sub string) bool {
	if root == "backup" || root == "restore" {
		return true
	}

	return (root == "copy" || root == "migrate" || root == "migrate-pod") && sub == "create"
}

func transferTuningRejection(root string) string {
	if root == "backup" || root == "restore" {
		return "tunes rsync transfers; backup and restore use rclone and do not consume it"
	}

	return "tunes transfers executed by this process; " + submissionMessage
}

// validateCopyTimeout rejects a per-attempt copy bound that can never fire:
// when both bounds are positive and the copy bound is not shorter than the
// effective operation timeout, the copy timeout would be silently ignored.
// Commands without data transfers do not consume the copy timeout and are
// not validated.
func (r *rootState) validateCopyTimeout(cmd *cobra.Command) error {
	if r.global.copyTimeout <= 0 {
		return nil
	}

	for c := cmd; c != nil; c = c.Parent() {
		if !dataTransferRootCommands[c.Name()] {
			continue
		}

		effective := r.effectiveTimeout()
		if effective > 0 && r.global.copyTimeout >= effective {
			return domain.NewError(
				domain.ErrorValidation,
				"flags",
				fmt.Sprintf(
					"--copy-timeout %s must be shorter than the effective --timeout %s for %s",
					r.global.copyTimeout,
					effective,
					c.Name(),
				),
			)
		}

		break
	}

	return nil
}

func (r *rootState) context(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := r.effectiveTimeout()
	if timeout <= 0 {
		return context.WithCancel(parent)
	}

	return context.WithTimeout(parent, timeout)
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

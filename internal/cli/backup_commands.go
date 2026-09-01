package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type bucketFlags struct {
	id                   string
	namespace            string
	pvc                  string
	backend              string
	bucket               string
	name                 string
	prefix               string
	path                 string
	s3Provider           string
	endpoint             string
	region               string
	accessKey            string
	secretKey            string
	sessionToken         string
	credentialsSecret    string
	objectStoreProfile   string
	accessKeyKey         string
	secretKeyKey         string
	sessionTokenKey      string
	accessKeyExplicit    bool
	secretKeyExplicit    bool
	sessionTokenExplicit bool
	prefixExplicit       bool
	allowInsecure        bool
	serverEncryption     string
	sseKMSKeyID          string
}

type backupFlags struct {
	bucketFlags
	online                 bool
	openEBSLVMEnableShared bool
}

type restoreFlags struct {
	bucketFlags
	restore restoreBucketFlags
}

func (r *rootState) newBackupCommand() *cobra.Command {
	flags := &backupFlags{}

	var dryRun bool

	command := &cobra.Command{
		Use:   "backup",
		Short: "Back up PVC data to object storage; use --online for active consumers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return r.runBackupCommand(cmd, flags, dryRun)
		},
	}
	bindBackupFlags(command, flags)
	bindDryRun(command, &dryRun)
	command.AddCommand(
		r.newBackupPlanCommand(),
		r.newBackupStatusCommand(),
		r.newBackupResumeCommand(),
		r.newBackupAbortCommand(),
		r.newBackupCleanupCommand(),
	)

	return command
}

func (r *rootState) runBackupCommand(cmd *cobra.Command, flags *backupFlags, dryRun bool) error {
	if err := validateBackupMode(flags.online, flags.openEBSLVMEnableShared); err != nil {
		return reportPreSessionError(cmd, err)
	}

	return r.runBackupTransfer(
		cmd,
		&flags.bucketFlags,
		flags.online,
		flags.openEBSLVMEnableShared,
		dryRun,
	)
}

func (r *rootState) runBackupTransfer(
	cmd *cobra.Command,
	flags *bucketFlags,
	online, openEBSLVMEnableShared, dryRun bool,
) error {
	if err := validateBucketFlags(flags, "source-pvc"); err != nil {
		return reportPreSessionError(cmd, err)
	}

	if flags.id != "" {
		if err := domain.ValidateSessionID(flags.id); err != nil {
			return reportPreSessionError(cmd, err)
		}
	}
	flags.prefixExplicit = cmd.Flags().Changed("prefix")

	runtime, err := r.runtime()
	if err != nil {
		return reportRuntimeError(cmd, err)
	}

	ctx, cancel := r.context(cmd.Context())
	defer cancel()

	flags.accessKeyExplicit = cmd.Flags().Changed("access-key")
	flags.secretKeyExplicit = cmd.Flags().Changed("secret-key")

	flags.sessionTokenExplicit = cmd.Flags().Changed("session-token")
	controllerWorkflow := controllerWorkflowAvailable(runtime, domain.SessionTypeBackup)
	if flags.objectStoreProfile != "" {
		if !controllerWorkflow {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, domain.NewError(
				domain.ErrorPrecondition,
				"object-store profile",
				"--object-store-profile requires the Backup CRD and controller mode",
			))
		}
		if !objectStoreProfileAvailable(runtime) {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, domain.NewError(
				domain.ErrorPrecondition,
				"object-store profile",
				"ObjectStoreProfile CRD is not served by this cluster; install deploy/crd.yaml",
			))
		}
		if err := validateControllerProfileFlags(flags); err != nil {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
		}
	}
	if runtime.controllerModeExplicit && controllerWorkflow && flags.objectStoreProfile == "" {
		return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, domain.NewError(
			domain.ErrorPrecondition,
			"object-store profile",
			"controller Backup workflows require --object-store-profile",
		))
	}

	var store *objectstore.Store
	if flags.objectStoreProfile != "" {
		store, err = newControllerProfileStore(flags)
	} else {
		if err := loadS3Credentials(ctx, runtime.clients.Kubernetes, flags); err != nil {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
		}
		store, err = r.newObjectStore(ctx, flags)
	}
	if err != nil {
		return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
	}

	if !dryRun && flags.id == "" {
		flags.id, err = domain.NewSessionID(time.Now())
		if err != nil {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
		}
	}

	request := r.objectTransferRequest(runtime, flags, store, online, openEBSLVMEnableShared)
	if flags.objectStoreProfile != "" {
		request.SessionNamespace = r.controllerPlanSessionNamespace(
			runtime,
			domain.SessionTypeBackup,
			flags.namespace,
			flags.namespace,
		)
	}
	request.SkipManifestCheck = flags.objectStoreProfile != ""
	request.ToolImageProber = kube.NewToolImageProber(runtime.clients.Kubernetes)

	plan, err := backup.Preflight(ctx, runtime.clients.Kubernetes, request, false)
	if err != nil {
		return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
	}

	if dryRun {
		if err := printerFor(r).Print(plan); err != nil {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
		}

		return writeTransferDryRunGuidance(
			cmd.ErrOrStderr(),
			"backup",
			flags.namespace,
			flags.pvc,
			kubectlCommandPrefixForCommand(cmd),
		)
	}

	if err := r.confirm(ctx, cmd, flags.name); err != nil {
		return reportApprovalError(cmd, err)
	}

	var session *domain.Session

	if err := requireControllerWorkflow(runtime, domain.SessionTypeBackup); err != nil {
		return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
	}

	if controllerWorkflowAvailable(runtime, domain.SessionTypeBackup) {
		var submitErr error

		session, submitErr = backup.Submit(
			ctx,
			runtime.clients.Kubernetes,
			request,
			plan.PVCUID,
			plan.PVUID,
		)
		if submitErr != nil {
			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, submitErr)
		}

		if deferred, deferErr := deferControllerExecution(ctx, cmd, runtime, session); deferred {
			return deferErr
		}
	}

	if session == nil || session.Backend != kube.SessionBackendCRD {
		// Submit may have selected the ConfigMap backend in auto mode. Pass the
		// durable record into execution so it is resumed under the same identity
		// instead of being prepared and persisted a second time.
		request.BackupSession = session
		if err := backup.Run(ctx, runtime.clients.Kubernetes, request, false); err != nil {
			lookupCtx, lookupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			session, lookupErr := runtime.store.Get(lookupCtx, r.global.sessionNamespace, flags.id)

			lookupCancel()

			if lookupErr == nil {
				return reportSessionError(cmd, session, err)
			}

			return reportTransferError(cmd, "backup", flags.namespace, flags.pvc, err)
		}
	}

	if session != nil && session.Backend == kube.SessionBackendCRD {
		return nil
	}

	return r.printObjectTransferResult(cmd, runtime, flags, "backup", false, online, plan, store)
}

func (r *rootState) backupResumeRequest(runtime *commandRuntime) backup.Request {
	return backup.Request{
		HelmTimeout:        r.global.helmTimeout,
		KubeconfigPath:     r.global.kubeconfig,
		KubeContext:        r.global.kubeContext,
		StreamToolLogs:     r.global.streamToolLogs,
		StructuredLogs:     r.global.logFormat == string(logFormatJSON),
		Writer:             r.errWriter(),
		Logger:             runtime.logger,
		ToolImageProber:    kube.NewToolImageProber(runtime.clients.Kubernetes),
		SessionStore:       runtime.store,
		SessionNamespace:   r.global.sessionNamespace,
		OpenEBSLVMManager:  runtime.openEBSLVMSharedVolumeManager,
		ObjectStoreFactory: r.options.objectStoreFactory,
	}
}

func (r *rootState) newBackupPlanCommand() *cobra.Command {
	flags := &backupFlags{}
	command := r.newObjectTransferPlanCommand(
		"backup plan",
		"source-pvc",
		false,
		false,
		&flags.bucketFlags,
		func(request *backup.Request) error {
			request.Online = flags.online
			request.OpenEBSLVMEnableShared = flags.openEBSLVMEnableShared
			return validateBackupMode(flags.online, flags.openEBSLVMEnableShared)
		},
	)
	bindBackupFlags(command, flags)

	return command
}

func (r *rootState) printObjectTransferResult(
	cmd *cobra.Command,
	runtime *commandRuntime,
	flags *bucketFlags,
	use string,
	restore, online bool,
	plan *backup.Plan,
	store *objectstore.Store,
) error {
	var completedSession *domain.Session
	if !restore {
		lookupCtx, lookupCancel := context.WithTimeout(context.Background(), 5*time.Second)

		var err error

		completedSession, err = runtime.store.Get(
			lookupCtx,
			r.global.sessionNamespace,
			flags.id,
		)

		lookupCancel()

		if err != nil {
			return reportSessionLookupError(cmd, r.global.sessionNamespace, flags.id, err)
		}
	}

	if output.Format(r.global.output) != output.Table {
		mode := backup.ModeOffline
		if online {
			mode = backup.ModeOnline
		}

		if restore {
			mode = backup.ModeRestore
		}

		sessionID := flags.id

		operationID := ""
		if restore {
			sessionID = ""
			operationID = flags.id
		}

		if err := printerFor(r).Print(&backup.Result{
			Operation:   use,
			OperationID: operationID,
			SessionID:   sessionID,
			Namespace:   flags.namespace,
			PVC:         flags.pvc,
			Path:        plan.Path,
			Name:        flags.name,
			Destination: store.Destination(),
			Mode:        mode,
			Status:      "completed",
		}); err != nil {
			return err
		}

		_, err := fmt.Fprintf(
			cmd.ErrOrStderr(),
			"%s completed. Verify the backup or restore result before the next workload change.\n",
			use,
		)
		if err != nil || completedSession == nil {
			return err
		}

		return writeSessionGuidance(
			cmd.ErrOrStderr(),
			completedSession,
			guidancePrefixesForCommand(cmd, completedSession.Spec.SessionNamespace),
		)
	}

	identityLabel, identity := transferResultIdentity(restore, flags.id)

	_, err := fmt.Fprintf(
		cmd.OutOrStdout(),
		"%s completed: %s/%s name=%s %s=%s\n",
		use,
		flags.namespace,
		flags.pvc,
		flags.name,
		identityLabel,
		identity,
	)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(
		cmd.ErrOrStderr(),
		"%s completed. Verify the backup or restore result before the next workload change.\n",
		use,
	)

	if err != nil || completedSession == nil {
		return err
	}

	return writeSessionGuidance(
		cmd.ErrOrStderr(),
		completedSession,
		guidancePrefixesForCommand(cmd, completedSession.Spec.SessionNamespace),
	)
}

// newObjectTransferPlanCommand contains the common object-store preflight
// mechanics; each workflow binds its own flags and request-specific options.
func (r *rootState) newObjectTransferPlanCommand(
	operation, pvcFlag string,
	online, restore bool,
	flags *bucketFlags,
	prepare func(*backup.Request) error,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "plan",
		Short: "Validate object-storage access and PVC state without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags.prefixExplicit = cmd.Flags().Changed("prefix")
			if err := validateBucketFlags(flags, pvcFlag); err != nil {
				return reportPreSessionError(cmd, err)
			}

			runtime, err := r.runtime()
			if err != nil {
				return reportRuntimeError(cmd, err)
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			flags.accessKeyExplicit = cmd.Flags().Changed("access-key")
			flags.secretKeyExplicit = cmd.Flags().Changed("secret-key")

			flags.sessionTokenExplicit = cmd.Flags().Changed("session-token")
			sessionType := domain.SessionTypeBackup
			if restore {
				sessionType = domain.SessionTypeRestore
			}
			controllerWorkflow := controllerWorkflowAvailable(runtime, sessionType)
			if flags.objectStoreProfile != "" {
				if !controllerWorkflow {
					return reportTransferError(cmd, operation, flags.namespace, flags.pvc, domain.NewError(
						domain.ErrorPrecondition,
						"object-store profile",
						"--object-store-profile requires the matching workflow CRD and controller mode",
					))
				}
				if !objectStoreProfileAvailable(runtime) {
					return reportTransferError(cmd, operation, flags.namespace, flags.pvc, domain.NewError(
						domain.ErrorPrecondition,
						"object-store profile",
						"ObjectStoreProfile CRD is not served by this cluster; install deploy/crd.yaml",
					))
				}
				if err := validateControllerProfileFlags(flags); err != nil {
					return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
				}
			}
			if runtime.controllerModeExplicit && controllerWorkflow && flags.objectStoreProfile == "" {
				return reportTransferError(cmd, operation, flags.namespace, flags.pvc, domain.NewError(
					domain.ErrorPrecondition,
					"object-store profile",
					"controller object-storage workflows require --object-store-profile",
				))
			}

			var store *objectstore.Store
			if flags.objectStoreProfile != "" {
				store, err = newControllerProfileStore(flags)
			} else {
				if err := loadS3Credentials(ctx, runtime.clients.Kubernetes, flags); err != nil {
					return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
				}
				store, err = r.newObjectStore(ctx, flags)
			}
			if err != nil {
				return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
			}

			request := r.objectTransferRequest(runtime, flags, store, online, false)
			if flags.objectStoreProfile != "" {
				request.SessionNamespace = r.controllerPlanSessionNamespace(
					runtime,
					sessionType,
					flags.namespace,
					flags.namespace,
				)
			}
			request.SkipManifestCheck = flags.objectStoreProfile != ""
			if prepare != nil {
				if err := prepare(&request); err != nil {
					return reportPreSessionError(cmd, err)
				}
			}

			plan, err := backup.Preflight(ctx, runtime.clients.Kubernetes, request, restore)
			if err != nil {
				return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
			}

			if err := printerFor(r).Print(plan); err != nil {
				return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
			}

			return writeTransferDryRunGuidance(
				cmd.ErrOrStderr(),
				operation,
				flags.namespace,
				flags.pvc,
				kubectlCommandPrefixForCommand(cmd),
			)
		},
	}

	return command
}

func (r *rootState) objectTransferRequest(
	runtime *commandRuntime,
	flags *bucketFlags,
	store *objectstore.Store,
	online bool,
	openEBSLVMEnableShared bool,
) backup.Request {
	return backup.Request{
		ID:                     flags.id,
		ToolImage:              r.global.toolImage,
		Namespace:              flags.namespace,
		PVCName:                flags.pvc,
		Path:                   flags.path,
		Online:                 online,
		HelmTimeout:            r.global.helmTimeout,
		KubeconfigPath:         r.global.kubeconfig,
		KubeContext:            r.global.kubeContext,
		StreamToolLogs:         r.global.streamToolLogs,
		StructuredLogs:         r.global.logFormat == string(logFormatJSON),
		Store:                  store,
		Writer:                 r.errWriter(),
		Logger:                 runtime.logger,
		SessionStore:           runtime.store,
		SessionNamespace:       r.global.sessionNamespace,
		ObjectStoreProfile:     flags.objectStoreProfile,
		OpenEBSLVMEnableShared: openEBSLVMEnableShared,
		OpenEBSLVMManager:      runtime.openEBSLVMSharedVolumeManager,
	}
}

func bindObjectStoreFlags(command *cobra.Command, flags *bucketFlags, pvcFlag, idHelp string) {
	command.Flags().StringVar(&flags.id, "id", "", idHelp)
	command.Flags().StringVarP(&flags.namespace, "namespace", "n", "default", "PVC namespace")
	command.Flags().StringVar(&flags.pvc, pvcFlag, "", "PVC name")
	command.Flags().StringVar(
		&flags.backend,
		"backend",
		string(domain.ObjectStoreBackendS3),
		"S3-compatible object backend",
	)
	command.Flags().StringVar(&flags.bucket, "bucket", "", "Bucket or container name")
	command.Flags().StringVar(&flags.name, "name", "", "Backup identity inside the bucket")
	command.Flags().StringVar(&flags.prefix, "prefix", "pv-migrate", "Global bucket prefix")
	command.Flags().StringVar(&flags.path, "path", "", "PVC subdirectory")
	command.Flags().StringVar(&flags.s3Provider, "s3-provider", "", "rclone S3 provider")
	command.Flags().StringVar(&flags.endpoint, "endpoint", "", "S3 endpoint")
	command.Flags().StringVar(&flags.region, "region", "", "S3 region")
	command.Flags().
		StringVar(&flags.accessKey, "access-key", os.Getenv("AWS_ACCESS_KEY_ID"), "S3 access key; defaults to AWS_ACCESS_KEY_ID")
	command.Flags().
		StringVar(&flags.secretKey, "secret-key", os.Getenv("AWS_SECRET_ACCESS_KEY"), "S3 secret key; defaults to AWS_SECRET_ACCESS_KEY")
	command.Flags().
		StringVar(&flags.sessionToken, "session-token", os.Getenv("AWS_SESSION_TOKEN"), "S3 session token; defaults to AWS_SESSION_TOKEN")
	command.Flags().
		StringVar(&flags.credentialsSecret, "credentials-secret", "", "Kubernetes Secret containing S3 credentials")
	command.Flags().StringVar(&flags.objectStoreProfile, "object-store-profile", "", "Administrator-managed ObjectStoreProfile for controller mode")
	command.Flags().
		StringVar(&flags.accessKeyKey, "access-key-key", "accessKey", "Key in --credentials-secret containing the access key")
	command.Flags().
		StringVar(&flags.secretKeyKey, "secret-key-key", "secretKey", "Key in --credentials-secret containing the secret key")
	command.Flags().
		StringVar(&flags.sessionTokenKey, "session-token-key", "sessionToken", "Key in --credentials-secret containing the session token")
	command.Flags().
		BoolVar(&flags.allowInsecure, "allow-insecure-endpoint", false, "Allow an HTTP S3 endpoint; use HTTPS for production")
	command.Flags().
		StringVar(&flags.serverEncryption, "s3-server-side-encryption", "", "S3 server-side encryption: AES256 or aws:kms")
	command.Flags().
		StringVar(&flags.sseKMSKeyID, "s3-sse-kms-key-id", "", "S3 KMS key ID when using aws:kms")
}

func bindBackupFlags(command *cobra.Command, flags *backupFlags) {
	bindObjectStoreFlags(
		command,
		&flags.bucketFlags,
		"source-pvc",
		"Backup Session ID; generated when omitted during execution",
	)
	command.Flags().
		BoolVar(&flags.online, "online", false, "Copy from an active source without pausing consumers")
	command.Flags().
		BoolVar(&flags.openEBSLVMEnableShared, "openebs-lvm-enable-shared", false, "Temporarily enable OpenEBS LVM shared mounts for an active source PVC")
}

func validateBackupMode(online, openEBSLVMEnableShared bool) error {
	if openEBSLVMEnableShared && !online {
		return domain.NewError(
			domain.ErrorValidation,
			"backup flags",
			"--openebs-lvm-enable-shared requires --online",
		)
	}

	return nil
}

func bindRestoreFlags(command *cobra.Command, flags *restoreFlags) {
	bindObjectStoreFlags(
		command,
		&flags.bucketFlags,
		"destination-pvc",
		"Restore session ID; generated when omitted during execution",
	)
	bindRestoreBucketFlags(command, &flags.restore)
}

func transferResultIdentity(restore bool, id string) (string, string) {
	label := "session"
	if restore {
		label = "operation-id"
	}

	if id == "" {
		id = "-"
	}

	return label, id
}

func validateBucketFlags(flags *bucketFlags, pvcFlag string) error {
	if flags == nil {
		return domain.NewError(domain.ErrorValidation, "backup/restore", "object-store flags are required")
	}

	if flags.pvc == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"backup/restore",
			"--"+pvcFlag+" is required",
		)
	}

	if flags.name == "" {
		return domain.NewError(domain.ErrorValidation, "backup/restore", "--name is required")
	}

	// Controller workflows resolve all connection and bucket settings from the
	// administrator-owned ObjectStoreProfile. Only the PVC and recovery-point
	// name are tenant inputs in this mode.
	if flags.objectStoreProfile != "" {
		if flags.backend != "" && flags.backend != string(domain.ObjectStoreBackendS3) {
			return domain.NewError(
				domain.ErrorValidation,
				"backup/restore",
				fmt.Sprintf("unsupported backend %q", flags.backend),
			)
		}

		return nil
	}

	if flags.backend == "" || flags.bucket == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"backup/restore",
			"--backend and --bucket are required",
		)
	}

	if flags.backend != string(domain.ObjectStoreBackendS3) {
		return domain.NewError(
			domain.ErrorValidation,
			"backup/restore",
			fmt.Sprintf("unsupported backend %q", flags.backend),
		)
	}

	return nil
}

func (r *rootState) newObjectStore(
	ctx context.Context,
	flags *bucketFlags,
) (*objectstore.Store, error) {
	cfg := objectstore.Config{
		Bucket:                flags.bucket,
		Prefix:                flags.prefix,
		Name:                  flags.name,
		Provider:              flags.s3Provider,
		Endpoint:              flags.endpoint,
		Region:                flags.region,
		AccessKey:             flags.accessKey,
		SecretKey:             flags.secretKey,
		SessionToken:          flags.sessionToken,
		AllowInsecureEndpoint: flags.allowInsecure,
		ForcePathStyle:        flags.endpoint != "",
		ServerSideEncryption:  flags.serverEncryption,
		SSEKMSKeyID:           flags.sseKMSKeyID,
	}
	if r.options.objectStoreFactory != nil {
		return r.options.objectStoreFactory(ctx, cfg)
	}

	return objectstore.New(ctx, cfg)
}

func newControllerProfileStore(flags *bucketFlags) (*objectstore.Store, error) {
	if flags == nil || flags.objectStoreProfile == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"object-store profile",
			"--object-store-profile is required",
		)
	}

	return objectstore.NewConfigOnly(objectstore.Config{
		// Config-only validation still needs syntactically valid placeholders.
		// The controller replaces these values with the profile's scoped bucket
		// and namespace prefix before any object-store call is made.
		Bucket: "profile",
		Prefix: "",
		Name:   flags.name,
	})
}

func validateControllerProfileFlags(flags *bucketFlags) error {
	if flags == nil || flags.objectStoreProfile == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"object-store profile",
			"--object-store-profile is required",
		)
	}

	if flags.bucket != "" || flags.s3Provider != "" || flags.endpoint != "" || flags.region != "" || flags.prefixExplicit ||
		flags.credentialsSecret != "" || flags.allowInsecure ||
		flags.accessKeyExplicit || flags.secretKeyExplicit || flags.sessionTokenExplicit ||
		flags.serverEncryption != "" || flags.sseKMSKeyID != "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"object-store profile",
			"controller workflows take provider, endpoint, region, and credentials from ObjectStoreProfile",
		)
	}

	return nil
}

func loadS3Credentials(ctx context.Context, client kubernetes.Interface, flags *bucketFlags) error {
	if flags.credentialsSecret == "" {
		return nil
	}

	secret, err := client.CoreV1().
		Secrets(flags.namespace).
		Get(ctx, flags.credentialsSecret, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"S3 credentials",
			"read credentials Secret",
			err,
		)
	}

	read := func(key string) string {
		if value := secret.Data[key]; len(value) > 0 {
			return string(value)
		}
		return secret.StringData[key]
	}
	if !flags.accessKeyExplicit {
		if value := read(flags.accessKeyKey); value != "" {
			flags.accessKey = value
		}
	}

	if !flags.secretKeyExplicit {
		if value := read(flags.secretKeyKey); value != "" {
			flags.secretKey = value
		}
	}

	if !flags.sessionTokenExplicit {
		if value := read(flags.sessionTokenKey); value != "" {
			flags.sessionToken = value
		}
	}

	return nil
}

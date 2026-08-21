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
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type bucketFlags struct {
	id                     string
	namespace              string
	pvc                    string
	backend                string
	bucket                 string
	name                   string
	prefix                 string
	path                   string
	s3Provider             string
	endpoint               string
	region                 string
	accessKey              string
	secretKey              string
	sessionToken           string
	credentialsSecret      string
	accessKeyKey           string
	secretKeyKey           string
	sessionTokenKey        string
	accessKeyExplicit      bool
	secretKeyExplicit      bool
	sessionTokenExplicit   bool
	allowInsecure          bool
	serverEncryption       string
	sseKMSKeyID            string
	online                 bool
	allowMounted           bool
	deleteExtraneous       bool
	openEBSLVMEnableShared bool
}

func (r *rootState) newBackupCommand(restore bool) *cobra.Command {
	return r.newObjectTransferCommand(restore, false)
}

func (r *rootState) newLiveBackupCommand() *cobra.Command {
	return r.newObjectTransferCommand(false, true)
}

func (r *rootState) newObjectTransferCommand(restore, forceOnline bool) *cobra.Command {
	flags := &bucketFlags{}

	var dryRun bool

	use := "backup"
	short := "Back up PVC data to object storage"

	pvcFlag := "source-pvc"
	if forceOnline {
		use = "live-backup"
		short = "Back up PVC data while source consumers remain active"
	} else if restore {
		use = "restore"
		short = "Restore object-storage backup data into a PVC"
		pvcFlag = "destination-pvc"
	}

	command := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateBucketFlags(flags, pvcFlag); err != nil {
				return reportPreSessionError(cmd, err)
			}
			if !restore && flags.id != "" {
				if err := domain.ValidateSessionID(flags.id); err != nil {
					return reportPreSessionError(cmd, err)
				}
			}

			online := !restore && (forceOnline || flags.online)

			runtime, err := r.runtime()
			if err != nil {
				return reportRuntimeError(cmd, err)
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			flags.accessKeyExplicit = cmd.Flags().Changed("access-key")
			flags.secretKeyExplicit = cmd.Flags().Changed("secret-key")

			flags.sessionTokenExplicit = cmd.Flags().Changed("session-token")
			if err := loadS3Credentials(ctx, runtime.clients.Kubernetes, flags); err != nil {
				return reportTransferError(cmd, use, flags.namespace, flags.pvc, err)
			}

			store, err := r.newObjectStore(ctx, flags)
			if err != nil {
				return reportTransferError(cmd, use, flags.namespace, flags.pvc, err)
			}
			if !restore && !dryRun && flags.id == "" {
				flags.id, err = domain.NewSessionID(time.Now())
				if err != nil {
					return reportTransferError(cmd, use, flags.namespace, flags.pvc, err)
				}
			}

			request := backup.Request{
				ID:                     flags.id,
				ToolImage:              r.global.toolImage,
				Namespace:              flags.namespace,
				PVCName:                flags.pvc,
				Path:                   flags.path,
				Online:                 online,
				AllowMounted:           flags.allowMounted,
				DeleteExtraneousFiles:  flags.deleteExtraneous,
				HelmTimeout:            r.global.helmTimeout,
				KubeconfigPath:         r.global.kubeconfig,
				KubeContext:            r.global.kubeContext,
				StreamToolLogs:         r.global.streamToolLogs,
				StructuredLogs:         r.global.logFormat == "json",
				Store:                  store,
				Writer:                 r.errWriter(),
				Logger:                 runtime.logger,
				ToolImageProber:        kube.NewToolImageProber(runtime.clients.Kubernetes),
				SessionStore:           runtime.store,
				SessionNamespace:       r.global.sessionNamespace,
				OpenEBSLVMEnableShared: flags.openEBSLVMEnableShared,
				OpenEBSLVMManager:      runtime.openEBSLVMSharedVolumeManager,
			}

			plan, err := backup.Preflight(ctx, runtime.clients.Kubernetes, request, restore)
			if err != nil {
				return reportTransferError(cmd, use, flags.namespace, flags.pvc, err)
			}

			if dryRun {
				if err := printerFor(r).Print(plan); err != nil {
					return reportTransferError(cmd, use, flags.namespace, flags.pvc, err)
				}

				return writeTransferDryRunGuidance(
					cmd.ErrOrStderr(),
					use,
					flags.namespace,
					flags.pvc,
					kubectlCommandPrefixForCommand(cmd),
				)
			}

			if err := r.confirm(ctx, cmd, flags.name); err != nil {
				return reportApprovalError(cmd, err)
			}

			err = backup.Run(ctx, runtime.clients.Kubernetes, request, restore)
			if err != nil {
				if !restore {
					lookupCtx, lookupCancel := context.WithTimeout(context.Background(), 5*time.Second)
					session, lookupErr := runtime.store.Get(lookupCtx, r.global.sessionNamespace, flags.id)
					lookupCancel()
					if lookupErr == nil {
						return reportSessionError(cmd, session, err)
					}
				}
				return reportTransferError(cmd, use, flags.namespace, flags.pvc, err)
			}

			var completedSession *domain.Session
			if !restore {
				lookupCtx, lookupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				completedSession, err = runtime.store.Get(lookupCtx, r.global.sessionNamespace, flags.id)
				lookupCancel()
				if err != nil {
					return reportSessionLookupError(cmd, r.global.sessionNamespace, flags.id, err)
				}
			}

			if r.global.output != "table" {
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
			_, err = fmt.Fprintf(
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
		},
	}
	bindBucketFlags(command, flags, restore, !restore && !forceOnline)
	bindDryRun(command, &dryRun)
	command.AddCommand(r.newBackupPlanCommand(restore, forceOnline))

	return command
}

func (r *rootState) newBackupPlanCommand(restore, forceOnline bool) *cobra.Command {
	flags := &bucketFlags{}
	use := "plan"
	pvcFlag := "source-pvc"

	operation := "backup plan"
	if forceOnline {
		operation = "live-backup plan"
	} else if restore {
		pvcFlag = "destination-pvc"
		operation = "restore plan"
	}

	command := &cobra.Command{
		Use:   use,
		Short: "Validate object-storage access and PVC state without mutations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateBucketFlags(flags, pvcFlag); err != nil {
				return reportPreSessionError(cmd, err)
			}

			online := !restore && (forceOnline || flags.online)

			runtime, err := r.runtime()
			if err != nil {
				return reportRuntimeError(cmd, err)
			}

			ctx, cancel := r.context(cmd.Context())
			defer cancel()

			flags.accessKeyExplicit = cmd.Flags().Changed("access-key")
			flags.secretKeyExplicit = cmd.Flags().Changed("secret-key")

			flags.sessionTokenExplicit = cmd.Flags().Changed("session-token")
			if err := loadS3Credentials(ctx, runtime.clients.Kubernetes, flags); err != nil {
				return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
			}

			store, err := r.newObjectStore(ctx, flags)
			if err != nil {
				return reportTransferError(cmd, operation, flags.namespace, flags.pvc, err)
			}

			request := backup.Request{
				ID:                     flags.id,
				ToolImage:              r.global.toolImage,
				Namespace:              flags.namespace,
				PVCName:                flags.pvc,
				Path:                   flags.path,
				Online:                 online,
				AllowMounted:           flags.allowMounted,
				DeleteExtraneousFiles:  flags.deleteExtraneous,
				HelmTimeout:            r.global.helmTimeout,
				KubeconfigPath:         r.global.kubeconfig,
				KubeContext:            r.global.kubeContext,
				StreamToolLogs:         r.global.streamToolLogs,
				StructuredLogs:         r.global.logFormat == "json",
				Store:                  store,
				Writer:                 r.errWriter(),
				Logger:                 runtime.logger,
				SessionStore:           runtime.store,
				SessionNamespace:       r.global.sessionNamespace,
				OpenEBSLVMEnableShared: flags.openEBSLVMEnableShared,
				OpenEBSLVMManager:      runtime.openEBSLVMSharedVolumeManager,
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
	bindBucketFlags(command, flags, restore, !restore && !forceOnline)

	return command
}

func bindBucketFlags(command *cobra.Command, flags *bucketFlags, restore, includeOnline bool) {
	pvcFlag := "source-pvc"
	if restore {
		pvcFlag = "destination-pvc"
	}

	idHelp := "Backup Session ID; generated when omitted during execution"
	if restore {
		idHelp = "Restore attempt ID used for logs and temporary tool resources; no Session is created"
	}
	command.Flags().StringVar(&flags.id, "id", "", idHelp)
	command.Flags().StringVarP(&flags.namespace, "namespace", "n", "default", "PVC namespace")
	command.Flags().StringVar(&flags.pvc, pvcFlag, "", "PVC name")
	command.Flags().StringVar(&flags.backend, "backend", "s3", "S3-compatible object backend")
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

	if includeOnline {
		command.Flags().
			BoolVar(&flags.online, "online", false, "Run a best-effort online backup while Pods keep using the source PVC")
	}

	if !restore {
		command.Flags().BoolVar(&flags.openEBSLVMEnableShared, "openebs-lvm-enable-shared", false, "Temporarily enable OpenEBS LVM shared mounts for an active source PVC")
	}

	if restore {
		command.Flags().
			BoolVar(&flags.allowMounted, "allow-mounted", false, "Allow restore while the destination PVC has Pod consumers")
		command.Flags().
			BoolVar(&flags.deleteExtraneous, "delete-extraneous", false, "Delete destination files absent from the backup (destructive)")
	}
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

	if flags.backend == "" || flags.bucket == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"backup/restore",
			"--backend and --bucket are required",
		)
	}

	if flags.backend != "s3" {
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

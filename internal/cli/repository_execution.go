package cli

import (
	"context"
	"errors"
	"fmt"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/spf13/cobra"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type cliRepositoryResolver struct {
	clients *kube.Clients
	load    backup.RepositoryLoader
	factory func(context.Context, objectstore.Config) (*objectstore.Store, error)
}

func (r cliRepositoryResolver) Resolve(
	ctx context.Context,
	key crclient.ObjectKey,
	name string,
) (backup.S3RepositoryStore, *v1alpha1.BackupRepositoryBindingStatus, error) {
	return backup.NewS3RepositoryResolver(r.load, r.clients.Kubernetes, r.factory).
		Resolve(ctx, key, name)
}

func (r *rootState) repositoryResolver(runtime *commandRuntime) backup.S3RepositoryResolver {
	load := kube.NewConfigMapRepositoryStore(runtime.clients.Kubernetes).Load

	return cliRepositoryResolver{
		clients: runtime.clients,
		load:    load,
		factory: r.options.objectStoreFactory,
	}
}

func (r *rootState) repositoryTools(runtime *commandRuntime) backup.ToolRuntime {
	return backup.ToolRuntime{
		HelmTimeout:     r.global.helmTimeout,
		KubeconfigPath:  r.global.kubeconfig,
		KubeContext:     r.global.kubeContext,
		StreamToolLogs:  r.global.streamToolLogs,
		StructuredLogs:  r.global.logFormat == string(logFormatJSON),
		Writer:          r.errWriter(),
		Logger:          runtime.logger,
		ToolImageProber: kube.NewToolImageProber(runtime.clients.Kubernetes),
	}
}

func (r *rootState) backupExecutor(
	runtime *commandRuntime,
	namespace string,
	store kube.WorkflowStore[*v1alpha1.Backup],
	backend string,
) *backup.BackupExecutor {
	config := backup.BackupExecutorConfig{
		Tools:               r.repositoryTools(runtime),
		Repository:          r.repositoryResolver(runtime),
		SharedVolumeManager: runtime.openEBSLVMSharedVolumeManager,
	}
	config.RepositoryResources = kube.NewConfigMapRepositoryStore(runtime.clients.Kubernetes)

	return backup.NewBackupExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		namespace,
		config,
	)
}

func (r *rootState) restoreExecutor(
	runtime *commandRuntime,
	namespace string,
	store kube.WorkflowStore[*v1alpha1.Restore],
	backend string,
) *backup.RestoreExecutor {
	config := backup.RestoreExecutorConfig{
		Tools:      r.repositoryTools(runtime),
		Repository: r.repositoryResolver(runtime),
	}
	config.RepositoryResources = kube.NewConfigMapRepositoryStore(runtime.clients.Kubernetes)

	return backup.NewRestoreExecutor(
		runtime.clients.Kubernetes,
		store,
		cliWorkflowLockerForBackend(runtime, backend),
		namespace,
		config,
	)
}

func persistInlineRepository(
	ctx context.Context,
	runtime *commandRuntime,
	repository *v1alpha1.BackupRepository,
	owner v1alpha1.ObjectReference,
	data map[string][]byte,
) error {
	store := kube.NewConfigMapRepositoryStore(runtime.clients.Kubernetes)
	if err := store.Create(ctx, repository, owner); err != nil {
		return err
	}

	return store.CreateCredentials(ctx, repository.Namespace, owner, data)
}

func reportRepositoryWorkflowError(
	cmd *cobra.Command,
	operation, namespace, name string,
	phase v1alpha1.WorkflowPhase,
	cause error,
) error {
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"%s %s/%s stopped in phase %s. Inspect %s status %s before resume or cleanup.\n",
		operation,
		namespace,
		name,
		phase,
		operation,
		name,
	)

	return errors.Join(cause, err)
}

func printRepositoryWorkflowResult(
	cmd *cobra.Command,
	runtime *commandRuntime,
	object crclient.Object,
	operation, storageNamespace string,
) error {
	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	prefix := guidancePrefixesForCommand(cmd, storageNamespace).pvcMigrate
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Inspect: %s %s status %s\nFinalize after verifying the result: %s --yes %s cleanup %s --finalize --delete-session --dry-run=false\n",
		prefix,
		operation,
		shellQuote(object.GetName()),
		prefix,
		operation,
		shellQuote(object.GetName()),
	)

	return err
}

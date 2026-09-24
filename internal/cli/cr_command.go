package cli

import (
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

// newCRCommand builds the controller-workflow command group. Every verb under
// cr addresses API-server workflow CRs reconciled by the controller; the
// session command families never touch these records, and cr commands never
// touch ConfigMap sessions.
func (r *rootState) newCRCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "cr",
		Short: "Create and drive controller-owned workflow CRs",
		Long: "Operate on workflow custom resources directly: create submits a CR for " +
			"controller reconciliation, while status, watch, and the lifecycle verbs " +
			"address an existing CR by name and namespace (-n). These commands never " +
			"read or write ConfigMap migration sessions.",
	}

	command.AddCommand(
		r.newCRFamilyCommand(
			"migrate-pod",
			"PodMigration workflows",
			r.newCRPodMigrationCreateCommand(),
			false,
			r.newPodMigrationStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindPodMigration,
				resource: domain.PodMigrationResource,
			}),
			r.newPodMigrationResumeCommand(sourceController),
			r.newPodMigrationAbortCommand(sourceController),
			r.newPodMigrationRollbackCommand(sourceController),
			r.newPodMigrationCleanupCommand(sourceController),
		),
		r.newCRFamilyCommand(
			"migrate",
			"Migration workflows",
			r.newCRMigrationCreateCommand(),
			false,
			r.newOfflineMigrationStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindMigration,
				resource: domain.MigrationResource,
			}),
			r.newOfflineMigrationResumeCommand(sourceController),
			r.newOfflineMigrationAbortCommand(sourceController),
			r.newOfflineMigrationRollbackCommand(sourceController),
			r.newOfflineMigrationCleanupCommand(sourceController),
		),
		r.newCRFamilyCommand(
			"cluster-migrate",
			"ClusterMigration workflows",
			r.newCRClusterMigrationCreateCommand(),
			true,
			r.newOfflineMigrationStatusCommand(sourceClusterController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindClusterMigration,
				resource: domain.ClusterMigrationResource,
				cluster:  true,
			}),
			r.newOfflineMigrationResumeCommand(sourceClusterController),
			r.newOfflineMigrationAbortCommand(sourceClusterController),
			r.newOfflineMigrationRollbackCommand(sourceClusterController),
			r.newOfflineMigrationCleanupCommand(sourceClusterController),
		),
		r.newCRFamilyCommand(
			"copy",
			"Copy workflows",
			r.newCRCopyCreateCommand(),
			false,
			r.newCopyStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindCopy,
				resource: domain.CopyResource,
			}),
			r.newCopyResumeCommand(sourceController),
			r.newCopyAbortCommand(sourceController),
			r.newCopyCleanupCommand(sourceController),
		),
		r.newCRFamilyCommand(
			"cluster-copy",
			"ClusterCopy workflows",
			r.newCRClusterCopyCreateCommand(),
			true,
			r.newCopyStatusCommand(sourceClusterController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindClusterCopy,
				resource: domain.ClusterCopyResource,
				cluster:  true,
			}),
			r.newCopyResumeCommand(sourceClusterController),
			r.newCopyAbortCommand(sourceClusterController),
			r.newCopyCleanupCommand(sourceClusterController),
		),
		r.newCRFamilyCommand(
			"reserve",
			"Reservation workflows",
			r.newCRReserveCreateCommand(),
			false,
			r.newReserveStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindReservation,
				resource: domain.ReservationResource,
			}),
			r.newReserveResumeCommand(sourceController),
			r.newReserveAbortCommand(sourceController),
			r.newReserveCleanupCommand(sourceController),
		),
		r.newCRFamilyCommand(
			"cluster-reserve",
			"ClusterReservation workflows",
			r.newCRClusterReserveCreateCommand(),
			true,
			r.newReserveStatusCommand(sourceClusterController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindClusterReservation,
				resource: domain.ClusterReservationResource,
				cluster:  true,
			}),
			r.newReserveResumeCommand(sourceClusterController),
			r.newReserveAbortCommand(sourceClusterController),
			r.newReserveCleanupCommand(sourceClusterController),
		),
		r.newCRFamilyCommand(
			"rename",
			"Rename workflows",
			r.newCRRenameCreateCommand(),
			false,
			r.newRenameStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindRename,
				resource: domain.RenameResource,
			}),
			r.newRenameResumeCommand(sourceController),
			r.newRenameAbortCommand(sourceController),
			r.newRenameRollbackCommand(sourceController),
			r.newRenameCleanupCommand(sourceController),
		),
		r.newCRFamilyCommand(
			"move",
			"Move workflows",
			r.newCRMoveCreateCommand(),
			true,
			r.newMoveStatusCommand(sourceClusterController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindMove,
				resource: domain.MoveResource,
				cluster:  true,
			}),
			r.newMoveResumeCommand(sourceClusterController),
			r.newMoveAbortCommand(sourceClusterController),
			r.newMoveRollbackCommand(sourceClusterController),
			r.newMoveCleanupCommand(sourceClusterController),
		),
		r.newCRFamilyCommand(
			"backup",
			"Backup workflows",
			r.newCRBackupCreateCommand(),
			false,
			r.newBackupStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindBackup,
				resource: domain.BackupResource,
			}),
			r.newBackupResumeCommand(sourceController),
			r.newBackupAbortCommand(sourceController),
			r.newBackupCleanupCommand(sourceController),
		),
		r.newCRFamilyCommand(
			"restore",
			"Restore workflows",
			r.newCRRestoreCreateCommand(),
			false,
			r.newRestoreStatusCommand(sourceController),
			r.newCRWatchCommand(crWatchTarget{
				kind:     domain.ControllerKindRestore,
				resource: domain.RestoreResource,
			}),
			r.newRestoreResumeCommand(sourceController),
			r.newRestoreAbortCommand(sourceController),
			r.newRestoreCleanupCommand(sourceController),
		),
	)

	return command
}

// newCRFamilyCommand shapes one workflow family inside the cr group: a create
// verb plus the shared addressing verbs. Namespaced families carry -n on every
// addressing verb; cluster-scoped families (Move) do not.
func (r *rootState) newCRFamilyCommand(
	name, short string,
	create *cobra.Command,
	clusterScoped bool,
	verbs ...*cobra.Command,
) *cobra.Command {
	command := &cobra.Command{
		Use:   name,
		Short: short,
		Args:  cobra.NoArgs,
	}

	command.AddCommand(create)

	for _, verb := range verbs {
		if !clusterScoped && verb.Flags().Lookup("namespace") == nil {
			var namespace string
			bindCRNamespace(verb, &namespace)
		}

		command.AddCommand(verb)
	}

	return command
}

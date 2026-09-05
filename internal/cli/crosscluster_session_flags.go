package cli

import (
	"fmt"
	"slices"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
)

func validateExistingCrossClusterFlags(cmd *cobra.Command, additional ...string) error {
	names := slices.Concat(additional, []string{
		"source-namespace", "destination-namespace", "source-pvc", "destination-pvc",
		"destination-capacity", "source-path", "destination-path", "destination-storage-class",
		"allow-volume-shrink", "skip-source-usage-check", "target-node",
	})

	var changed []string
	for _, name := range names {
		if cmd.Flags().Changed(name) {
			changed = append(changed, "--"+name)
		}
	}

	if len(changed) == 0 {
		return nil
	}

	return domain.NewError(
		domain.ErrorValidation,
		"cross-cluster session flags",
		fmt.Sprintf(
			"existing session reuses its recorded storage and paths; omit %s or create a new session with the intended plan",
			strings.Join(changed, ", "),
		),
	)
}

// Configure only before the first transfer. Retries must keep the exact scope
// and consistency settings associated with already completed volumes.
func configureExistingCrossClusterCopy(
	cmd *cobra.Command,
	session *crosscluster.Session,
	flags *crossClusterCopyFlags,
) error {
	if err := validateExistingCrossClusterFlags(cmd); err != nil {
		return err
	}

	spec := session.Spec
	if cmd.Flags().Changed("tool-image") {
		spec.ToolImage = flags.toolImage
	}

	if cmd.Flags().Changed("strategy") {
		spec.Strategies = slices.Clone(flags.strategies)
	}

	if cmd.Flags().Changed("online") {
		spec.Online = flags.online
	}

	if cmd.Flags().Changed("verify-checksum") {
		spec.VerifyChecksum = flags.verifyChecksum
	}

	if cmd.Flags().Changed("delete-extraneous") {
		spec.DeleteExtraneous = flags.deleteExtraneous
	}

	changed := spec.ToolImage != session.Spec.ToolImage ||
		!slices.Equal(spec.Strategies, session.Spec.Strategies) ||
		spec.Online != session.Spec.Online ||
		spec.VerifyChecksum != session.Spec.VerifyChecksum ||
		spec.DeleteExtraneous != session.Spec.DeleteExtraneous
	if !changed {
		return nil
	}

	started := session.Status.Phase == crosscluster.PhaseTransferring ||
		session.Status.Phase == crosscluster.PhaseCompleted
	for _, volume := range session.Status.Volumes {
		started = started || volume.Transfer.Attempts > 0 || volume.Transfer.CompletedAt != nil
	}

	if started || session.Status.Phase == crosscluster.PhaseCleaning ||
		session.Status.Phase == crosscluster.PhaseCleaned {
		return domain.NewError(
			domain.ErrorValidation,
			"cross-cluster session flags",
			"copy settings cannot change after transfer has started or cleanup has begun; resume with the recorded settings or create a new session",
		)
	}

	updated := *session

	updated.Spec = spec
	if err := updated.Validate(); err != nil {
		return domain.NewError(domain.ErrorValidation, "cross-cluster session flags", err.Error())
	}

	session.Spec = spec

	return nil
}

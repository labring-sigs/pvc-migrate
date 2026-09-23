package output

import (
	"fmt"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (p Printer) printPodMigration(object *v1alpha1.PodMigration) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tWARM COPIED\tFINAL SYNCED\tACTIVATED\tROLLED BACK",
	); err != nil {
		return err
	}

	checkpoints := make(map[string]v1alpha1.PodMigrationVolumeStatus, len(object.Status.Volumes))
	for _, checkpoint := range object.Status.Volumes {
		checkpoints[checkpoint.SourcePVCName] = checkpoint
	}

	if plan := object.Status.Plan; plan != nil {
		for _, volume := range plan.Volumes {
			checkpoint := checkpoints[volume.SourcePVC.Name]

			if err := printPodMigrationVolume(
				w,
				object.Namespace+"/"+volume.SourcePVC.Name,
				checkpoint.Sync.WarmCompletedAt != nil,
				checkpoint.Sync.FinalCompletedAt != nil,
				checkpoint.Activation.ActivatedAt != nil,
				checkpoint.Activation.RolledBackAt != nil,
			); err != nil {
				return err
			}
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return p.printWorkflowLifecycle(object.Status.WorkflowStatus)
}

func printPodMigrationVolume(
	w *tabwriter.Writer,
	source string,
	warmCopied, finalSynced, activated, rolledBack bool,
) error {
	_, err := fmt.Fprintf(
		w,
		"%s\t%t\t%t\t%t\t%t\n",
		source,
		warmCopied,
		finalSynced,
		activated,
		rolledBack,
	)

	return err
}

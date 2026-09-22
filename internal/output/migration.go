package output

import (
	"fmt"
	"io"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (p Printer) printMigration(object *v1alpha1.Migration) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tSTAGED PVC\tDESTINATION PV\tFINAL SYNCED\tACTIVATED\tROLLED BACK",
	); err != nil {
		return err
	}

	checkpoints := make(map[string]v1alpha1.MigrationVolumeStatus, len(object.Status.Volumes))
	for _, checkpoint := range object.Status.Volumes {
		checkpoints[checkpoint.SourcePVCName] = checkpoint
	}

	if plan := object.Status.Plan; plan != nil {
		for _, volume := range plan.Volumes {
			checkpoint := checkpoints[volume.SourcePVC.Name]

			pv := ""
			if checkpoint.DestinationPV != nil {
				pv = checkpoint.DestinationPV.Name
			}

			if err := printMigrationVolume(
				w,
				object.Namespace,
				volume.SourcePVC.Name,
				object.Namespace,
				volume.DestinationPVC.Name,
				pv,
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

func (p Printer) printClusterMigration(object *v1alpha1.ClusterMigration) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tSTAGED PVC\tDESTINATION PV\tFINAL SYNCED\tACTIVATED\tROLLED BACK",
	); err != nil {
		return err
	}

	checkpoints := make(
		map[string]v1alpha1.ClusterMigrationVolumeStatus,
		len(object.Status.Volumes),
	)
	for _, checkpoint := range object.Status.Volumes {
		checkpoints[checkpoint.SourcePVCName] = checkpoint
	}

	if plan := object.Status.Plan; plan != nil {
		for _, volume := range plan.Volumes {
			checkpoint := checkpoints[volume.SourcePVC.Name]

			pv := ""
			if checkpoint.DestinationPV != nil {
				pv = checkpoint.DestinationPV.Name
			}

			if err := printMigrationVolume(
				w,
				string(plan.SourceNamespace),
				volume.SourcePVC.Name,
				string(plan.TemporaryNamespace),
				volume.DestinationPVC.Name,
				pv,
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

func printMigrationVolume(
	w io.Writer,
	sourceNamespace, source, temporaryNamespace, destination, pv string,
	synced, activated, rolledBack bool,
) error {
	_, err := fmt.Fprintf(
		w,
		"%s/%s\t%s/%s\t%s\t%t\t%t\t%t\n",
		sourceNamespace,
		source,
		temporaryNamespace,
		destination,
		valueOrUnknown(pv),
		synced,
		activated,
		rolledBack,
	)

	return err
}

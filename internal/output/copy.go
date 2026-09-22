package output

import (
	"fmt"
	"io"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (p Printer) printCopy(object *v1alpha1.Copy) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tDESTINATION PVC\tDESTINATION PV\tCOPIED",
	); err != nil {
		return err
	}

	checkpoints := make(map[string]v1alpha1.CopyVolumeStatus, len(object.Status.Volumes))
	for _, checkpoint := range object.Status.Volumes {
		checkpoints[checkpoint.SourcePVCName] = checkpoint
	}

	if object.Status.Plan != nil {
		for _, volume := range object.Status.Plan.Volumes {
			checkpoint := checkpoints[volume.SourcePVC.Name]

			pv := ""
			if checkpoint.DestinationPV != nil {
				pv = checkpoint.DestinationPV.Name
			}

			if err := printCopyVolume(
				w,
				object.Namespace,
				volume.SourcePVC.Name,
				object.Namespace,
				volume.DestinationPVC.Name,
				pv,
				checkpoint.Sync.WarmCompletedAt != nil,
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

func (p Printer) printClusterCopy(object *v1alpha1.ClusterCopy) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tDESTINATION PVC\tDESTINATION PV\tCOPIED",
	); err != nil {
		return err
	}

	checkpoints := make(map[string]v1alpha1.ClusterCopyVolumeStatus, len(object.Status.Volumes))
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

			if err := printCopyVolume(
				w,
				string(plan.SourceNamespace),
				volume.SourcePVC.Name,
				string(plan.DestinationNamespace),
				volume.DestinationPVC.Name,
				pv,
				checkpoint.Sync.WarmCompletedAt != nil,
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

func printCopyVolume(
	w io.Writer,
	sourceNamespace, source, destinationNamespace, destination, pv string,
	copied bool,
) error {
	_, err := fmt.Fprintf(
		w,
		"%s/%s\t%s/%s\t%s\t%t\n",
		sourceNamespace,
		source,
		destinationNamespace,
		destination,
		valueOrUnknown(pv),
		copied,
	)

	return err
}

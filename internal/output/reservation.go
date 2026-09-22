package output

import (
	"fmt"
	"io"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (p Printer) printReservation(object *v1alpha1.Reservation) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tDESTINATION PVC\tRESERVED PV\tRESERVED",
	); err != nil {
		return err
	}

	checkpoints := make(map[string]v1alpha1.ReservationVolumeStatus, len(object.Status.Volumes))
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

			if err := printReservationVolume(
				w,
				object.Namespace,
				volume.SourcePVC.Name,
				object.Namespace,
				volume.DestinationPVC.Name,
				pv,
				checkpoint.Reserved,
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

func (p Printer) printClusterReservation(object *v1alpha1.ClusterReservation) error {
	if object == nil {
		return nil
	}

	if err := p.printWorkflowInventory([]crclient.Object{object}); err != nil {
		return err
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tDESTINATION PVC\tRESERVED PV\tRESERVED",
	); err != nil {
		return err
	}

	checkpoints := make(
		map[string]v1alpha1.ClusterReservationVolumeStatus,
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

			if err := printReservationVolume(
				w,
				string(plan.SourceNamespace),
				volume.SourcePVC.Name,
				string(plan.DestinationNamespace),
				volume.DestinationPVC.Name,
				pv,
				checkpoint.Reserved,
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

func printReservationVolume(
	w io.Writer,
	sourceNamespace, source, destinationNamespace, destination, pv string,
	reserved bool,
) error {
	_, err := fmt.Fprintf(
		w,
		"%s/%s\t%s/%s\t%s\t%t\n",
		sourceNamespace,
		source,
		destinationNamespace,
		destination,
		valueOrUnknown(pv),
		reserved,
	)

	return err
}

// printWorkflowInventory performs heterogeneous dispatch only at the output
// boundary. Business code continues to receive concrete operation objects.
func (p Printer) printWorkflowInventory(objects []crclient.Object) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "KIND\tSESSION\tNAMESPACE\tPHASE\tUPDATED\tMESSAGE"); err != nil {
		return err
	}

	for _, object := range objects {
		var (
			kind   string
			status v1alpha1.WorkflowStatus
		)
		switch current := object.(type) {
		case *v1alpha1.Reservation:
			kind, status = "Reservation", current.Status.WorkflowStatus
		case *v1alpha1.ClusterReservation:
			kind, status = "ClusterReservation", current.Status.WorkflowStatus
		case *v1alpha1.Copy:
			kind, status = "Copy", current.Status.WorkflowStatus
		case *v1alpha1.ClusterCopy:
			kind, status = "ClusterCopy", current.Status.WorkflowStatus
		case *v1alpha1.Migration:
			kind, status = "Migration", current.Status.WorkflowStatus
		case *v1alpha1.ClusterMigration:
			kind, status = "ClusterMigration", current.Status.WorkflowStatus
		default:
			return fmt.Errorf("unsupported workflow inventory object %T", object)
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			kind,
			object.GetName(),
			valueOrUnknown(object.GetNamespace()),
			valueOrUnknown(string(status.Phase)),
			formatDisplayTime(status.UpdatedAt.Time),
			status.Message,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

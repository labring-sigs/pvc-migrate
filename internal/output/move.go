package output

import (
	"fmt"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func (p Printer) printMoves(objects []*v1alpha1.Move) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"SESSION\tPHASE\tSOURCE PVC\tDESTINATION PVC\tPV\tACTIVE PVC",
	); err != nil {
		return err
	}

	for _, object := range objects {
		if object == nil {
			continue
		}

		destination := object.Spec.SourcePVC.Name
		if object.Spec.DestinationPVC != nil && object.Spec.DestinationPVC.Name != "" {
			destination = object.Spec.DestinationPVC.Name
		}

		pv, active := "-", "-"
		if object.Status.Plan != nil {
			pv = object.Status.Plan.Identity.SourcePV.Name
		}

		if object.Status.Activation.ActivePVC != nil {
			ref := object.Status.Activation.ActivePVC
			active = ref.Namespace + "/" + ref.Name
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s/%s\t%s/%s\t%s\t%s\n",
			object.Name,
			valueOrUnknown(
				string(object.Status.Phase),
			),
			object.Spec.SourceNamespace,
			object.Spec.SourcePVC.Name,
			object.Spec.DestinationNamespace,
			destination,
			pv,
			active,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

func (p Printer) printMove(object *v1alpha1.Move) error {
	if err := p.printMoves([]*v1alpha1.Move{object}); err != nil {
		return err
	}

	if object == nil {
		return nil
	}

	return p.printWorkflowLifecycle(object.Status.WorkflowStatus)
}

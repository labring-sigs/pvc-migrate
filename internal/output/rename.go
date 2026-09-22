package output

import (
	"fmt"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func (p Printer) printRenames(objects []*v1alpha1.Rename) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"SESSION\tNAMESPACE\tPHASE\tSOURCE PVC\tDESTINATION PVC\tPV\tACTIVE PVC",
	); err != nil {
		return err
	}

	for _, object := range objects {
		if object == nil {
			continue
		}

		pv, active := "-", "-"
		if object.Status.Plan != nil {
			pv = object.Status.Plan.SourcePV.Name
		}

		if object.Status.Activation.ActivePVC != nil {
			active = object.Status.Activation.ActivePVC.Name
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			object.Name,
			object.Namespace,
			valueOrUnknown(string(object.Status.Phase)),
			object.Spec.SourcePVC.Name,
			object.Spec.DestinationPVC.Name,
			pv,
			active,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

func (p Printer) printRename(object *v1alpha1.Rename) error {
	if err := p.printRenames([]*v1alpha1.Rename{object}); err != nil {
		return err
	}

	if object == nil {
		return nil
	}

	return p.printWorkflowLifecycle(object.Status.WorkflowStatus)
}

func (p Printer) printWorkflowLifecycle(status v1alpha1.WorkflowStatus) error {
	if status.Message != "" {
		if _, err := fmt.Fprintf(p.Writer, "\n%s\n", status.Message); err != nil {
			return err
		}
	}

	if status.ResumeFrom != "" && status.Phase == "Failed" {
		if _, err := fmt.Fprintf(
			p.Writer,
			"Resume checkpoint: %s\n",
			status.ResumeFrom,
		); err != nil {
			return err
		}
	}

	if len(status.History) == 0 {
		return nil
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "\nTIME\tPHASE\tMESSAGE"); err != nil {
		return err
	}

	for _, entry := range status.History {
		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\n",
			entry.Time.UTC().Format("2006-01-02T15:04:05Z"),
			entry.Phase,
			entry.Message,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

package output

import (
	"fmt"
	"text/tabwriter"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
)

func (p Printer) printBackupWorkflow(object *v1alpha1.Backup) error {
	if object == nil {
		return nil
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"SESSION\tNAMESPACE\tPHASE\tSOURCE PVC\tREPOSITORY\tBACKUP\tPATH\tONLINE",
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\n",
		object.Name, object.Namespace, valueOrUnknown(string(object.Status.Phase)),
		object.Spec.SourcePVC.Name, object.Spec.RepositoryRef.Name,
		object.Spec.Name, object.Spec.Path, object.Spec.Online,
	); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return p.printWorkflowLifecycle(object.Status.WorkflowStatus)
}

func (p Printer) printRestoreWorkflow(object *v1alpha1.Restore) error {
	if object == nil {
		return nil
	}

	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"SESSION\tNAMESPACE\tPHASE\tDESTINATION PVC\tREPOSITORY\tBACKUP\tPATH\tCREATE PVC",
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\n",
		object.Name, object.Namespace, valueOrUnknown(string(object.Status.Phase)),
		object.Spec.DestinationPVC.Name, object.Spec.RepositoryRef.Name,
		object.Spec.Name, object.Spec.Path, object.Spec.CreatePVC,
	); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return p.printWorkflowLifecycle(object.Status.WorkflowStatus)
}

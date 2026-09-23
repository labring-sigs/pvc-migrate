package output

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

type Format string

const (
	Table Format = "table"
	JSON  Format = "json"
	YAML  Format = "yaml"
)

type Printer struct {
	Writer io.Writer
	Format Format
}

func (p Printer) Print(value any) error {
	switch p.Format {
	case JSON:
		encoder := json.NewEncoder(p.Writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	case YAML:
		data, err := yaml.Marshal(value)
		if err != nil {
			return err
		}

		_, err = p.Writer.Write(data)

		return err
	case Table, "":
		return p.printTable(value)
	default:
		return domain.NewError(
			domain.ErrorValidation,
			"output",
			fmt.Sprintf("unsupported format %q", p.Format),
		)
	}
}

func (p Printer) printTable(value any) error {
	switch typed := value.(type) {
	case *domain.TransferPlan:
		return p.printPlan(typed)
	case *domain.PVCIdentityReport:
		return p.printIdentityPlan(typed)
	case *v1alpha1.Rename:
		return p.printRename(typed)
	case []*v1alpha1.Rename:
		return p.printRenames(typed)
	case *v1alpha1.Move:
		return p.printMove(typed)
	case *v1alpha1.Reservation:
		return p.printReservation(typed)
	case *v1alpha1.ClusterReservation:
		return p.printClusterReservation(typed)
	case *v1alpha1.Copy:
		return p.printCopy(typed)
	case *v1alpha1.ClusterCopy:
		return p.printClusterCopy(typed)
	case *v1alpha1.Migration:
		return p.printMigration(typed)
	case *v1alpha1.ClusterMigration:
		return p.printClusterMigration(typed)
	case *v1alpha1.PodMigration:
		return p.printPodMigration(typed)
	case *v1alpha1.ClusterPodMigration:
		return p.printClusterPodMigration(typed)
	case []crclient.Object:
		return p.printWorkflowInventory(typed)
	case []*v1alpha1.Move:
		return p.printMoves(typed)
	case *v1alpha1.Backup:
		return p.printBackupWorkflow(typed)
	case *v1alpha1.Restore:
		return p.printRestoreWorkflow(typed)
	case *domain.OrphanCleanupPlan:
		return p.printOrphanCleanupPlan(typed)
	default:
		encoder := json.NewEncoder(p.Writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
}

func (p Printer) printOrphanCleanupPlan(plan *domain.OrphanCleanupPlan) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	switch plan.Mode {
	case domain.OrphanCleanupPreActivation:
		resources := plan.PreActivation
		if resources == nil {
			return domain.NewError(
				domain.ErrorInternal,
				"output",
				"pre-activation orphan resources are missing",
			)
		}

		if _, err := fmt.Fprintf(
			w,
			"SESSION\tREADY\tMODE\tSOURCE PVC\tSOURCE PV\tDESTINATION PVC\tDESTINATION PV\n%s\t%t\t%s\t%s/%s\t%s\t%s/%s\t%s\n\n",
			plan.SessionID,
			plan.Ready,
			plan.Mode,
			resources.SourcePVC.Namespace,
			resources.SourcePVC.Name,
			resources.SourcePV.Name,
			resources.DestinationPVC.Namespace,
			resources.DestinationPVC.Name,
			resources.DestinationPV.Name,
		); err != nil {
			return err
		}
	case domain.OrphanCleanupPostActivation:
		resources := plan.PostActivation
		if resources == nil {
			return domain.NewError(
				domain.ErrorInternal,
				"output",
				"post-activation orphan resources are missing",
			)
		}

		if _, err := fmt.Fprintf(
			w,
			"SESSION\tREADY\tMODE\tSOURCE PVC\tACTIVE PV\tROLLBACK PV\n%s\t%t\t%s\t%s/%s\t%s\t%s\n\n",
			plan.SessionID,
			plan.Ready,
			plan.Mode,
			resources.SourcePVC.Namespace,
			resources.SourcePVC.Name,
			resources.ActivePV.Name,
			resources.RollbackPV.Name,
		); err != nil {
			return err
		}
	default:
		if _, err := fmt.Fprintf(
			w,
			"SESSION\tREADY\tMODE\n%s\t%t\t%s\n\n",
			plan.SessionID,
			plan.Ready,
			plan.Mode,
		); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w, "CHECK\tRESULT\tSEVERITY\tMESSAGE"); err != nil {
		return err
	}

	for _, check := range plan.Checks {
		result := "PASS"
		if !check.Passed {
			result = "FAIL"
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\n",
			check.Name,
			result,
			check.Severity,
			check.Message,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

func (p Printer) printPlan(plan *domain.TransferPlan) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(
		w,
		"SESSION\tREADY\tSOURCE\tSTAGING\tTARGET NODE\n%s\t%t\t%s\t%s\t%s\n\n",
		plan.SessionID,
		plan.Ready,
		plan.SourceNamespace,
		plan.TemporaryNamespace,
		plan.TargetNode,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"PVC\tSOURCE PV\tDESTINATION PVC\tTRANSFER SCOPE\tSOURCE CAPACITY\tSOURCE USED\tDESTINATION CAPACITY\tCLASS\tMODE",
	); err != nil {
		return err
	}

	for _, volume := range plan.Volumes {
		if _, err := fmt.Fprintf(
			w,
			"%s/%s\t%s\t%s/%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			volume.SourcePVC.Namespace,
			volume.SourcePVC.Name,
			volume.SourcePV.Name,
			volume.DestinationPVC.Namespace,
			volume.DestinationPVC.Name,
			transferScopeText(volume.TransferScope),
			valueOrUnknown(volume.SourceCapacity),
			sourceUsageText(volume.SourceUsageKnown, volume.SourceUsedBytes),
			volume.Capacity,
			volume.StorageClass,
			volume.VolumeMode,
		); err != nil {
			return err
		}
	}

	if len(plan.StorageCapacity) > 0 {
		if _, err := fmt.Fprintln(
			w,
			"\nSTORAGE CLASS\tTARGET NODE\tREQUESTED\tREPORTED\tMAX VOLUME\tSTATUS",
		); err != nil {
			return err
		}

		for _, capacity := range plan.StorageCapacity {
			if _, err := fmt.Fprintf(
				w,
				"%s\t%s\t%s\t%s\t%s\t%s\n",
				capacity.StorageClass,
				capacity.TargetNode,
				capacity.RequestedCapacity,
				valueOrUnknown(capacity.ReportedCapacity),
				valueOrUnknown(capacity.MaximumVolumeSize),
				capacity.Status,
			); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintln(w, "\nCHECK\tRESULT\tSEVERITY\tMESSAGE"); err != nil {
		return err
	}

	for _, check := range plan.Checks {
		result := "PASS"
		if !check.Passed {
			result = "FAIL"
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\n",
			check.Name,
			result,
			check.Severity,
			check.Message,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

func valueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func formatDisplayTime(value time.Time) string {
	return value.In(time.Local).Format(time.RFC3339)
}

func sourceUsageText(known bool, bytes int64) string {
	if !known {
		return "unknown"
	}
	return resource.NewQuantity(bytes, resource.BinarySI).String()
}

func transferScopeText(scope *v1alpha1.TransferScope) string {
	if scope == nil {
		return "full"
	}
	return scope.SourcePath + " -> " + scope.DestinationPath
}

func (p Printer) printIdentityPlan(plan *domain.PVCIdentityReport) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(
		w,
		"SESSION\tREADY\tSOURCE\tDESTINATION\n%s\t%t\t%s\t%s\n\n",
		plan.SessionID,
		plan.Ready,
		plan.SourceNamespace,
		plan.DestinationNamespace,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(w, "PVC\tPV\tDESTINATION PVC"); err != nil {
		return err
	}

	for _, volume := range plan.Volumes {
		if _, err := fmt.Fprintf(
			w,
			"%s/%s\t%s\t%s/%s\n",
			volume.SourcePVC.Namespace,
			volume.SourcePVC.Name,
			volume.SourcePV.Name,
			volume.DestinationPVC.Namespace,
			volume.DestinationPVC.Name,
		); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w, "\nCHECK\tRESULT\tSEVERITY\tMESSAGE"); err != nil {
		return err
	}

	for _, check := range plan.Checks {
		result := "PASS"
		if !check.Passed {
			result = "FAIL"
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\n",
			check.Name,
			result,
			check.Severity,
			check.Message,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

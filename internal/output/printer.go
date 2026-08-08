package output

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		return domain.NewError(domain.ErrorValidation, "output", fmt.Sprintf("unsupported format %q", p.Format))
	}
}

func (p Printer) printTable(value any) error {
	switch typed := value.(type) {
	case *domain.MigrationPlan:
		return p.printPlan(typed)
	case *domain.Session:
		return p.printSession(typed)
	case []*domain.Session:
		return p.printSessions(typed)
	default:
		encoder := json.NewEncoder(p.Writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
}

func (p Printer) printPlan(plan *domain.MigrationPlan) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(w, "SESSION\tREADY\tSOURCE\tSTAGING\tTARGET NODE\n%s\t%t\t%s\t%s\t%s\n\n", plan.SessionID, plan.Ready, plan.SourceNamespace, plan.TemporaryNamespace, plan.TargetNode); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "PVC\tSOURCE PV\tDESTINATION PVC\tCAPACITY\tCLASS\tMODE"); err != nil {
		return err
	}
	for _, volume := range plan.Volumes {
		if _, err := fmt.Fprintf(w, "%s/%s\t%s\t%s/%s\t%s\t%s\t%s\n", volume.SourcePVC.Namespace, volume.SourcePVC.Name, volume.SourcePV.Name, volume.DestinationPVC.Namespace, volume.DestinationPVC.Name, volume.Capacity, volume.StorageClass, volume.VolumeMode); err != nil {
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
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", check.Name, result, check.Severity, check.Message); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (p Printer) printSession(session *domain.Session) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "SESSION\tOPERATION\tPHASE\tUPDATED\tMESSAGE"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", session.ID, session.Spec.Operation(), session.Status.Phase, session.Status.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"), session.Status.Message); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "\nPVC\tRESERVED\tWARM SYNC\tFINAL SYNC\tACTIVE PV"); err != nil {
		return err
	}
	for index, status := range session.Status.Volumes {
		warm := timeValue(status.Sync.WarmCompletedAt)
		final := timeValue(status.Sync.FinalCompletedAt)
		activePV := ""
		if index < len(session.Spec.Volumes) && status.Activation.ActivatedAt != nil {
			if session.Spec.Operation().RebindsPVC() || status.Activation.RolledBackAt != nil {
				activePV = session.Spec.Volumes[index].SourcePV.Name
			} else {
				activePV = session.Spec.Volumes[index].DestinationPV.Name
			}
		}
		if _, err := fmt.Fprintf(w, "%s\t%t\t%s\t%s\t%s\n", status.SourcePVCName, status.Reserved, warm, final, activePV); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (p Printer) printSessions(sessions []*domain.Session) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "SESSION\tOPERATION\tPHASE\tSOURCE NAMESPACE\tUPDATED"); err != nil {
		return err
	}
	for _, session := range sessions {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", session.ID, session.Spec.Operation(), session.Status.Phase, session.Spec.SourceNamespace, session.Status.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z")); err != nil {
			return err
		}
	}
	return w.Flush()
}

func timeValue(value *metav1.Time) string {
	if value == nil || value.IsZero() {
		return "-"
	}
	return value.UTC().Format("2006-01-02T15:04:05Z")
}

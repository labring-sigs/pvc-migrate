package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/crosscluster"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"k8s.io/apimachinery/pkg/api/resource"
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
		return domain.NewError(
			domain.ErrorValidation,
			"output",
			fmt.Sprintf("unsupported format %q", p.Format),
		)
	}
}

func (p Printer) printTable(value any) error {
	switch typed := value.(type) {
	case *domain.MigrationPlan:
		return p.printPlan(typed)
	case *domain.OrphanCleanupPlan:
		return p.printOrphanCleanupPlan(typed)
	case *backup.Plan:
		return p.printBackupPlan(typed)
	case *backup.Result:
		return p.printBackupResult(typed)
	case *domain.Session:
		return p.printSession(typed)
	case []*domain.Session:
		return p.printSessions(typed)
	case *crosscluster.Plan:
		return p.printCrossClusterPlan(typed)
	case *crosscluster.Session:
		return p.printCrossClusterSession(typed)
	default:
		encoder := json.NewEncoder(p.Writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
}

func (p Printer) printCrossClusterPlan(plan *crosscluster.Plan) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(
		w,
		"SESSION\tREADY\tSOURCE CLUSTER\tDESTINATION CLUSTER\tSOURCE NAMESPACE\tDESTINATION NAMESPACE\tTARGET NODE\n%s\t%t\t%s\t%s\t%s\t%s\t%s\n\n",
		plan.SessionID,
		plan.Ready,
		plan.SourceCluster.ID,
		plan.DestinationCluster.ID,
		plan.SourceNamespace,
		plan.DestinationNamespace,
		valueOrUnknown(plan.TargetNode),
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"SOURCE PVC\tDESTINATION PVC\tSOURCE PATH\tDESTINATION PATH\tSOURCE CAPACITY\tDESTINATION CAPACITY\tSTORAGE CLASS",
	); err != nil {
		return err
	}

	for _, volume := range plan.Volumes {
		if _, err := fmt.Fprintf(
			w,
			"%s/%s\t%s/%s\t%s\t%s\t%s\t%s\t%s\n",
			volume.SourceNamespace,
			volume.SourcePVC,
			volume.DestinationNamespace,
			volume.DestinationPVC,
			transferPathOrRoot(volume.SourcePath),
			transferPathOrRoot(volume.DestinationPath),
			volume.SourceCapacity,
			volume.Capacity,
			volume.StorageClass,
		); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w, "\nCHECK\tRESULT\tMESSAGE"); err != nil {
		return err
	}

	for _, check := range plan.Checks {
		result := "PASS"
		if !check.Passed {
			result = "FAIL"
		}

		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\n", check.Name, result, check.Message); err != nil {
			return err
		}
	}

	return w.Flush()
}

func (p Printer) printCrossClusterSession(session *crosscluster.Session) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(
		w,
		"SESSION\tPHASE\tSOURCE CLUSTER\tDESTINATION CLUSTER\tUPDATED\tMESSAGE\n%s\t%s\t%s\t%s\t%s\t%s\n\n",
		session.ID,
		session.Status.Phase,
		session.Spec.SourceCluster.ID,
		session.Spec.DestinationCluster.ID,
		session.Status.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		session.Status.Message,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"SOURCE PVC\tDESTINATION PVC\tRESERVED PV\tTRANSFER\tATTEMPTS\tLAST ERROR",
	); err != nil {
		return err
	}

	for index, volume := range session.Spec.Volumes {
		status := session.Status.Volumes[index]

		transfer := "pending"
		if status.Transfer.CompletedAt != nil {
			transfer = "completed"
		}

		if _, err := fmt.Fprintf(
			w,
			"%s/%s\t%s/%s\t%s\t%s\t%d\t%s\n",
			volume.Source.PVC.Namespace,
			volume.Source.PVC.Name,
			volume.Destination.PVC.Namespace,
			volume.Destination.PVC.Name,
			valueOrUnknown(volume.Destination.PV.Name),
			transfer,
			status.Transfer.Attempts,
			status.Transfer.LastError,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

func (p Printer) printBackupPlan(plan *backup.Plan) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"OPERATION\tPVC\tPATH\tMODE\tCONSISTENCY\tCAPACITY\tVOLUME MODE\tTOOL NODE\tDESTINATION",
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"%s\t%s/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		plan.Operation,
		plan.Namespace,
		plan.PVC,
		transferPathOrRoot(plan.Path),
		plan.Mode,
		plan.Consistency,
		plan.Capacity,
		plan.VolumeMode,
		valueOrUnknown(plan.ToolNode),
		plan.Destination,
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		w,
		"\nMANIFEST\tOBJECTS\tTOTAL BYTES\tINVENTORY SHA256\tDELETE EXTRA\tCOMPRESSION",
	); err != nil {
		return err
	}

	manifest := "absent"
	if plan.ManifestPresent {
		manifest = "present"
	}

	if _, err := fmt.Fprintf(
		w,
		"%s\t%d\t%d\t%s\t%t\t%s\n",
		manifest,
		plan.ObjectCount,
		plan.TotalBytes,
		valueOrUnknown(plan.InventorySHA256),
		plan.DeleteExtraneous,
		plan.Compression,
	); err != nil {
		return err
	}

	if len(plan.MountedPods) > 0 {
		if _, err := fmt.Fprintf(
			w,
			"\nMOUNTED PODS\t%s\n",
			strings.Join(plan.MountedPods, ", "),
		); err != nil {
			return err
		}
	}

	if len(plan.Warnings) > 0 {
		if _, err := fmt.Fprintln(w, "\nWARNING"); err != nil {
			return err
		}

		for _, warning := range plan.Warnings {
			if _, err := fmt.Fprintln(w, warning); err != nil {
				return err
			}
		}
	}

	return w.Flush()
}

func (p Printer) printBackupResult(result *backup.Result) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	identityHeader := "SESSION"

	identity := result.SessionID
	if result.Mode == backup.ModeRestore || result.OperationID != "" {
		identityHeader = "OPERATION ID"
		identity = result.OperationID
	}

	if _, err := fmt.Fprintf(
		w,
		"OPERATION\t%s\tPVC\tPATH\tMODE\tSTATUS\tDESTINATION\n",
		identityHeader,
	); err != nil {
		return err
	}

	if identity == "" {
		identity = "-"
	}

	if _, err := fmt.Fprintf(
		w,
		"%s\t%s\t%s/%s\t%s\t%s\t%s\t%s\n",
		result.Operation,
		identity,
		result.Namespace,
		result.PVC,
		transferPathOrRoot(result.Path),
		result.Mode,
		result.Status,
		result.Destination,
	); err != nil {
		return err
	}

	return w.Flush()
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

func (p Printer) printPlan(plan *domain.MigrationPlan) error {
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

func (p Printer) printSession(session *domain.Session) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "SESSION\tOPERATION\tPHASE\tUPDATED\tMESSAGE"); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"%s\t%s\t%s\t%s\t%s\n",
		session.ID,
		session.Spec.Operation(),
		session.Status.Phase,
		session.Status.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		session.Status.Message,
	); err != nil {
		return err
	}

	if session.Spec.Type == domain.SessionTypeBackup && session.Spec.Backup != nil {
		return printBackupSessionDetails(w, session.Spec.Backup)
	}

	if _, err := fmt.Fprintln(
		w,
		"\nPVC\tTRANSFER SCOPE\tRESERVED\tWARM SYNC\tFINAL SYNC\tSOURCE CAPACITY\tSOURCE USED\tDESTINATION CAPACITY\tACTIVE PV",
	); err != nil {
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

		source, destination, sourceUsed, scope := "unknown", "unknown", "unknown", "unknown"
		if index < len(session.Spec.Volumes) {
			volume := session.Spec.Volumes[index]
			source = valueOrUnknown(volume.SourceCapacity)
			sourceUsed = sourceUsageText(volume.SourceUsageKnown, volume.SourceUsedBytes)
			destination = valueOrUnknown(volume.Capacity)
			scope = transferScopeText(volume.TransferScope)
		}

		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%t\t%s\t%s\t%s\t%s\t%s\t%s\n",
			status.SourcePVCName,
			scope,
			status.Reserved,
			warm,
			final,
			source,
			sourceUsed,
			destination,
			activePV,
		); err != nil {
			return err
		}
	}

	return w.Flush()
}

func printBackupSessionDetails(w *tabwriter.Writer, payload *domain.BackupSessionSpec) error {
	mode := "offline"
	if payload.Online {
		mode = "online"
	}

	transferPath := payload.Path
	if transferPath == "" {
		transferPath = "."
	}

	destinationParts := []string{payload.Bucket}
	if payload.Prefix != "" {
		destinationParts = append(destinationParts, payload.Prefix)
	}

	destinationParts = append(destinationParts, payload.Name)

	credentials := "-"
	if payload.CredentialsSecret.Name != "" {
		credentials = payload.CredentialsSecret.Namespace + "/" + payload.CredentialsSecret.Name
	}

	if _, err := fmt.Fprintln(
		w,
		"\nSOURCE PVC\tSOURCE PV\tMODE\tPATH\tDESTINATION\tCREDENTIALS SECRET",
	); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(
		w,
		"%s/%s\t%s\t%s\t%s\ts3://%s/\t%s\n",
		payload.SourcePVC.Namespace,
		payload.SourcePVC.Name,
		payload.SourcePV.Name,
		mode,
		transferPath,
		strings.Join(destinationParts, "/"),
		credentials,
	); err != nil {
		return err
	}

	return w.Flush()
}

func (p Printer) printSessions(sessions []*domain.Session) error {
	w := tabwriter.NewWriter(p.Writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(
		w,
		"SESSION\tOPERATION\tPHASE\tSOURCE NAMESPACE\tUPDATED",
	); err != nil {
		return err
	}

	for _, session := range sessions {
		if _, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\n",
			session.ID,
			session.Spec.Operation(),
			session.Status.Phase,
			session.Spec.SourceNamespace,
			session.Status.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z"),
		); err != nil {
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

func sourceUsageText(known bool, bytes int64) string {
	if !known {
		return "unknown"
	}
	return resource.NewQuantity(bytes, resource.BinarySI).String()
}

func transferPathOrRoot(value string) string {
	if value == "" {
		return domain.VolumeRootPath
	}
	return value
}

func transferScopeText(scope *domain.TransferScope) string {
	if scope == nil {
		return "full"
	}
	return scope.SourcePath + " -> " + scope.DestinationPath
}

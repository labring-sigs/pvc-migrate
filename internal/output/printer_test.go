package output_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/backup"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	printeroutput "github.com/labring-sigs/pvc-migrate/internal/output"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestPrinterJSONAndYAML(t *testing.T) {
	value := map[string]any{"name": "data", "ready": true}
	for _, tc := range []struct {
		name   string
		format printeroutput.Format
		want   string
	}{
		{name: "json", format: printeroutput.JSON, want: "\"name\": \"data\""},
		{name: "yaml", format: printeroutput.YAML, want: "name: data"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := (printeroutput.Printer{Writer: &output, Format: tc.format}).Print(
				value,
			); err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(output.String(), tc.want) ||
				!strings.HasSuffix(output.String(), "\n") {
				t.Fatalf("output = %q", output.String())
			}
		})
	}

	var (
		decoded map[string]any
		output  bytes.Buffer
	)
	if err := (printeroutput.Printer{Writer: &output, Format: printeroutput.JSON}).Print(
		value,
	); err != nil {
		t.Fatal(err)
	}

	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil || decoded["ready"] != true {
		t.Fatalf("decoded=%v error=%v", decoded, err)
	}
}

func TestPrinterRejectsFormatAndPropagatesWriterErrors(t *testing.T) {
	wantErr := errors.New("disk full")

	if err := (printeroutput.Printer{Writer: &bytes.Buffer{}, Format: printeroutput.Format("toml")}).Print(
		map[string]string{},
	); domain.CategoryOf(
		err,
	) != domain.ErrorValidation {
		t.Fatalf("unsupported format error=%v category=%q", err, domain.CategoryOf(err))
	}

	for _, format := range []printeroutput.Format{printeroutput.JSON, printeroutput.YAML, printeroutput.Table} {
		t.Run(string(format), func(t *testing.T) {
			err := (printeroutput.Printer{Writer: failingWriter{err: wantErr}, Format: format}).Print(
				map[string]string{"a": "b"},
			)
			if !errors.Is(err, wantErr) {
				t.Fatalf("Print() error = %v, want %v", err, wantErr)
			}
		})
	}
}

func TestStructuredPrintersPropagateMarshalErrors(t *testing.T) {
	value := make(chan int)
	for _, format := range []printeroutput.Format{printeroutput.JSON, printeroutput.YAML} {
		t.Run(string(format), func(t *testing.T) {
			var output bytes.Buffer
			if err := (printeroutput.Printer{Writer: &output, Format: format}).Print(
				value,
			); err == nil {
				t.Fatal("unsupported value encoded successfully")
			}
		})
	}
}

func TestTablePlanRendersChecksAndVolumes(t *testing.T) {
	plan := &domain.MigrationPlan{
		SessionID:          "mig-1",
		Ready:              false,
		SourceNamespace:    "app",
		TemporaryNamespace: "staging",
		TargetNode:         "node-b",
		Volumes: []domain.PlannedVolume{{
			SourcePVC:      domain.ObjectReference{Namespace: "app", Name: "data"},
			SourcePV:       domain.ObjectReference{Name: "pv-a"},
			DestinationPVC: domain.ObjectReference{Namespace: "staging", Name: "data-mig"},
			SourceCapacity: "8Gi",
			Capacity:       "10Gi",
			StorageClass:   "fast",
			VolumeMode:     "Filesystem",
			TransferScope:  &domain.TransferScope{SourcePath: "mysql/data", DestinationPath: "."},
		}},
		Checks: []domain.Check{
			{Name: "quota", Passed: true, Severity: domain.SeverityInfo, Message: "enough"},
			{
				Name:     "topology",
				Passed:   false,
				Severity: domain.SeverityError,
				Message:  "unavailable",
			},
		},
		StorageCapacity: []domain.StorageCapacityReport{
			{
				StorageClass:      "fast",
				TargetNode:        "node-b",
				RequestedCapacity: "10Gi",
				ReportedCapacity:  "20Gi",
				MaximumVolumeSize: "15Gi",
				Status:            domain.StorageCapacitySufficient,
			},
		},
	}

	var output bytes.Buffer
	if err := (printeroutput.Printer{Writer: &output}).Print(plan); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"SESSION", "mig-1", "app/data", "pv-a", "staging/data-mig", "TRANSFER SCOPE", "mysql/data -> .", "SOURCE CAPACITY", "DESTINATION CAPACITY", "8Gi", "10Gi", "STORAGE CLASS", "20Gi", "15Gi", "sufficient", "PASS", "FAIL", "unavailable"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("table missing %q:\n%s", want, output.String())
		}
	}
}

func TestTableBackupPlanAndResultAreHumanReadable(t *testing.T) {
	plan := &backup.Plan{
		Operation:        "restore",
		ToolImage:        "ghcr.io/example/tool:latest",
		Namespace:        "app",
		PVC:              "data",
		Path:             "database/current",
		Mode:             backup.ModeRestore,
		Consistency:      "destination PVC write; application must be quiesced",
		Destination:      "s3://backups/pv-migrate/daily/",
		ManifestPresent:  true,
		Capacity:         "1Gi",
		VolumeMode:       "Filesystem",
		ToolNode:         "node-a",
		ObjectCount:      3,
		TotalBytes:       42,
		InventorySHA256:  "digest",
		DeleteExtraneous: true,
		Compression:      "none",
		MountedPods:      []string{"app/writer"},
		Warnings:         []string{"restore is explicitly allowed while mounted"},
	}

	var output bytes.Buffer
	if err := (printeroutput.Printer{Writer: &output}).Print(plan); err != nil {
		t.Fatal(err)
	}

	text := output.String()
	for _, want := range []string{"OPERATION", "restore", "app/data", "PATH", "database/current", "s3://backups/pv-migrate/daily/", "MANIFEST", "present", "3", "42", "digest", "MOUNTED PODS", "app/writer", "WARNING", "restore is explicitly allowed"} {
		if !strings.Contains(text, want) {
			t.Fatalf("backup plan table missing %q:\n%s", want, text)
		}
	}

	if strings.HasPrefix(strings.TrimSpace(text), "{") {
		t.Fatalf("backup plan table unexpectedly used printeroutput.JSON fallback:\n%s", text)
	}

	output.Reset()

	result := &backup.Result{
		Operation:   "backup",
		SessionID:   "backup-test",
		Namespace:   "app",
		PVC:         "data",
		Path:        "database/current",
		Mode:        backup.ModeOffline,
		Status:      "completed",
		Destination: "s3://backups/pv-migrate/daily/",
	}
	if err := (printeroutput.Printer{Writer: &output}).Print(result); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"OPERATION", "backup", "backup-test", "app/data", "database/current", "offline", "completed", "s3://backups/pv-migrate/daily/"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("backup result table missing %q:\n%s", want, output.String())
		}
	}

	output.Reset()

	restoreResult := &backup.Result{
		Operation: "restore", OperationID: "restore-attempt", SessionID: "must-not-print",
		Namespace: "app", PVC: "restored", Mode: backup.ModeRestore, Status: "completed",
		Destination: "s3://backups/pv-migrate/daily/",
	}
	if err := (printeroutput.Printer{Writer: &output}).Print(restoreResult); err != nil {
		t.Fatal(err)
	}

	if text := output.String(); !strings.Contains(text, "OPERATION ID") ||
		!strings.Contains(text, "restore-attempt") || strings.Contains(text, "must-not-print") {
		t.Fatalf("restore result table has the wrong identity:\n%s", text)
	}

	restoreResult.SessionID = ""

	encoded, err := json.Marshal(restoreResult)
	if err != nil {
		t.Fatal(err)
	}

	if text := string(encoded); !strings.Contains(text, `"operationID":"restore-attempt"`) ||
		strings.Contains(text, `"sessionID"`) {
		t.Fatalf("restore JSON has the wrong identity: %s", text)
	}
}

func TestTableSessionRendersSyncAndActivationState(t *testing.T) {
	warm := metav1.NewTime(time.Date(2026, time.August, 7, 1, 2, 3, 0, time.UTC))
	activated := metav1.NewTime(time.Date(2026, time.August, 7, 2, 0, 0, 0, time.UTC))
	session := &domain.Session{
		ID: "mig-1",
		Spec: domain.NewSessionSpec(domain.OperationMigrate, domain.SessionCommon{
			Volumes: []domain.VolumeSpec{
				{
					SourcePVC:      domain.ObjectReference{Name: "data"},
					DestinationPV:  domain.ObjectReference{Name: "pv-new"},
					SourceCapacity: "2Gi",
					Capacity:       "3Gi",
					TransferScope: &domain.TransferScope{
						SourcePath:      "data/current",
						DestinationPath: ".",
					},
				},
				{
					SourcePVC:     domain.ObjectReference{Name: "logs"},
					DestinationPV: domain.ObjectReference{Name: "pv-logs"},
				},
			},
		}, domain.WorkloadSpec{Adapter: domain.WorkloadNone}, false, domain.SessionWorkflowOptions{}),
		Status: domain.SessionStatus{
			Phase:     domain.PhaseActivated,
			UpdatedAt: warm,
			Message:   "cutover complete",
			Volumes: []domain.VolumeStatus{
				{
					SourcePVCName: "data",
					Reserved:      true,
					Sync:          domain.SyncState{WarmCompletedAt: &warm},
					Activation:    domain.ActivationState{ActivatedAt: &activated},
				},
				{SourcePVCName: "logs"},
			},
		},
	}

	var output bytes.Buffer
	if err := (printeroutput.Printer{Writer: &output, Format: printeroutput.Table}).Print(
		session,
	); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"mig-1", "Activated", "cutover complete", "2026-08-07T01:02:03Z", "TRANSFER SCOPE", "data/current -> .", "full", "SOURCE CAPACITY", "DESTINATION CAPACITY", "2Gi", "3Gi", "pv-new", "logs", "-"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("session table missing %q:\n%s", want, output.String())
		}
	}
}

func TestTableBackupSessionRendersBackupDetails(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 21, 1, 2, 3, 0, time.UTC))
	spec := domain.NewSessionSpec(
		domain.OperationBackup,
		domain.SessionCommon{SourceNamespace: "app", SessionNamespace: "sessions"},
		domain.WorkloadSpec{Adapter: domain.WorkloadNone},
		true,
		domain.SessionWorkflowOptions{},
	)
	spec.Backup.SourcePVC = domain.ObjectReference{Namespace: "app", Name: "data"}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data"}
	spec.Backup.Bucket = "backups"
	spec.Backup.Prefix = "daily"
	spec.Backup.Name = "point"
	spec.Backup.CredentialsSecret = domain.ObjectReference{
		Namespace: "sessions",
		Name:      "backup-credentials",
	}
	session := &domain.Session{
		ID:   "backup-test",
		Spec: spec,
		Status: domain.SessionStatus{
			Phase:     domain.PhaseCompleted,
			UpdatedAt: now,
			Message:   "backup completed",
		},
	}

	var output bytes.Buffer
	if err := (printeroutput.Printer{Writer: &output, Format: printeroutput.Table}).Print(
		session,
	); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"SOURCE PVC", "SOURCE PV", "MODE", "DESTINATION", "CREDENTIALS SECRET",
		"app/data", "pv-data", "online", "s3://backups/daily/point/", "sessions/backup-credentials",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("backup table missing %q:\n%s", want, output.String())
		}
	}

	if strings.Contains(output.String(), "WARM SYNC") {
		t.Fatalf("backup table contains migration columns:\n%s", output.String())
	}
}

func TestTableSessionShowsSourcePVAfterRollback(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 7, 2, 0, 0, 0, time.UTC))
	session := &domain.Session{
		ID: "mig-rollback",
		Spec: domain.NewSessionSpec(
			domain.OperationMigrate,
			domain.SessionCommon{Volumes: []domain.VolumeSpec{
				{
					SourcePV: domain.ObjectReference{
						Name: "pv-source",
					},
					DestinationPV: domain.ObjectReference{Name: "pv-destination"},
				},
			}},
			domain.WorkloadSpec{Adapter: domain.WorkloadNone},
			false,
			domain.SessionWorkflowOptions{},
		),
		Status: domain.SessionStatus{
			Phase: domain.PhaseRolledBack, UpdatedAt: now,
			Volumes: []domain.VolumeStatus{{
				SourcePVCName: "data",
				Activation:    domain.ActivationState{ActivatedAt: &now, RolledBackAt: &now},
			}},
		},
	}

	var output bytes.Buffer
	if err := (printeroutput.Printer{Writer: &output, Format: printeroutput.Table}).Print(
		session,
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(output.String(), "pv-source") ||
		strings.Contains(output.String(), "pv-destination") {
		t.Fatalf("rollback table=%s", output.String())
	}
}

func TestTableRenameShowsReboundSourcePV(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.August, 7, 2, 0, 0, 0, time.UTC))
	session := &domain.Session{
		ID: "rename",
		Spec: domain.NewSessionSpec(
			domain.OperationRename,
			domain.SessionCommon{Volumes: []domain.VolumeSpec{{
				SourcePV: domain.ObjectReference{Name: "pv-rebound"},
			}}},
			domain.WorkloadSpec{Adapter: domain.WorkloadNone},
			false,
			domain.SessionWorkflowOptions{},
		),
		Status: domain.SessionStatus{
			Phase:     domain.PhaseCompleted,
			UpdatedAt: now,
			Volumes: []domain.VolumeStatus{
				{SourcePVCName: "data", Activation: domain.ActivationState{ActivatedAt: &now}},
			},
		},
	}

	var output bytes.Buffer
	if err := (printeroutput.Printer{Writer: &output, Format: printeroutput.Table}).Print(
		session,
	); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(output.String(), "pv-rebound") {
		t.Fatalf("rename table=%s", output.String())
	}
}

func TestTableSessionListAndGenericFallback(t *testing.T) {
	updated := metav1.NewTime(time.Date(2026, time.August, 7, 1, 2, 3, 0, time.UTC))
	sessions := []*domain.Session{
		{
			ID: "mig-1",
			Spec: domain.NewSessionSpec(
				domain.OperationRename,
				domain.SessionCommon{SourceNamespace: "app"},
				domain.WorkloadSpec{Adapter: domain.WorkloadNone},
				false,
				domain.SessionWorkflowOptions{},
			),
			Status: domain.SessionStatus{Phase: domain.PhaseCompleted, UpdatedAt: updated},
		},
	}

	var table bytes.Buffer
	if err := (printeroutput.Printer{Writer: &table, Format: printeroutput.Table}).Print(
		sessions,
	); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"SOURCE NAMESPACE", "mig-1", "Rename", "Completed", "app"} {
		if !strings.Contains(table.String(), want) {
			t.Fatalf("session list missing %q:\n%s", want, table.String())
		}
	}

	var generic bytes.Buffer
	if err := (printeroutput.Printer{Writer: &generic, Format: printeroutput.Table}).Print(struct {
		Value string `json:"value"`
	}{Value: "fallback"}); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(generic.String(), `"value": "fallback"`) {
		t.Fatalf("generic table fallback = %q", generic.String())
	}
}

func TestTableSpecificWritersPropagateFlushFailure(t *testing.T) {
	wantErr := errors.New("closed")
	updated := metav1.Now()

	values := []any{
		&domain.MigrationPlan{},
		&domain.Session{Status: domain.SessionStatus{UpdatedAt: updated}},
		[]*domain.Session{{Status: domain.SessionStatus{UpdatedAt: updated}}},
	}
	for _, value := range values {
		if err := (printeroutput.Printer{Writer: failingWriter{err: wantErr}, Format: printeroutput.Table}).Print(
			value,
		); !errors.Is(
			err,
			wantErr,
		) {
			t.Fatalf("Print(%T) error = %v", value, err)
		}
	}
}

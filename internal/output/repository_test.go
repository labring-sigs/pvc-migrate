package output_test

import (
	"bytes"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRepositoryTablesRenderConcreteWorkflowsAndRecoveryState(t *testing.T) {
	status := v1alpha1.WorkflowStatus{
		Phase:      "Failed",
		ResumeFrom: "WarmCopied",
		Message:    "repository unavailable",
		History: []v1alpha1.WorkflowHistoryEntry{
			{Phase: "WarmCopied", Message: "transfer checkpoint saved"},
		},
	}
	for _, scenario := range []struct {
		name   string
		object crclient.Object
		column string
	}{
		{
			name: "backup", column: "SOURCE PVC",
			object: &v1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "backup-job", Namespace: "application"},
				Spec: v1alpha1.BackupSpec{
					SourcePVC:     v1alpha1.LocalResourceReference{Name: "data"},
					RepositoryRef: v1alpha1.LocalObjectReference{Name: "archive"},
					Name:          "daily", Path: "database/current", Online: true,
				},
				Status: v1alpha1.BackupStatus{WorkflowStatus: status},
			},
		},
		{
			name: "restore", column: "DESTINATION PVC",
			object: &v1alpha1.Restore{
				ObjectMeta: metav1.ObjectMeta{Name: "restore-job", Namespace: "application"},
				Spec: v1alpha1.RestoreSpec{
					DestinationPVC: v1alpha1.LocalResourceReference{Name: "data"},
					RepositoryRef:  v1alpha1.LocalObjectReference{Name: "archive"},
					Name:           "daily", Path: "database/current", CreatePVC: true,
				},
				Status: v1alpha1.RestoreStatus{WorkflowStatus: status},
			},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var printed bytes.Buffer
			if err := (output.Printer{Writer: &printed}).Print(scenario.object); err != nil {
				t.Fatal(err)
			}

			for _, expected := range []string{
				scenario.object.GetName(), "application", scenario.column, "data", "archive", "daily",
				"database/current", "true", "Failed", "repository unavailable",
				"Resume checkpoint: WarmCopied", "transfer checkpoint saved",
			} {
				if !strings.Contains(printed.String(), expected) {
					t.Fatalf("table missing %q:\n%s", expected, printed.String())
				}
			}

			if strings.HasPrefix(strings.TrimSpace(printed.String()), "{") {
				t.Fatalf("concrete workflow used JSON fallback:\n%s", printed.String())
			}
		})
	}
}

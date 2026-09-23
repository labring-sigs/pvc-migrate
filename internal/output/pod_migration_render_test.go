package output_test

import (
	"bytes"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestPrintClusterPodMigrationRendersTable pins the regression fix: a finished
// pod migration must render the inventory + volume + phase-history tables like
// every other workflow, not fall through to a raw JSON object dump.
func TestPrintClusterPodMigrationRendersTable(t *testing.T) {
	object := &v1alpha1.ClusterPodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "mig-1", Namespace: "pvc-migrate-system"},
		Status: v1alpha1.ClusterPodMigrationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{
				Phase:   "Completed",
				Message: "migration completed and workload is ready",
				History: []v1alpha1.WorkflowHistoryEntry{
					{Phase: "WarmCopied", Message: "pod migration warm copy completed"},
					{Phase: "Completed", Message: "migration completed and workload is ready"},
				},
			},
			Plan: &v1alpha1.ClusterPodMigrationPlan{
				SourceNamespace: "default",
				Volumes: []v1alpha1.VolumeSpec{{
					SourcePVC: v1alpha1.LocalResourceReference{Name: "data-app-0"},
				}},
			},
			Volumes: []v1alpha1.ClusterPodMigrationVolumeStatus{{
				SourcePVCName: "data-app-0",
				Sync: v1alpha1.PodMigrationSyncStatus{
					WarmCompletedAt: &metav1.Time{},
				},
			}},
		},
	}

	var out bytes.Buffer
	if err := (output.Printer{Writer: &out, Format: output.Table}).Print(object); err != nil {
		t.Fatal(err)
	}

	rendered := out.String()
	for _, phrase := range []string{
		"SOURCE PVC", "WARM COPIED", "FINAL SYNCED", "ACTIVATED", "ROLLED BACK",
		"default/data-app-0",
		"TIME", "PHASE", "MESSAGE",
		"WarmCopied",
		"migration completed and workload is ready",
	} {
		if !strings.Contains(rendered, phrase) {
			t.Fatalf("rendered output missing %q:\n%s", phrase, rendered)
		}
	}

	if strings.Contains(rendered, "\"apiVersion\"") {
		t.Fatalf("table format fell back to a JSON dump:\n%s", rendered)
	}
}

func TestPrintPodMigrationNamespacedRendersTable(t *testing.T) {
	object := &v1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "mig-2", Namespace: "default"},
		Status: v1alpha1.PodMigrationStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: "WarmCopied"},
			Plan: &v1alpha1.PodMigrationPlan{
				Volumes: []v1alpha1.VolumeSpec{{
					SourcePVC: v1alpha1.LocalResourceReference{Name: "data"},
				}},
			},
		},
	}

	var out bytes.Buffer
	if err := (output.Printer{Writer: &out, Format: output.Table}).Print(object); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out.String(), "default/data") {
		t.Fatalf("namespaced render missing source PVC:\n%s", out.String())
	}
}

package output_test

import (
	"bytes"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// tableCases covers every branch of printTable with a minimal object so the
// rendering paths (headers, phase columns, lifecycle tables) all execute.
func tableCases() map[string]crclient.Object {
	now := metav1.Now()
	status := func(phase domain.Phase) v1alpha1.WorkflowStatus {
		return v1alpha1.WorkflowStatus{
			Phase:     phase,
			StartedAt: now,
			UpdatedAt: now,
		}
	}
	meta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "app"}
	}

	return map[string]crclient.Object{
		"copy": &v1alpha1.Copy{
			ObjectMeta: meta("copy-1"),
			Status: v1alpha1.CopyStatus{
				WorkflowStatus: status(domain.PhaseWarmCopied),
				Volumes: []v1alpha1.CopyVolumeStatus{{
					VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
						SourcePVCName: "data",
						DestinationPVC: &v1alpha1.LocalResourceReference{
							Name: "data-copy", UID: "dst-uid",
						},
					},
				}},
			},
		},
		"cluster copy": &v1alpha1.ClusterCopy{
			ObjectMeta: metav1.ObjectMeta{Name: "ccopy-1"},
			Status: v1alpha1.ClusterCopyStatus{
				WorkflowStatus: status(domain.PhaseWarmCopied),
				Volumes: []v1alpha1.ClusterCopyVolumeStatus{{
					ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
						SourcePVCName: "data",
						DestinationPVC: &v1alpha1.ObjectReference{
							Namespace: "app", Name: "data-copy", UID: "dst-uid",
						},
					},
				}},
			},
		},
		"migration": &v1alpha1.Migration{
			ObjectMeta: meta("mig-1"),
			Status: v1alpha1.MigrationStatus{
				WorkflowStatus: status(domain.PhaseCompleted),
				Volumes: []v1alpha1.MigrationVolumeStatus{{
					SourcePVCName: "data",
					DestinationPVC: &v1alpha1.LocalResourceReference{
						Name: "data-migrated", UID: "dst-uid",
					},
					Activation: v1alpha1.VolumeActivationStatus{
						ActivePVC: &v1alpha1.LocalResourceReference{
							Name: "data", UID: "active-uid",
						},
					},
				}},
			},
		},
		"cluster migration": &v1alpha1.ClusterMigration{
			ObjectMeta: metav1.ObjectMeta{Name: "cmig-1"},
			Status: v1alpha1.ClusterMigrationStatus{
				WorkflowStatus: status(domain.PhaseCompleted),
				Volumes: []v1alpha1.ClusterMigrationVolumeStatus{{
					SourcePVCName: "data",
					DestinationPVC: &v1alpha1.ObjectReference{
						Namespace: "app", Name: "data-migrated", UID: "dst-uid",
					},
					Activation: v1alpha1.ClusterVolumeActivationStatus{
						ActivePVC: &v1alpha1.ObjectReference{
							Namespace: "app", Name: "data", UID: "active-uid",
						},
					},
				}},
			},
		},
		"reservation": &v1alpha1.Reservation{
			ObjectMeta: meta("rsv-1"),
			Status: v1alpha1.ReservationStatus{
				WorkflowStatus: status(domain.PhaseReserved),
				Volumes: []v1alpha1.ReservationVolumeStatus{{
					VolumeReservationStatus: v1alpha1.VolumeReservationStatus{
						SourcePVCName: "data",
						DestinationPVC: &v1alpha1.LocalResourceReference{
							Name: "data-staged", UID: "dst-uid",
						},
					},
				}},
			},
		},
		"cluster reservation": &v1alpha1.ClusterReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "crsv-1"},
			Status: v1alpha1.ClusterReservationStatus{
				WorkflowStatus: status(domain.PhaseReserved),
				Volumes: []v1alpha1.ClusterReservationVolumeStatus{{
					ClusterVolumeReservationStatus: v1alpha1.ClusterVolumeReservationStatus{
						SourcePVCName: "data",
						DestinationPVC: &v1alpha1.ObjectReference{
							Namespace: "app", Name: "data-staged", UID: "dst-uid",
						},
					},
				}},
			},
		},
		"rename": &v1alpha1.Rename{
			ObjectMeta: meta("rn-1"),
			Status: v1alpha1.RenameStatus{
				WorkflowStatus: status(domain.PhaseCompleted),
			},
		},
		"move": &v1alpha1.Move{
			ObjectMeta: metav1.ObjectMeta{Name: "mv-1"},
			Status: v1alpha1.MoveStatus{
				WorkflowStatus: status(domain.PhaseCompleted),
			},
		},
		"backup": &v1alpha1.Backup{
			ObjectMeta: meta("bk-1"),
			Status: v1alpha1.BackupStatus{
				WorkflowStatus: status(domain.PhaseCompleted),
			},
		},
		"restore": &v1alpha1.Restore{
			ObjectMeta: meta("rst-1"),
			Status: v1alpha1.RestoreStatus{
				WorkflowStatus: status(domain.PhaseCompleted),
			},
		},
	}
}

func TestPrintTableRendersEveryWorkflowKind(t *testing.T) {
	for name, object := range tableCases() {
		var out bytes.Buffer

		printer := output.Printer{Writer: &out, Format: output.Table}

		if err := printer.Print(object); err != nil {
			t.Errorf("%s: Print failed: %v", name, err)
			continue
		}

		if out.Len() == 0 {
			t.Errorf("%s: table output is empty", name)
		}
	}
}

func TestPrintWorkflowInventoryListsAllKinds(t *testing.T) {
	objects := []crclient.Object{
		tableCases()["copy"], tableCases()["cluster copy"],
		tableCases()["migration"], tableCases()["cluster migration"],
		tableCases()["reservation"], tableCases()["cluster reservation"],
	}

	var out bytes.Buffer

	printer := output.Printer{Writer: &out, Format: output.Table}

	if err := printer.Print(objects); err != nil {
		t.Fatalf("inventory print failed: %v", err)
	}

	for _, needle := range []string{"copy-1", "ccopy-1", "mig-1", "cmig-1", "rsv-1", "crsv-1"} {
		if !strings.Contains(out.String(), needle) {
			t.Errorf("inventory output missing %s", needle)
		}
	}
}

func TestPrintMovesRendersTable(t *testing.T) {
	move := &v1alpha1.Move{
		ObjectMeta: metav1.ObjectMeta{Name: "mv-list"},
		Status: v1alpha1.MoveStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseCompleted},
		},
	}

	var out bytes.Buffer

	printer := output.Printer{Writer: &out, Format: output.Table}

	if err := printer.Print([]*v1alpha1.Move{move}); err != nil {
		t.Fatalf("moves print failed: %v", err)
	}

	if !strings.Contains(out.String(), "mv-list") {
		t.Error("moves table missing move id")
	}
}

func TestPrintFormatDispatch(t *testing.T) {
	object := &v1alpha1.Rename{
		ObjectMeta: metav1.ObjectMeta{Name: "rn-fmt", Namespace: "app"},
		Status: v1alpha1.RenameStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseCompleted},
		},
	}

	var jsonOut bytes.Buffer
	if err := (output.Printer{Writer: &jsonOut, Format: output.JSON}).Print(object); err != nil {
		t.Fatalf("json print failed: %v", err)
	}

	if !strings.Contains(jsonOut.String(), "rn-fmt") {
		t.Error("json output should carry the workflow name")
	}

	var yamlOut bytes.Buffer
	if err := (output.Printer{Writer: &yamlOut, Format: output.YAML}).Print(object); err != nil {
		t.Fatalf("yaml print failed: %v", err)
	}

	if !strings.Contains(yamlOut.String(), "rn-fmt") {
		t.Error("yaml output should carry the workflow name")
	}

	var fallback bytes.Buffer

	printer := output.Printer{Writer: &fallback, Format: output.Table}
	if err := printer.Print("unsupported-type"); err != nil {
		t.Fatalf("fallback print failed: %v", err)
	}

	if !strings.Contains(fallback.String(), "unsupported-type") {
		t.Error("unsupported types should fall through to json encoding")
	}

	err := (output.Printer{Writer: &bytes.Buffer{}, Format: output.Format("bogus")}).Print(object)
	if err == nil || !strings.Contains(err.Error(), "unsupported format") {
		t.Errorf("bogus format should be rejected, got %v", err)
	}

	var unstructuredOut bytes.Buffer

	unstructuredValue := &unstructured.Unstructured{}
	unstructuredValue.SetKind("Raw")
	unstructuredValue.SetAPIVersion("v1")

	unstructuredValue.Object["spec"] = map[string]any{"x": "y"}
	if err := (output.Printer{Writer: &unstructuredOut, Format: output.Table}).Print(
		unstructuredValue,
	); err != nil {
		t.Fatalf("unstructured print failed: %v", err)
	}

	if unstructuredOut.Len() == 0 {
		t.Error("unstructured values should still render as json")
	}
}

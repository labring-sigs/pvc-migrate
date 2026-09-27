package cli

import (
	"bytes"
	"io"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The bare `cr backup status` list must honor -n exactly as single-record
// addressing does: an operator scoped to one tenant namespace sees only that
// tenant's workflows, while the unscoped default still lists everything.
func TestCRBackupStatusHonorsNamespaceFlag(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	backupInTenant := func(namespace, name string) *v1alpha1.Backup {
		return &v1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: namespace, UID: types.UID(name),
			},
			Status: v1alpha1.BackupStatus{
				WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhaseCompleted},
			},
		}
	}

	crClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(backupInTenant("tenant-a", "backup-a"), backupInTenant("tenant-b", "backup-b")).
		Build()

	run := func(args ...string) string {
		var stdout bytes.Buffer

		command := NewRoot(Options{
			Out: &stdout, ErrOut: io.Discard, In: strings.NewReader(""),
			runtimeFactory: func(state *rootState) (*commandRuntime, error) {
				return &commandRuntime{
					clients: &kube.Clients{
						Runtime:    crClient,
						Kubernetes: kubefake.NewClientset(),
					},
					printer: printerFor(state),
					controllerKinds: []domain.ControllerKind{
						domain.ControllerKindBackup,
					},
					controllerDiscoveryComplete: true,
				}, nil
			},
		})
		command.SetArgs(args)

		if err := command.Execute(); err != nil {
			t.Fatalf("%v: %v", args, err)
		}

		return stdout.String()
	}

	scoped := run("cr", "backup", "status", "-n", "tenant-a")
	if !strings.Contains(scoped, "backup-a") || strings.Contains(scoped, "backup-b") {
		t.Fatalf("-n tenant-a output must list only tenant-a backups: %q", scoped)
	}

	unscoped := run("cr", "backup", "status")
	if !strings.Contains(unscoped, "backup-a") || !strings.Contains(unscoped, "backup-b") {
		t.Fatalf("default output must list every namespace: %q", unscoped)
	}
}

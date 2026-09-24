package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/output"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type repositorySubmissionClient struct {
	crclient.Client
}

func (c repositorySubmissionClient) Create(
	ctx context.Context,
	object crclient.Object,
	options ...crclient.CreateOption,
) error {
	object.SetUID("workflow-uid")
	return c.Client.Create(ctx, object, options...)
}

func TestCRBackupCreatePrintsControllerFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	failed := &v1alpha1.Backup{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Backup"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "backup",
			Namespace:       "data",
			UID:             "workflow-uid",
			ResourceVersion: "2",
		},
		Status: v1alpha1.BackupStatus{WorkflowStatus: v1alpha1.WorkflowStatus{
			Phase:         domain.PhaseFailed,
			ErrorCategory: string(domain.ErrorConflict),
			Message:       "source identity changed",
		}},
	}

	var out, diagnostics bytes.Buffer

	command := NewRoot(
		Options{
			Out:    &out,
			ErrOut: &diagnostics,
			runtimeFactory: func(state *rootState) (*commandRuntime, error) {
				return &commandRuntime{
					clients: &kube.Clients{
						Runtime: repositorySubmissionClient{
							crfake.NewClientBuilder().WithScheme(scheme).Build(),
						},
						Kubernetes: kubernetesfake.NewClientset(),
						Dynamic:    dynamicfake.NewSimpleDynamicClient(scheme, failed),
					},
					printer: output.Printer{Writer: &out, Format: output.JSON},
				}, nil
			},
		},
	)
	command.SetArgs([]string{
		"cr", "backup", "create",
		"--id", "backup",
		"-n", "data",
		"--source-pvc", "source",
		"--name", "snapshot",
		"--backup-repository", "repository",
		"--yes", "--dry-run=false",
	})

	err := command.Execute()
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("controller failure lost its category: %v", err)
	}

	if !strings.Contains(out.String(), `"phase": "Failed"`) ||
		!strings.Contains(out.String(), "source identity changed") {
		t.Fatalf("final failed CRD was not printed: %s", out.String())
	}
}

func TestCRBackupAndRestoreCreatePersistConcreteCRDs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	client := crfake.NewClientBuilder().WithScheme(scheme).Build()

	var out, diagnostics bytes.Buffer

	command := NewRoot(
		Options{
			Out:    &out,
			ErrOut: &diagnostics,
			runtimeFactory: func(state *rootState) (*commandRuntime, error) {
				return &commandRuntime{
					clients: &kube.Clients{
						Runtime:    client,
						Kubernetes: kubernetesfake.NewClientset(),
					},
					printer: output.Printer{Writer: &out, Format: output.JSON},
				}, nil
			},
		},
	)

	command.SetArgs([]string{
		"cr", "backup", "create",
		"--id", "backup",
		"-n", "data",
		"--source-pvc", "source",
		"--name", "snapshot",
		"--path", "data",
		"--online",
		"--openebs-lvm-enable-shared",
		"--backup-repository", "repository",
		"--yes", "--dry-run=false", "--wait=false",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	backup := &v1alpha1.Backup{}
	if err := client.Get(
		t.Context(),
		crclient.ObjectKey{Namespace: "data", Name: "backup"},
		backup,
	); err != nil {
		t.Fatal(err)
	}

	backupSpec := v1alpha1.BackupSpec{
		SourcePVC:     v1alpha1.LocalResourceReference{Name: "source"},
		RepositoryRef: v1alpha1.LocalObjectReference{Name: "repository"},
		Name:          "snapshot", Path: "data", Online: true, OpenEBSLVMEnableShared: true,
	}
	if backup.Spec != backupSpec || backup.Status.Plan != nil || len(backup.Finalizers) == 0 {
		t.Fatalf("backup request changed or protection missing: %+v", backup)
	}

	if !strings.Contains(out.String(), `"kind": "Backup"`) {
		t.Fatalf("expected CRD output, got %s", out.String())
	}

	out.Reset()

	command.SetArgs([]string{
		"cr", "restore", "create",
		"--id", "restore",
		"-n", "data",
		"--destination-pvc", "target",
		"--name", "snapshot",
		"--path", "restore",
		"--create-pvc",
		"--destination-storage-class", "storage",
		"--destination-capacity", "2Gi",
		"--destination-access-mode", "ReadWriteOnce",
		"--target-node", "worker",
		"--delete-extraneous",
		"--backup-repository", "repository",
		"--yes", "--dry-run=false", "--wait=false",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	restore := &v1alpha1.Restore{}
	if err := client.Get(
		t.Context(),
		crclient.ObjectKey{Namespace: "data", Name: "restore"},
		restore,
	); err != nil {
		t.Fatal(err)
	}

	restoreSpec := v1alpha1.RestoreSpec{
		DestinationPVC: v1alpha1.LocalResourceReference{Name: "target"},
		RepositoryRef:  v1alpha1.LocalObjectReference{Name: "repository"},
		Name:           "snapshot", Path: "restore", CreatePVC: true,
		DestinationStorageClass: "storage", DestinationCapacity: "2Gi",
		DestinationAccessMode: "ReadWriteOnce", TargetNode: "worker", DeleteExtraneous: true,
	}
	if restore.Spec != restoreSpec || restore.Status.Plan != nil || len(restore.Finalizers) == 0 {
		t.Fatalf("restore request changed or protection missing: %+v", restore)
	}

	if !strings.Contains(out.String(), `"kind": "Restore"`) {
		t.Fatalf("expected CRD output, got %s", out.String())
	}
}

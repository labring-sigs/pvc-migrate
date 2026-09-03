package cli

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNewControllerRepositoryStoreUsesRoutingFieldsWithoutCredentials(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "archive", Namespace: "application"},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket:                "backups",
				Prefix:                "controller",
				Provider:              "Minio",
				Endpoint:              "https://object-store.example",
				Region:                "us-east-1",
				ForcePathStyle:        true,
				AllowInsecureEndpoint: false,
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{
					Name: "credentials",
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build()
	r := &rootState{}
	commandRuntime := &commandRuntime{clients: &kube.Clients{Runtime: client}}

	store, err := r.newControllerRepositoryStore(
		t.Context(),
		commandRuntime,
		&bucketFlags{namespace: "application", backupRepository: "archive", name: "daily"},
	)
	if err != nil {
		t.Fatal(err)
	}

	cfg := store.Config()

	if cfg.Bucket != "backups" || cfg.Prefix != "controller" || cfg.Name != "daily" ||
		cfg.Provider != "Minio" || cfg.Endpoint != "https://object-store.example" ||
		cfg.Region != "us-east-1" || !cfg.ForcePathStyle {
		t.Fatalf("repository routing config = %#v", cfg)
	}

	if cfg.AccessKey != "" || cfg.SecretKey != "" || cfg.SessionToken != "" {
		t.Fatalf("controller repository store retained credentials: %#v", cfg)
	}

	if got := store.Destination(); got != "s3://backups/controller/daily/" {
		t.Fatalf("destination = %q", got)
	}
}

func TestNewControllerRepositoryStoreRejectsUnsupportedBackend(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "archive", Namespace: "application"},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypePVC,
			PVC: &v1alpha1.PVCBackupRepositorySpec{
				ClaimRef: v1alpha1.LocalObjectReference{Name: "archive-pvc"},
			},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build()
	r := &rootState{}

	_, err := r.newControllerRepositoryStore(
		t.Context(),
		&commandRuntime{clients: &kube.Clients{Runtime: client}},
		&bucketFlags{namespace: "application", backupRepository: "archive", name: "daily"},
	)
	if err == nil || !containsError(err, "only S3 BackupRepository") {
		t.Fatalf("unsupported backend error = %v", err)
	}
}

func containsError(err error, fragment string) bool {
	return err != nil && strings.Contains(err.Error(), fragment)
}

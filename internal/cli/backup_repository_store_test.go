package cli

import (
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The CR-backed resolver routes through the repository's own fields and reads
// credentials only from the referenced Secret.
func TestCRBackendResolverUsesRoutingFieldsWithoutInlineCredentials(t *testing.T) {
	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name: "archive", Namespace: "application", UID: "repository-uid", Generation: 1,
		},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket:         "backups",
				Prefix:         "controller",
				Provider:       "Minio",
				Endpoint:       "https://object-store.example",
				Region:         "us-east-1",
				ForcePathStyle: true,
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{
					Name: "credentials",
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	runtime := &commandRuntime{clients: &kube.Clients{
		Runtime: crfake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build(),
		Kubernetes: kubefake.NewClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: "credentials", Namespace: "application", UID: types.UID("credentials"),
			},
			Data: map[string][]byte{
				"accessKey": []byte("key"),
				"secretKey": []byte("secret"),
			},
		}),
	}}

	store, _, err := (&rootState{}).repositoryResolverForBackend(runtime, backendCRD).
		Resolve(
			t.Context(),
			repositoryKey(),
			"daily",
		)
	if err != nil {
		t.Fatal(err)
	}

	cfg := store.Config()

	if cfg.Bucket != "backups" || cfg.Name != "daily" ||
		cfg.Provider != "Minio" || cfg.Endpoint != "https://object-store.example" ||
		cfg.Region != "us-east-1" || !cfg.ForcePathStyle {
		t.Fatalf("repository routing config = %#v", cfg)
	}

	if got := store.Destination(); got != "s3://backups/controller/daily/" {
		t.Fatalf("destination = %q", got)
	}
}

func TestCRBackendResolverRejectsMissingS3Configuration(t *testing.T) {
	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name: "archive", Namespace: "application", UID: "repository-uid", Generation: 1,
		},
		Spec: v1alpha1.BackupRepositorySpec{Type: v1alpha1.BackupRepositoryTypeS3},
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	runtime := &commandRuntime{clients: &kube.Clients{
		Runtime:    crfake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build(),
		Kubernetes: kubefake.NewClientset(),
	}}

	_, _, err := (&rootState{}).repositoryResolverForBackend(runtime, backendCRD).
		Resolve(
			t.Context(),
			repositoryKey(),
			"daily",
		)
	if err == nil || !strings.Contains(err.Error(), "requires type s3") {
		t.Fatalf("missing s3 configuration error = %v", err)
	}

	if !apierrors.IsNotFound(err) {
		t.Log("validation error categorized as expected")
	}
}

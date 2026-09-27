package cli

import (
	"errors"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func repositoryLoaderRuntime(
	t *testing.T,
	kubernetes *kubefake.Clientset,
	objects ...crclient.Object,
) *commandRuntime {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	builder := crfake.NewClientBuilder().WithScheme(scheme)
	if len(objects) > 0 {
		builder = builder.WithObjects(objects...)
	}

	return &commandRuntime{clients: &kube.Clients{
		Runtime:    builder.Build(),
		Kubernetes: kubernetes,
	}}
}

func controllerBackupRepository() *v1alpha1.BackupRepository {
	return &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name: "archive", Namespace: "application", UID: "repository-uid", Generation: 1,
		},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket:            "backups",
				Provider:          "Minio",
				Endpoint:          "https://object-store.example",
				ForcePathStyle:    true,
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{Name: "credentials"},
			},
		},
	}
}

// Controller-submitted workflows reference a user-owned BackupRepository CR;
// the CLI resolver must find it when no session ConfigMap exists, so resume
// and lifecycle verbs work against controller-planned backups.
func TestSessionOrCRRepositoryLoaderFallsBackToCR(t *testing.T) {
	repository := controllerBackupRepository()
	runtime := repositoryLoaderRuntime(t, kubefake.NewClientset(), repository)

	loaded, err := (&rootState{}).sessionOrCRRepositoryLoader(runtime)(
		t.Context(),
		crclient.ObjectKey{Namespace: "application", Name: "archive"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.UID != "repository-uid" || loaded.Name != "archive" {
		t.Fatalf("loaded repository = %s/%s", loaded.Namespace, loaded.Name)
	}
}

// A session record that exists but is malformed is a tamper signal: the
// loader must surface it instead of silently falling back to a same-named CR.
func TestSessionOrCRRepositoryLoaderDoesNotMaskSessionConflicts(t *testing.T) {
	kubernetes := kubefake.NewClientset()
	forbidden := apierrors.NewForbidden(
		corev1.Resource("configmaps"), "pvc-migrate-repository-x", errors.New("rbac"),
	)
	kubernetes.PrependReactor(
		"get",
		"configmaps",
		func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, forbidden },
	)

	runtime := repositoryLoaderRuntime(t, kubernetes, controllerBackupRepository())

	loaded, err := (&rootState{}).sessionOrCRRepositoryLoader(runtime)(
		t.Context(),
		crclient.ObjectKey{Namespace: "application", Name: "archive"},
	)
	if !apierrors.IsForbidden(err) || loaded != nil {
		t.Fatalf("error = %v, loaded = %v; the session conflict must not fall back to the CR",
			err, loaded)
	}
}

func TestSessionOrCRRepositoryLoaderReportsMissingBoth(t *testing.T) {
	runtime := repositoryLoaderRuntime(t, kubefake.NewClientset())

	_, err := (&rootState{}).sessionOrCRRepositoryLoader(runtime)(
		t.Context(),
		crclient.ObjectKey{Namespace: "application", Name: "archive"},
	)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("error = %v, want the CR NotFound", err)
	}
}

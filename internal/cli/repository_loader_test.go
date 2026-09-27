package cli

import (
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
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

func repositoryKey() crclient.ObjectKey {
	return crclient.ObjectKey{Namespace: "application", Name: "archive"}
}

// The CR record backend resolves repositories only through BackupRepository
// CRs: a user-owned CR must load, and a missing one reports the CR NotFound
// without consulting session storage.
func TestCRBackendRepositoryLoaderIsCRScoped(t *testing.T) {
	runtime := repositoryLoaderRuntime(t, kubefake.NewClientset(), controllerBackupRepository())

	loaded, err := (&rootState{}).crRepositoryLoader(runtime)(t.Context(), repositoryKey())
	if err != nil {
		t.Fatal(err)
	}

	if loaded.UID != "repository-uid" || loaded.Name != "archive" {
		t.Fatalf("loaded repository = %s/%s", loaded.Namespace, loaded.Name)
	}
}

func TestCRBackendRepositoryLoaderReportsCRNotFound(t *testing.T) {
	runtime := repositoryLoaderRuntime(t, kubefake.NewClientset())

	_, err := (&rootState{}).crRepositoryLoader(runtime)(t.Context(), repositoryKey())
	if !apierrors.IsNotFound(err) {
		t.Fatalf("error = %v, want the CR NotFound", err)
	}
}

// The ConfigMap record backend resolves repositories only through the
// session's inline ConfigMap record: a same-named user-owned CR must not
// silently satisfy the lookup when no session record exists.
func TestConfigMapBackendRepositoryLoaderIgnoresCRs(t *testing.T) {
	runtime := repositoryLoaderRuntime(t, kubefake.NewClientset(), controllerBackupRepository())

	resolver := (&rootState{}).repositoryResolverForBackend(runtime, backendConfigMap)

	_, _, err := resolver.Resolve(t.Context(), repositoryKey(), "daily")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("error = %v, want the session record NotFound despite the same-named CR", err)
	}
}

// The CR record backend resolves the same reference the ConfigMap backend
// ignored: the two loaders are disjoint by construction.
func TestCRBackendResolverReadsTheCR(t *testing.T) {
	kubernetes := kubefake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "credentials", Namespace: "application", UID: "credentials-uid",
		},
		Data: map[string][]byte{
			"accessKey": []byte("key"),
			"secretKey": []byte("secret"),
		},
	})
	runtime := repositoryLoaderRuntime(t, kubernetes, controllerBackupRepository())

	resolver := (&rootState{}).repositoryResolverForBackend(runtime, backendCRD)

	store, binding, err := resolver.Resolve(t.Context(), repositoryKey(), "daily")
	if err != nil {
		t.Fatal(err)
	}

	if binding.UID != "repository-uid" || store.Config().Bucket != "backups" {
		t.Fatalf("binding = %+v", binding)
	}
}

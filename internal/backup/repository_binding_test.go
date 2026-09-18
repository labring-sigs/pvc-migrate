package backup

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func s3BindingRepository() *v1alpha1.BackupRepository {
	return &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "archive",
			Namespace:  "tenant",
			UID:        "repository",
			Generation: 1,
		},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket: "backups", Prefix: "team", Provider: "AWS", Region: "us-east-1",
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{Name: "credentials"},
			},
		},
	}
}

func TestRepositoryResolutionAllowsCredentialRotationAndRejectsReplacement(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	repository := s3BindingRepository()
	reader := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(repository).Build()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "credentials",
			Namespace: "tenant",
			UID:       "credentials-uid",
		},
		Data: map[string][]byte{
			kube.BackupAccessKeyDataKey: []byte("access"),
			kube.BackupSecretKeyDataKey: []byte("secret"),
		},
	}
	client := kubefake.NewClientset(secret)
	key := crclient.ObjectKeyFromObject(repository)

	config, binding, err := ResolveS3Repository(
		t.Context(),
		reader,
		client,
		key,
		"daily",
	)
	if err != nil {
		t.Fatal(err)
	}

	if config.SecretKey != "secret" || binding.UID != repository.UID ||
		binding.Generation != repository.Generation || binding.S3.CredentialsSecretUID != secret.UID {
		t.Fatal("connection and binding lost their independent identities")
	}

	encoded, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(encoded), `"secret"`) ||
		strings.Contains(string(encoded), `"access"`) {
		t.Fatal("binding exposed credentials")
	}

	status := v1alpha1.BackupStatus{}
	writes := 0

	save := func(context.Context) error { writes++; return nil }
	if err := PinRepository(t.Context(), binding, &status.Repository, save); err != nil {
		t.Fatal(err)
	}

	secret.Data[kube.BackupSecretKeyDataKey] = []byte("rotated")
	if _, err := client.CoreV1().
		Secrets("tenant").
		Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	rotatedConfig, rotated, err := ResolveS3Repository(
		t.Context(),
		reader,
		client,
		key,
		"daily",
	)
	if err != nil || rotatedConfig.SecretKey != "rotated" {
		t.Fatalf("credential rotation failed: %v", err)
	}

	if err := PinRepository(
		t.Context(),
		rotated,
		&status.Repository,
		save,
	); err != nil ||
		writes != 1 {
		t.Fatalf("in-place rotation changed durable binding: writes=%d error=%v", writes, err)
	}

	secret.UID = "replacement"
	if _, err := client.CoreV1().
		Secrets("tenant").
		Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	_, replacement, err := ResolveS3Repository(
		t.Context(),
		reader,
		client,
		key,
		"daily",
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := PinRepository(
		t.Context(),
		replacement,
		&status.Repository,
		save,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("replacement credentials accepted: %v", err)
	}

	if writes != 1 || !reflect.DeepEqual(status.Repository, binding) {
		t.Fatal("replacement changed the persisted repository identity")
	}
}

func TestRepositoryRoutingUsesRecoveryPointName(t *testing.T) {
	repository := s3BindingRepository()
	original := repository.DeepCopy()

	// No namespace or cluster segments: the prefix is used verbatim and the
	// recovery-point name stays a separate data-plane segment.
	for _, name := range []string{"daily", "weekly"} {
		config, err := S3RepositoryLocation(repository, name)
		if err != nil {
			t.Fatal(err)
		}

		if config.Prefix != "team" {
			t.Fatalf("prefix = %q, want %q", config.Prefix, "team")
		}

		if config.Name != name {
			t.Fatalf("recovery-point name = %q, want %q", config.Name, name)
		}
	}

	config, err := S3RepositoryLocation(repository, "monthly")
	if err != nil {
		t.Fatal(err)
	}

	if config.Prefix != "team" || config.Name != "monthly" {
		t.Fatalf("repeated routing drifted: %q / %q", config.Prefix, config.Name)
	}

	if !reflect.DeepEqual(repository, original) {
		t.Fatal("routing mutated the repository spec")
	}

	repository.Spec.S3.Prefix = strings.Repeat("p", 1100)
	if _, err := S3RepositoryLocation(repository, "daily"); err == nil {
		t.Fatal("object-store prefix exceeded its limit")
	}
}

func TestRepositoryCheckpointHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	status := v1alpha1.RestoreStatus{}

	err := PinRepository(ctx, nil, &status.Repository, func(context.Context) error {
		t.Fatal("cancelled workflow wrote a binding")
		return nil
	})
	if !errors.Is(err, context.Canceled) || status.Repository != nil {
		t.Fatalf("cancelled pin changed state: status=%+v err=%v", status, err)
	}
}

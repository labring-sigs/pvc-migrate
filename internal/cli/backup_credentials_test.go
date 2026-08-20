package cli_test

import (
	"context"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/cli"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCredentialsSecretOverridesEnvironmentDefaults(t *testing.T) {
	flags := BucketFlagsForTest{
		AccessKey: "environment-access", SecretKey: "environment-secret", Namespace: "default",
		AccessKeyKey: "accessKey", SecretKeyKey: "secretKey", CredentialsSecret: "s3-credentials",
	}

	client := fake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "s3-credentials"},
		Data: map[string][]byte{
			"accessKey": []byte("secret-access"),
			"secretKey": []byte("secret-key"),
		},
	})

	got, err := LoadS3CredentialsForTest(context.Background(), client, flags)
	if err != nil {
		t.Fatal(err)
	}

	if got.AccessKey != "secret-access" || got.SecretKey != "secret-key" {
		t.Fatalf("credentials=%q/%q", got.AccessKey, got.SecretKey)
	}
}

func TestExplicitCredentialsFlagsTakePrecedenceOverSecret(t *testing.T) {
	flags := BucketFlagsForTest{
		AccessKey:         "explicit-access",
		SecretKey:         "explicit-secret",
		Namespace:         "default",
		AccessKeyExplicit: true,
		SecretKeyExplicit: true,
		AccessKeyKey:      "accessKey",
		SecretKeyKey:      "secretKey",
		CredentialsSecret: "s3-credentials",
	}

	client := fake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "s3-credentials"},
		Data: map[string][]byte{
			"accessKey": []byte("secret-access"),
			"secretKey": []byte("secret-key"),
		},
	})

	got, err := LoadS3CredentialsForTest(context.Background(), client, flags)
	if err != nil {
		t.Fatal(err)
	}

	if got.AccessKey != "explicit-access" || got.SecretKey != "explicit-secret" {
		t.Fatalf("credentials=%q/%q", got.AccessKey, got.SecretKey)
	}
}

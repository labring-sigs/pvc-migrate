package cli

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCredentialsSecretOverridesEnvironmentDefaults(t *testing.T) {
	flags := &bucketFlags{
		accessKey:         "environment-access",
		secretKey:         "environment-secret",
		namespace:         "default",
		accessKeyKey:      "accessKey",
		secretKeyKey:      "secretKey",
		credentialsSecret: "s3-credentials",
	}
	client := fake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "s3-credentials"},
		Data:       map[string][]byte{"accessKey": []byte("secret-access"), "secretKey": []byte("secret-key")},
	})
	if err := loadS3Credentials(context.Background(), client, flags); err != nil {
		t.Fatal(err)
	}
	if flags.accessKey != "secret-access" || flags.secretKey != "secret-key" {
		t.Fatalf("credentials=%q/%q", flags.accessKey, flags.secretKey)
	}
}

func TestExplicitCredentialsFlagsTakePrecedenceOverSecret(t *testing.T) {
	flags := &bucketFlags{
		accessKey: "explicit-access", secretKey: "explicit-secret",
		namespace:         "default",
		accessKeyExplicit: true, secretKeyExplicit: true,
		accessKeyKey: "accessKey", secretKeyKey: "secretKey",
		credentialsSecret: "s3-credentials",
	}
	client := fake.NewClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "s3-credentials"},
		Data:       map[string][]byte{"accessKey": []byte("secret-access"), "secretKey": []byte("secret-key")},
	})
	if err := loadS3Credentials(context.Background(), client, flags); err != nil {
		t.Fatal(err)
	}
	if flags.accessKey != "explicit-access" || flags.secretKey != "explicit-secret" {
		t.Fatalf("credentials=%q/%q", flags.accessKey, flags.secretKey)
	}
}

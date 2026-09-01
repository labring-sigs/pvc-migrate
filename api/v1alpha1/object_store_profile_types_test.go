package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestObjectStoreProfileCredentialsSecretIsTrulyOptional(t *testing.T) {
	spec := ObjectStoreProfileSpec{
		Backend: ObjectStoreBackendS3,
		Bucket:  "pvc-backups",
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "credentialsSecret") {
		t.Fatalf("omitted credentialsSecret serialized as an empty object: %s", raw)
	}

	spec.CredentialsSecret = &ObjectStoreCredentialsReference{
		Name: "credentials",
	}
	raw, err = json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"credentialsSecret":{"name":"credentials"}`) {
		t.Fatalf("configured credentialsSecret was omitted: %s", raw)
	}
}

func TestObjectStoreProfileDeepCopyCopiesCredentialsSecret(t *testing.T) {
	original := &ObjectStoreProfile{Spec: ObjectStoreProfileSpec{
		Backend: ObjectStoreBackendS3,
		Bucket:  "pvc-backups",
		CredentialsSecret: &ObjectStoreCredentialsReference{
			Name: "credentials",
		},
	}}
	copy := original.DeepCopy()
	if copy.Spec.CredentialsSecret == original.Spec.CredentialsSecret {
		t.Fatal("DeepCopy reused credentialsSecret pointer")
	}
	copy.Spec.CredentialsSecret.Name = "replacement"
	if original.Spec.CredentialsSecret.Name != "credentials" {
		t.Fatal("DeepCopy mutation changed original credentialsSecret")
	}
}

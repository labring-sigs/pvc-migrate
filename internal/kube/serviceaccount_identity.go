package kube

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
)

// ServiceAccountIdentityFingerprint captures the metadata that can influence
// a cloud workload identity. UID and namespace/name bind the Kubernetes
// subject; labels and annotations cover provider-specific admission hooks and
// IAM role bindings. ResourceVersion and token/image-pull fields are excluded
// because they rotate during normal ServiceAccount operation.
func ServiceAccountIdentityFingerprint(account *corev1.ServiceAccount) string {
	if account == nil {
		return ""
	}

	identity := struct {
		Namespace   string            `json:"namespace"`
		Name        string            `json:"name"`
		UID         string            `json:"uid"`
		Labels      map[string]string `json:"labels,omitempty"`
		Annotations map[string]string `json:"annotations,omitempty"`
		Automount   *bool             `json:"automountServiceAccountToken,omitempty"`
	}{
		Namespace:   account.Namespace,
		Name:        account.Name,
		UID:         string(account.UID),
		Labels:      account.Labels,
		Annotations: account.Annotations,
		Automount:   account.AutomountServiceAccountToken,
	}

	raw, err := json.Marshal(identity)
	if err != nil {
		return ""
	}

	digest := sha256.Sum256(raw)

	return hex.EncodeToString(digest[:])
}

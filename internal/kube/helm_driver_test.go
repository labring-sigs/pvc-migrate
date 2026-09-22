package kube

import "testing"

func TestHelmReleaseStorage(t *testing.T) {
	tests := []struct {
		env  string
		want HelmReleaseStorageClass
	}{
		{"", HelmReleaseStorageSecret},
		{"secret", HelmReleaseStorageSecret},
		{"configmap", HelmReleaseStorageConfigMap},
		{"configmaps", HelmReleaseStorageConfigMap},
		// The driver name is case-insensitive and may carry stray whitespace:
		// the resource estimate and the RBAC checks must agree with whatever
		// spelling the operator used.
		{"Configmap", HelmReleaseStorageConfigMap},
		{" CONFIGMAPS ", HelmReleaseStorageConfigMap},
		{"memory", HelmReleaseStorageEphemeral},
		{"sql", HelmReleaseStorageEphemeral},
	}
	for _, tc := range tests {
		got := HelmReleaseStorage(func(string) (string, bool) { return tc.env, true })
		if got != tc.want {
			t.Errorf("HELM_DRIVER=%q: storage class = %q, want %q", tc.env, got, tc.want)
		}
	}

	if got := HelmReleaseStorage(
		func(string) (string, bool) { return "", false },
	); got != HelmReleaseStorageSecret {
		t.Errorf("unset HELM_DRIVER: storage class = %q, want %q", got, HelmReleaseStorageSecret)
	}
}

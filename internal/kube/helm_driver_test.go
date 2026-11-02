package kube

import "testing"

func TestHelmReleaseStorage(t *testing.T) {
	tests := []struct {
		env  string
		want HelmReleaseStorageClass
	}{
		{"", HelmReleaseStorageSecret},
		{"secret", HelmReleaseStorageSecret},
		{"secrets", HelmReleaseStorageSecret},
		{"secret ", HelmReleaseStorageUnknown},
		{"configmap", HelmReleaseStorageConfigMap},
		{"configmaps", HelmReleaseStorageConfigMap},
		// Helm's driver switch is case-sensitive and rejects unrecognized
		// values, so a differently spelled driver must not be classified as a
		// working storage class the RBAC planner then plans around.
		{"Configmap", HelmReleaseStorageUnknown},
		{" CONFIGMAPS ", HelmReleaseStorageUnknown},
		{"CONFIGMAP", HelmReleaseStorageUnknown},
		{"memory", HelmReleaseStorageEphemeral},
		{"sql", HelmReleaseStorageEphemeral},
		{"postgres", HelmReleaseStorageUnknown},
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

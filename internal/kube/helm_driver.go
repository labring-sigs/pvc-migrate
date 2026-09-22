package kube

import (
	"os"
	"strings"
)

// HelmReleaseStorageClass classifies how the configured Helm release storage
// driver persists releases.
type HelmReleaseStorageClass string

const (
	// HelmReleaseStorageSecret is Helm's default Secret-backed driver.
	HelmReleaseStorageSecret HelmReleaseStorageClass = "secret"
	// HelmReleaseStorageConfigMap persists releases in ConfigMaps.
	HelmReleaseStorageConfigMap HelmReleaseStorageClass = "configmap"
	// HelmReleaseStorageEphemeral drivers (memory, sql) leave no in-cluster
	// release objects for this tool to count.
	HelmReleaseStorageEphemeral HelmReleaseStorageClass = "ephemeral"
)

// HelmReleaseStorage resolves the configured Helm release storage driver.
// The process environment is the source of truth Helm itself honors; tests
// may inject a custom lookup.
func HelmReleaseStorage(lookup func(string) (string, bool)) HelmReleaseStorageClass {
	driver, ok := lookup("HELM_DRIVER")
	if !ok {
		return HelmReleaseStorageSecret
	}

	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "configmap", "configmaps":
		return HelmReleaseStorageConfigMap
	case "memory", "sql":
		return HelmReleaseStorageEphemeral
	default:
		return HelmReleaseStorageSecret
	}
}

// HelmReleaseStorageFromEnv resolves the driver from the process environment.
func HelmReleaseStorageFromEnv() HelmReleaseStorageClass {
	return HelmReleaseStorage(os.LookupEnv)
}

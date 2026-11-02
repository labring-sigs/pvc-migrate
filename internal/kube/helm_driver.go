package kube

import (
	"os"
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
	// HelmReleaseStorageUnknown marks a HELM_DRIVER value Helm itself rejects;
	// Helm's driver switch is case-sensitive and errors on unrecognized values.
	HelmReleaseStorageUnknown HelmReleaseStorageClass = "unknown"
)

// HelmReleaseStorage resolves the configured Helm release storage driver.
// The process environment is the source of truth Helm itself honors — both
// this project and the upstream transfer library pass HELM_DRIVER to Helm's
// storage driver switch, which matches driver names case-sensitively and
// rejects unknown values. Tests may inject a custom lookup.
func HelmReleaseStorage(lookup func(string) (string, bool)) HelmReleaseStorageClass {
	driver, ok := lookup("HELM_DRIVER")
	if !ok {
		return HelmReleaseStorageSecret
	}

	switch driver {
	case "secret", "secrets", "":
		return HelmReleaseStorageSecret
	case "configmap", "configmaps":
		return HelmReleaseStorageConfigMap
	case "memory", "sql":
		return HelmReleaseStorageEphemeral
	default:
		return HelmReleaseStorageUnknown
	}
}

// HelmReleaseStorageFromEnv resolves the driver from the process environment.
func HelmReleaseStorageFromEnv() HelmReleaseStorageClass {
	return HelmReleaseStorage(os.LookupEnv)
}

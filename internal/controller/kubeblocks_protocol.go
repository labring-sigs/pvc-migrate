package controller

import "strings"

const (
	kubeBlocksOperationsAPIGroup = "operations.kubeblocks.io"

	kubeBlocksFieldClusterRef     = "clusterRef"
	kubeBlocksFieldClusterName    = "clusterName"
	kubeBlocksFieldComponents     = "components"
	kubeBlocksFieldComponentSpecs = "componentSpecs"
	kubeBlocksFieldPaused         = "paused"
)

type kubeBlocksPhase string

const (
	kubeBlocksPhaseRunning   kubeBlocksPhase = "Running"
	kubeBlocksPhaseSucceeded kubeBlocksPhase = "Succeed"
	kubeBlocksPhaseFailed    kubeBlocksPhase = "Failed"
	kubeBlocksPhaseCancelled kubeBlocksPhase = "Cancelled"
	kubeBlocksPhaseAborted   kubeBlocksPhase = "Aborted"
	kubeBlocksPhaseStopped   kubeBlocksPhase = "Stopped"
)

func usesComponentScopedKubeBlocksOps(apiVersion string) bool {
	return strings.HasPrefix(apiVersion, kubeBlocksOperationsAPIGroup+"/")
}

func kubeBlocksClusterField(apiVersion string) string {
	if usesComponentScopedKubeBlocksOps(apiVersion) {
		return kubeBlocksFieldClusterName
	}

	return kubeBlocksFieldClusterRef
}

func kubeBlocksOpsFailed(phase string) bool {
	switch kubeBlocksPhase(phase) {
	case kubeBlocksPhaseFailed, kubeBlocksPhaseCancelled, kubeBlocksPhaseAborted:
		return true
	default:
		return false
	}
}

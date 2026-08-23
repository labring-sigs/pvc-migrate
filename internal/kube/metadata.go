package kube

import "maps"

const (
	AppNameLabel      = "app.kubernetes.io/name"
	AppInstanceLabel  = "app.kubernetes.io/instance"
	AppComponentLabel = "app.kubernetes.io/component"
	ManagedByLabel    = "app.kubernetes.io/managed-by"
	ManagedByValue    = "pvc-migrate"
)

const (
	SessionKey                  = "pvc-migrate.io/session"
	PVCProtectionFinalizer      = "kubernetes.io/pvc-protection"
	PVCStorageResizerAnnotation = "volume.kubernetes.io/storage-resizer"
	ResourceRoleLabel           = "pvc-migrate.io/role"
	SessionFinalizer            = "pvc-migrate.io/session-protection"
	OriginalPolicyAnnotation    = "pvc-migrate.io/original-reclaim-policy"
	SourcePVCUIDAnnotation      = "pvc-migrate.io/source-pvc-uid"
	SourcePVAnnotation          = "pvc-migrate.io/source-pv"
	RollbackPVAnnotation        = "pvc-migrate.io/rollback-pv"
	PairedPVAnnotation          = "pvc-migrate.io/paired-pv"
	PauseSessionAnnotation      = "pvc-migrate.io/pause-session"

	pvcBindCompletedAnnotation          = "pv.kubernetes.io/bind-completed"
	pvcBoundByControllerAnnotation      = "pv.kubernetes.io/bound-by-controller"
	pvcSelectedNodeAnnotation           = "volume.kubernetes.io/selected-node"
	pvcStorageProvisionerAnnotation     = "volume.kubernetes.io/storage-provisioner"
	pvcBetaStorageProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	kubectlLastAppliedConfigAnnotation  = "kubectl.kubernetes.io/last-applied-configuration"
)

const (
	ResourceRoleSource              = "source"
	ResourceRoleDestination         = "destination"
	ResourceRoleActive              = "active"
	ResourceRoleRollback            = "rollback"
	ResourceRoleRename              = "rename"
	ResourceRoleReservationConsumer = "reservation-consumer"
	ResourceRoleToolProbe           = "tool-probe"
)

// PVCAnnotationsForRecreation copies user annotations while removing
// controller-owned binding state and stale pvc-migrate ownership.
func PVCAnnotationsForRecreation(input map[string]string) map[string]string {
	result := maps.Clone(input)
	if result == nil {
		return map[string]string{}
	}

	for _, key := range []string{
		pvcBindCompletedAnnotation,
		pvcBoundByControllerAnnotation,
		pvcSelectedNodeAnnotation,
		pvcStorageProvisionerAnnotation,
		pvcBetaStorageProvisionerAnnotation,
		PVCStorageResizerAnnotation,
		kubectlLastAppliedConfigAnnotation,
		SessionKey,
	} {
		delete(result, key)
	}

	return result
}

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
	MetadataDomain              = "migrate.sealos.io"
	PVCProtectionFinalizer      = "kubernetes.io/pvc-protection"
	PVCStorageResizerAnnotation = "volume.kubernetes.io/storage-resizer"
	SessionKey                  = MetadataDomain + "/session"
	ResourceRoleLabel           = MetadataDomain + "/role"
	SessionFinalizer            = MetadataDomain + "/session-protection"
	OriginalPolicyAnnotation    = MetadataDomain + "/original-reclaim-policy"
	SourcePVCUIDAnnotation      = MetadataDomain + "/source-pvc-uid"
	SourcePVAnnotation          = MetadataDomain + "/source-pv"
	RollbackPVAnnotation        = MetadataDomain + "/rollback-pv"
	PairedPVAnnotation          = MetadataDomain + "/paired-pv"
	PauseSessionAnnotation      = MetadataDomain + "/pause-session"

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
// controller-owned binding state and stale migration ownership.
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

func MergeSessionLabels(existing map[string]string, id string) map[string]string {
	result := maps.Clone(existing)
	if result == nil {
		result = make(map[string]string, 2)
	}

	result[ManagedByLabel] = ManagedByValue
	result[SessionKey] = id

	return result
}

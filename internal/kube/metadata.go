package kube

const (
	AppNameLabel      = "app.kubernetes.io/name"
	AppInstanceLabel  = "app.kubernetes.io/instance"
	AppComponentLabel = "app.kubernetes.io/component"
	ManagedByLabel    = "app.kubernetes.io/managed-by"
	ManagedByValue    = "pvc-migrate"
)

const (
	SessionKey               = "pvc-migrate.io/session"
	ResourceRoleLabel        = "pvc-migrate.io/role"
	SessionFinalizer         = "pvc-migrate.io/session-protection"
	OriginalPolicyAnnotation = "pvc-migrate.io/original-reclaim-policy"
	SourcePVCUIDAnnotation   = "pvc-migrate.io/source-pvc-uid"
	SourcePVAnnotation       = "pvc-migrate.io/source-pv"
	RollbackPVAnnotation     = "pvc-migrate.io/rollback-pv"
	PairedPVAnnotation       = "pvc-migrate.io/paired-pv"
	PauseSessionAnnotation   = "pvc-migrate.io/pause-session"
)

const (
	ResourceRoleSource              = "source"
	ResourceRoleDestination         = "destination"
	ResourceRoleActive              = "active"
	ResourceRoleRollback            = "rollback"
	ResourceRoleRename              = "rename"
	ResourceRoleReservationConsumer = "reservation-consumer"
)

package domain

const (
	CoreAPIVersion = "v1"
	AppsAPIVersion = "apps/v1"
	AutoValue      = "auto"

	KubeBlocksAppsGroup = "apps.kubeblocks.io"
)

const (
	KindPod                   = "Pod"
	KindPersistentVolumeClaim = "PersistentVolumeClaim"
	KindPersistentVolume      = "PersistentVolume"
	KindStatefulSet           = "StatefulSet"
	KindDeployment            = "Deployment"
	KindReplicaSet            = "ReplicaSet"
	KindJob                   = "Job"
	KindBackup                = "Backup"
	KindCluster               = "Cluster"
	KindComponent             = "Component"
	KindInstanceSet           = "InstanceSet"
	KindVMCluster             = "VMCluster"
	KindGrafana               = "Grafana"
)

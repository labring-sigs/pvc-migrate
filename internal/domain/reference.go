package domain

const (
	CoreAPIVersion = "v1"
	AppsAPIVersion = "apps/v1"
	AutoValue      = "auto"

	KubeBlocksAppsGroup = "apps.kubeblocks.io"
	// Third-party GVKs accepted by controller workload adapters. Keeping these
	// identities in the domain registry prevents the controller boundary and
	// adapter implementations from drifting apart.
	KubeBlocksClusterAPIVersion         = KubeBlocksAppsGroup + "/v1alpha1"
	KubeBlocksOperationsAPIVersion      = "operations.kubeblocks.io/v1alpha1"
	KubeBlocksWorkloadsAPIVersion       = "workloads.kubeblocks.io/v1alpha1"
	KubeBlocksWorkloadsLegacyAPIVersion = "workloads.kubeblocks.io/v1"
	VictoriaMetricsAPIVersion           = "operator.victoriametrics.com/v1beta1"
	GrafanaAPIVersion                   = "grafana.integreatly.org/v1beta1"
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

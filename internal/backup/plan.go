package backup

type Mode string

const (
	ModeOffline Mode = "offline"
	ModeOnline  Mode = "online"
	ModeRestore Mode = "restore"
)

type TransferPlan struct {
	Operation       string   `json:"operation"                 yaml:"operation"`
	ToolImage       string   `json:"toolImage"                 yaml:"toolImage"`
	Namespace       string   `json:"namespace"                 yaml:"namespace"`
	PVC             string   `json:"pvc"                       yaml:"pvc"`
	Path            string   `json:"path"                      yaml:"path"`
	Mode            Mode     `json:"mode"                      yaml:"mode"`
	Consistency     string   `json:"consistency"               yaml:"consistency"`
	Destination     string   `json:"destination"               yaml:"destination"`
	ManifestPresent bool     `json:"manifestPresent"           yaml:"manifestPresent"`
	MountedPods     []string `json:"mountedPods,omitempty"     yaml:"mountedPods,omitempty"`
	Capacity        string   `json:"capacity"                  yaml:"capacity"`
	VolumeMode      string   `json:"volumeMode"                yaml:"volumeMode"`
	ToolNode        string   `json:"toolNode,omitempty"        yaml:"toolNode,omitempty"`
	PVCUID          string   `json:"pvcUID,omitempty"          yaml:"pvcUID,omitempty"`
	PVUID           string   `json:"pvUID,omitempty"           yaml:"pvUID,omitempty"`
	ObjectCount     int64    `json:"objectCount,omitempty"     yaml:"objectCount,omitempty"`
	TotalBytes      int64    `json:"totalBytes,omitempty"      yaml:"totalBytes,omitempty"`
	InventorySHA256 string   `json:"inventorySHA256,omitempty" yaml:"inventorySHA256,omitempty"`

	Compression string   `json:"compression"        yaml:"compression"`
	Warnings    []string `json:"warnings,omitempty" yaml:"warnings,omitempty"`
}

type (
	BackupPlan struct {
		TransferPlan
	}
	RestorePlan struct {
		TransferPlan
		DeleteExtraneous bool   `json:"deleteExtraneous,omitempty" yaml:"deleteExtraneous,omitempty"`
		CreatePVC        bool   `json:"createPVC,omitempty"        yaml:"createPVC,omitempty"`
		StorageClass     string `json:"storageClass,omitempty"     yaml:"storageClass,omitempty"`
		AccessMode       string `json:"accessMode,omitempty"       yaml:"accessMode,omitempty"`
	}
)

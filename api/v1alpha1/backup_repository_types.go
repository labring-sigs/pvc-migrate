package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRepositorySecretReference points to a Secret in the BackupRepository
// namespace. It intentionally has no namespace field, preventing a tenant CR
// from reading another namespace's credentials.
type BackupRepositorySecretReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string `json:"name" yaml:"name"`
}

// BackupRepository is a user-owned namespaced backup location. Its endpoint,
// bucket and prefix are deliberately user configurable; credentials are read
// from a Secret in the same namespace. Creating this object does not grant
// access to any other namespace or cluster resource.
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=br
type BackupRepository struct {
	metav1.TypeMeta   `                     json:",inline"`
	metav1.ObjectMeta `                     json:"metadata,omitempty"`
	Spec              BackupRepositorySpec `json:"spec"`
}

// BackupRepositoryList contains BackupRepository resources.
// +kubebuilder:object:root=true
type BackupRepositoryList struct {
	metav1.TypeMeta `                   json:",inline"`
	metav1.ListMeta `                   json:"metadata,omitempty"`
	Items           []BackupRepository `json:"items"`
}

// BackupRepositoryType selects the storage backend used by a repository.
// Backend-specific fields are isolated below dedicated configuration objects
// so adding a backend does not create invalid cross-backend combinations.
// +kubebuilder:validation:Enum=s3;pvc
type BackupRepositoryType string

const (
	BackupRepositoryTypeS3  BackupRepositoryType = "s3"
	BackupRepositoryTypePVC BackupRepositoryType = "pvc"
)

// S3BackupRepositorySpec describes an S3-compatible object store. Credentials
// are read from a Secret in the BackupRepository namespace.
// +kubebuilder:validation:XValidation:rule="!has(self.endpoint) || size(self.endpoint) == 0 || self.endpoint.startsWith('https://') || (has(self.allowInsecureEndpoint) && self.allowInsecureEndpoint)",message="object store endpoints must use HTTPS unless allowInsecureEndpoint is true"
// +kubebuilder:validation:XValidation:rule="!has(self.sseKmsKeyID) || size(self.sseKmsKeyID) == 0 || (has(self.serverSideEncryption) && self.serverSideEncryption == 'aws:kms')",message="sseKmsKeyID requires serverSideEncryption aws:kms"
type S3BackupRepositorySpec struct {
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Bucket string `json:"bucket" yaml:"bucket"`
	// +kubebuilder:validation:MaxLength=639
	// +kubebuilder:validation:Pattern=`^$|^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`
	Prefix string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	Provider string `json:"provider,omitempty" yaml:"provider,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	Region                string `json:"region,omitempty"                yaml:"region,omitempty"`
	ForcePathStyle        bool   `json:"forcePathStyle,omitempty"        yaml:"forcePathStyle,omitempty"`
	AllowInsecureEndpoint bool   `json:"allowInsecureEndpoint,omitempty" yaml:"allowInsecureEndpoint,omitempty"`
	// +kubebuilder:validation:Enum=AES256;"aws:kms"
	ServerSideEncryption string `json:"serverSideEncryption,omitempty" yaml:"serverSideEncryption,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	SSEKMSKeyID       string                          `json:"sseKmsKeyID,omitempty" yaml:"sseKmsKeyID,omitempty"`
	CredentialsSecret BackupRepositorySecretReference `json:"credentialsSecret"     yaml:"credentialsSecret"`
}

// PVCBackupRepositorySpec selects a PVC in the repository namespace. The
// controller exposes the schema before its data-plane adapter is enabled so
// clients can adopt one stable API while execution support is added.
type PVCBackupRepositorySpec struct {
	ClaimRef LocalObjectReference `json:"claimRef" yaml:"claimRef"`
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^$|^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`
	// +kubebuilder:validation:XValidation:rule="self == '' || self.split('/').all(segment, segment != '.' && segment != '..')",message="subPath must be a normalized relative path and may not contain '.' or '..' segments"
	SubPath string `json:"subPath,omitempty" yaml:"subPath,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="(self.type == 's3' && has(self.s3) && !has(self.pvc)) || (self.type == 'pvc' && has(self.pvc) && !has(self.s3))",message="exactly one backend configuration must match type"
type BackupRepositorySpec struct {
	Type BackupRepositoryType     `json:"type"          yaml:"type"`
	S3   *S3BackupRepositorySpec  `json:"s3,omitempty"  yaml:"s3,omitempty"`
	PVC  *PVCBackupRepositorySpec `json:"pvc,omitempty" yaml:"pvc,omitempty"`
}

// LocalObjectReference identifies a resource in the same namespace.
type LocalObjectReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string `json:"name" yaml:"name"`
}

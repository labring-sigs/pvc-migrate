package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObjectStoreNamespace is a Kubernetes namespace name permitted by an
// administrator-managed ObjectStoreProfile. A named scalar gives the CRD
// OpenAPI schema an item-level pattern without an expensive CEL loop.
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
type ObjectStoreNamespace string

// ObjectStoreCredentialsReference points to an administrator-owned Secret
// containing the controller's S3 credentials. The Secret is always resolved in
// the controller installation namespace; keeping the namespace out of the
// cluster-scoped profile prevents a profile author from retargeting a
// controller-wide credential lookup.
type ObjectStoreCredentialsReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string `json:"name" yaml:"name"`
}

// ObjectStoreServiceAccountReference binds one tenant namespace to an
// administrator-preprovisioned ServiceAccount. The UID and identity
// fingerprint prevent a tenant from replacing or mutating the account before
// a workflow's first reconcile.
type ObjectStoreServiceAccountReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string `json:"name" yaml:"name"`
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace" yaml:"namespace"`
	// The UID is intentionally required. An administrator can update the
	// profile when rotating an identity by creating a new ServiceAccount.
	// +kubebuilder:validation:MinLength=1
	UID string `json:"uid" yaml:"uid"`
	// IdentityFingerprint is the administrator-observed hash of the
	// ServiceAccount metadata that affects workload identity. Requiring the
	// fingerprint in the profile closes the window where a tenant could mutate
	// IAM annotations or automount settings before the first workflow reconcile.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	IdentityFingerprint string `json:"identityFingerprint" yaml:"identityFingerprint"`
}

// ObjectStoreProfileSpec contains administrator-owned connection and access
// policy shared by controller-backed Backup and Restore workflows. The bucket
// and optional prefix scope are kept with the credentials so tenant workflows
// cannot expand the IAM permission boundary.
// +kubebuilder:validation:XValidation:rule="(has(self.serviceAccountRefs) && size(self.serviceAccountRefs) > 0) || (has(self.credentialsSecret) && size(self.credentialsSecret.name) > 0)",message="static credential profiles require credentialsSecret; workload identity profiles may use the controller's ambient credentials"
// +kubebuilder:validation:XValidation:rule="!has(self.endpoint) || size(self.endpoint) == 0 || self.endpoint.startsWith('https://')",message="object store endpoints must use HTTPS"
// +kubebuilder:validation:XValidation:rule="!has(self.sseKmsKeyID) || size(self.sseKmsKeyID) == 0 || (has(self.serverSideEncryption) && self.serverSideEncryption == 'aws:kms')",message="sseKmsKeyID requires serverSideEncryption aws:kms"
// +kubebuilder:validation:XValidation:rule="(has(self.serviceAccountRefs) && size(self.serviceAccountRefs) > 0) || (has(self.allowStaticCredentialsInTenantNamespace) && self.allowStaticCredentialsInTenantNamespace)",message="configure workload identity or explicitly approve static credentials in the tenant namespace"
// +kubebuilder:validation:XValidation:rule="(has(self.serviceAccountRefs) && size(self.serviceAccountRefs) > 0) || (has(self.allowedNamespaces) && size(self.allowedNamespaces) == 1)",message="static credentials profiles must allow exactly one tenant namespace"
// +kubebuilder:validation:XValidation:rule="!(has(self.serviceAccountRefs) && size(self.serviceAccountRefs) > 0) || !has(self.allowStaticCredentialsInTenantNamespace) || !self.allowStaticCredentialsInTenantNamespace",message="tenant-namespace projection approval is only valid without workload identity"
// +kubebuilder:validation:XValidation:rule="!(has(self.serviceAccountRefs) && size(self.serviceAccountRefs) > 0) || !has(self.allowedNamespaces) || size(self.allowedNamespaces) == 0",message="allowedNamespaces is only valid for static credential profiles"
type ObjectStoreProfileSpec struct {
	// +kubebuilder:validation:Enum=s3
	Backend ObjectStoreBackend `json:"backend" yaml:"backend"`
	// Bucket is the only bucket a workflow may access through this profile.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Bucket string `json:"bucket" yaml:"bucket"`
	// Prefix scopes all workflow object names to this prefix. An empty prefix
	// permits the profile bucket root. Values are slash-separated safe segments.
	// The 639-character limit leaves room for the controller-owned cluster and
	// namespace scope, a 253-character recovery-point name, and the completion
	// manifest suffix within S3's 1024-byte object-key limit.
	// +kubebuilder:validation:MaxLength=639
	// +kubebuilder:validation:Pattern=`^$|^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`
	Prefix string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	Provider string `json:"provider,omitempty" yaml:"provider,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	Endpoint string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	// +kubebuilder:validation:MaxLength=63
	Region         string `json:"region,omitempty" yaml:"region,omitempty"`
	ForcePathStyle bool   `json:"forcePathStyle,omitempty" yaml:"forcePathStyle,omitempty"`
	// +kubebuilder:validation:Enum=AES256;"aws:kms"
	ServerSideEncryption string `json:"serverSideEncryption,omitempty" yaml:"serverSideEncryption,omitempty"`
	// SSEKMSKeyID is administrator-owned and is only used with aws:kms.
	// +kubebuilder:validation:MaxLength=2048
	SSEKMSKeyID string `json:"sseKmsKeyID,omitempty" yaml:"sseKmsKeyID,omitempty"`
	// CredentialsSecret is an optional administrator-owned Secret used by the
	// controller for S3 locks, manifests, and inventory checks. Static
	// transfer profiles must set it because the transfer Pod needs credentials;
	// workload identity profiles may omit it when the controller uses its own
	// ambient cloud identity. It is always read from the controller namespace.
	CredentialsSecret *ObjectStoreCredentialsReference `json:"credentialsSecret,omitempty" yaml:"credentialsSecret,omitempty"`
	// AllowedNamespaces restricts which tenant namespace may reference a static
	// credentials profile. Static credentials are deliberately single-tenant.
	// +kubebuilder:validation:MaxItems=256
	AllowedNamespaces []ObjectStoreNamespace `json:"allowedNamespaces,omitempty" yaml:"allowedNamespaces,omitempty"`
	// ServiceAccountRefs explicitly binds workload identity to administrator-
	// provisioned ServiceAccounts. Every workflow namespace must have one
	// matching entry, including the live ServiceAccount UID.
	// +kubebuilder:validation:MaxItems=256
	ServiceAccountRefs []ObjectStoreServiceAccountReference `json:"serviceAccountRefs,omitempty" yaml:"serviceAccountRefs,omitempty"`
	// AllowStaticCredentialsInTenantNamespace is an explicit administrator
	// acknowledgement that the transfer chart will materialize static S3
	// credentials in the tenant namespace for the duration of a transfer. Keep
	// static profiles single-tenant; use workload identity for shared profiles.
	AllowStaticCredentialsInTenantNamespace bool `json:"allowStaticCredentialsInTenantNamespace,omitempty" yaml:"allowStaticCredentialsInTenantNamespace,omitempty"`
}

// ObjectStoreProfile is an administrator-managed object-store connection.
// It is cluster-scoped so tenants cannot create a profile that points the
// controller at an arbitrary endpoint or Secret.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=osp
type ObjectStoreProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ObjectStoreProfileSpec `json:"spec"`
}

// ObjectStoreProfileList contains ObjectStoreProfile resources.
// +kubebuilder:object:root=true
type ObjectStoreProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ObjectStoreProfile `json:"items"`
}

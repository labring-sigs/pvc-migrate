package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	repositoryDataKey        = "repository.json"
	repositoryOwnerUID       = MetadataDomain + "/repository-owner-uid"
	repositoryOwnerNamespace = MetadataDomain + "/repository-owner-namespace"
	repositoryOwnerName      = MetadataDomain + "/repository-owner-name"
)

// ConfigMapRepositoryStore persists the API repository for session execution
// without requiring CRD installation. Credentials remain in a separate Secret
// in the repository namespace, exactly as for a BackupRepository CR.
type ConfigMapRepositoryStore struct{ client kubernetes.Interface }

func NewConfigMapRepositoryStore(client kubernetes.Interface) *ConfigMapRepositoryStore {
	return &ConfigMapRepositoryStore{client: client}
}

func repositoryConfigMapName(name string) string {
	digest := sha256.Sum256([]byte(name))
	return "pvc-migrate-repository-" + hex.EncodeToString(digest[:16])
}

func (s *ConfigMapRepositoryStore) Load(
	ctx context.Context,
	key crclient.ObjectKey,
) (*v1alpha1.BackupRepository, error) {
	if s.client == nil || key.Namespace == "" || key.Name == "" {
		return nil, errors.New("repository lookup requires a client, namespace and name")
	}

	cm, err := s.client.CoreV1().
		ConfigMaps(key.Namespace).
		Get(ctx, repositoryConfigMapName(key.Name), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return decodeRepositoryConfigMap(cm, key)
}

func decodeRepositoryConfigMap(
	cm *corev1.ConfigMap,
	key crclient.ObjectKey,
) (*v1alpha1.BackupRepository, error) {
	if cm.Name != repositoryConfigMapName(key.Name) || cm.Namespace != key.Namespace ||
		cm.Labels[ManagedByLabel] != ManagedByValue || cm.Labels[ResourceRoleLabel] != "repository" || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable {
		return nil, workflowStoreConflict(
			"load repository",
			"repository ConfigMap identity or ownership changed",
		)
	}

	object := &v1alpha1.BackupRepository{}
	decoder := json.NewDecoder(bytes.NewBufferString(cm.Data[repositoryDataKey]))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(object); err != nil {
		return nil, fmt.Errorf("decode repository: %w", err)
	}

	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("unexpected trailing repository data")
	}

	if object.Name != key.Name || object.Namespace != key.Namespace ||
		object.APIVersion != v1alpha1.GroupVersion.String() || object.Kind != "BackupRepository" || object.Generation != 1 {
		return nil, workflowStoreConflict(
			"load repository",
			"stored API repository differs from the requested object",
		)
	}

	copyWorkflowStorageVersion(object, cm)

	return object, nil
}

// Create binds generated repository metadata to the durable session identity.
// A cross-namespace session is recorded explicitly because Kubernetes owner
// references cannot cross namespace boundaries.
func (s *ConfigMapRepositoryStore) Create(
	ctx context.Context,
	object *v1alpha1.BackupRepository,
	owner v1alpha1.ObjectReference,
) error {
	if s.client == nil || object == nil || object.Name == "" || object.Namespace == "" ||
		object.UID != "" ||
		object.ResourceVersion != "" {
		return errors.New("repository creation requires a client and a new namespaced API object")
	}

	if err := s.validateOwner(ctx, owner); err != nil {
		return err
	}

	snapshot := object.DeepCopy()
	snapshot.APIVersion, snapshot.Kind = v1alpha1.GroupVersion.String(), "BackupRepository"
	snapshot.Generation = 1

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	immutable := true

	cm := &corev1.ConfigMap{
		Immutable: &immutable,
		ObjectMeta: metav1.ObjectMeta{
			Name:      repositoryConfigMapName(object.Name),
			Namespace: object.Namespace,
			Labels: map[string]string{
				ManagedByLabel:    ManagedByValue,
				ResourceRoleLabel: "repository",
			},
			Annotations: map[string]string{
				repositoryOwnerUID:       string(owner.UID),
				repositoryOwnerNamespace: owner.Namespace,
				repositoryOwnerName:      owner.Name,
			},
		},
		Data: map[string]string{repositoryDataKey: string(data)},
	}
	if owner.Namespace == object.Namespace {
		cm.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Name:       SessionConfigMapName(owner.Name),
				UID:        owner.UID,
			},
		}
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	created, err := s.client.CoreV1().
		ConfigMaps(object.Namespace).
		Create(ctx, cm, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	object.TypeMeta, object.Generation = snapshot.TypeMeta, snapshot.Generation
	copyWorkflowStorageVersion(object, created)

	return nil
}

func (s *ConfigMapRepositoryStore) validateOwner(
	ctx context.Context,
	owner v1alpha1.ObjectReference,
) error {
	if owner.Namespace == "" || owner.Name == "" || owner.UID == "" {
		return errors.New("repository creation requires the owning session namespace, name and UID")
	}

	cm, err := s.client.CoreV1().
		ConfigMaps(owner.Namespace).
		Get(ctx, SessionConfigMapName(owner.Name), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if cm.UID != owner.UID || cm.DeletionTimestamp != nil ||
		cm.Labels[ManagedByLabel] != ManagedByValue ||
		cm.Labels[SessionKey] != owner.Name {
		return workflowStoreConflict(
			"create repository",
			"owning session changed or is being deleted",
		)
	}

	return errors.Join(ctx.Err(), LeaseFenceError(ctx))
}

func (s *ConfigMapRepositoryStore) Delete(
	ctx context.Context,
	object *v1alpha1.BackupRepository,
	owner v1alpha1.ObjectReference,
) error {
	if s.client == nil || object == nil || object.Namespace == "" {
		return errors.New("repository deletion requires a client and namespaced object")
	}

	if err := requireWorkflowStorageVersion(object); err != nil {
		return err
	}

	if owner.Name == "" || owner.Namespace == "" || owner.UID == "" {
		return errors.New("repository deletion requires its owning session identity")
	}

	cm, err := s.client.CoreV1().
		ConfigMaps(object.Namespace).
		Get(ctx, repositoryConfigMapName(object.Name), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if _, err := decodeRepositoryConfigMap(cm, crclient.ObjectKeyFromObject(object)); err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(object, cm); err != nil {
		return err
	}

	if cm.Annotations[repositoryOwnerUID] != string(owner.UID) ||
		cm.Annotations[repositoryOwnerNamespace] != owner.Namespace ||
		cm.Annotations[repositoryOwnerName] != owner.Name {
		return workflowStoreConflict("delete repository", "repository belongs to another session")
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	err = s.client.CoreV1().
		ConfigMaps(object.Namespace).
		Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &object.UID, ResourceVersion: &object.ResourceVersion}})

	return crclient.IgnoreNotFound(err)
}

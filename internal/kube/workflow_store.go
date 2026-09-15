package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkflowStore persists the CRD object owned by one operation. The factory
// binds decoding to that operation; no execution payload is reconstructed.
type WorkflowStore[T crclient.Object] interface {
	Create(ctx context.Context, object T) error
	Load(ctx context.Context, key crclient.ObjectKey) (T, error)
	List(ctx context.Context, namespace string) ([]T, error)
	Save(ctx context.Context, object T) error
	Delete(ctx context.Context, object T) error
}

// LoadConfigMapWorkflow resolves only the stored API kind. Operation callers
// dispatch once at their input boundary and then keep the concrete CRD type.
func LoadConfigMapWorkflow(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
) (crclient.Object, error) {
	if client == nil || namespace == "" || name == "" {
		return nil, errors.New("workflow lookup requires a client, namespace and name")
	}

	cm, err := client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, SessionConfigMapName(name), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var header metav1.TypeMeta
	if err := json.Unmarshal([]byte(cm.Data[SessionDataKey]), &header); err != nil {
		return nil, fmt.Errorf("read workflow kind: %w", err)
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	decoded, err := scheme.New(header.GroupVersionKind())
	if err != nil {
		return nil, fmt.Errorf("resolve stored workflow kind: %w", err)
	}

	object, ok := decoded.(crclient.Object)
	if !ok {
		return nil, errors.New("stored API kind is not a workflow object")
	}

	store, err := NewConfigMapWorkflowStore(
		client,
		namespace,
		func() crclient.Object { return object },
	)
	if err != nil {
		return nil, err
	}

	return store.decode(cm, crclient.ObjectKey{Name: name})
}

// ConfigMapWorkflowStore stores a concrete CRD in a ConfigMap. Its namespace
// is a storage location, independent of the workflow's resource namespace.
type ConfigMapWorkflowStore[T crclient.Object] struct {
	client    kubernetes.Interface
	namespace string
	newObject func() T
	gvk       schema.GroupVersionKind
}

func NewConfigMapWorkflowStore[T crclient.Object](
	client kubernetes.Interface,
	namespace string,
	newObject func() T,
) (*ConfigMapWorkflowStore[T], error) {
	if newObject == nil {
		return nil, errors.New("workflow storage requires an object factory")
	}

	gvk, err := workflowStoreKind(newObject())
	if err != nil {
		return nil, err
	}

	if client == nil || namespace == "" {
		return nil, errors.New("workflow storage requires a client and namespace")
	}

	return &ConfigMapWorkflowStore[T]{
		client:    client,
		namespace: namespace,
		newObject: newObject,
		gvk:       gvk,
	}, nil
}

func workflowStoreKind(object crclient.Object) (schema.GroupVersionKind, error) {
	if object == nil || reflect.ValueOf(object).Kind() != reflect.Pointer ||
		reflect.ValueOf(object).IsNil() {
		return schema.GroupVersionKind{}, errors.New(
			"workflow factory must return a non-nil object pointer",
		)
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return schema.GroupVersionKind{}, err
	}

	kinds, _, err := scheme.ObjectKinds(object)
	if err != nil || len(kinds) != 1 || workflowKind(object) == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("unsupported workflow object %T", object)
	}

	return kinds[0], nil
}

func (s *ConfigMapWorkflowStore[T]) Create(ctx context.Context, object T) error {
	if object.GetName() == "" || object.GetUID() != "" || object.GetResourceVersion() != "" {
		return workflowStoreConflict(
			"create",
			"a new workflow requires a name and no persistence identity",
		)
	}

	data, err := s.encode(object)
	if err != nil {
		return err
	}

	created, err := s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: s.namespace,
			Name:      SessionConfigMapName(object.GetName()),
			Labels:    sessionLabels(object.GetName()),
		},
		Data: map[string]string{SessionDataKey: string(data)},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	object.GetObjectKind().SetGroupVersionKind(s.gvk)
	copyWorkflowStorageVersion(object, created)

	return nil
}

func (s *ConfigMapWorkflowStore[T]) Load(ctx context.Context, key crclient.ObjectKey) (T, error) {
	var zero T

	cm, err := s.client.CoreV1().
		ConfigMaps(s.namespace).
		Get(ctx, SessionConfigMapName(key.Name), metav1.GetOptions{})
	if err != nil {
		return zero, err
	}

	return s.decode(cm, key)
}

func (s *ConfigMapWorkflowStore[T]) Save(ctx context.Context, object T) error {
	if err := requireWorkflowStorageVersion(object); err != nil {
		return err
	}

	existing, err := s.client.CoreV1().
		ConfigMaps(s.namespace).
		Get(ctx, SessionConfigMapName(object.GetName()), metav1.GetOptions{})
	if err != nil {
		return err
	}

	previous, err := s.decode(existing, crclient.ObjectKeyFromObject(object))
	if err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(object, existing); err != nil {
		return err
	}

	if existing.DeletionTimestamp != nil {
		return workflowStoreConflict("save", "workflow ConfigMap is being deleted")
	}

	if err := unchangedWorkflowDefinition(previous, object); err != nil {
		return err
	}

	data, err := s.encode(object)
	if err != nil {
		return err
	}

	updated := existing.DeepCopy()
	updated.Data = map[string]string{SessionDataKey: string(data)}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	updated, err = s.client.CoreV1().
		ConfigMaps(s.namespace).
		Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	copyWorkflowStorageVersion(object, updated)

	return nil
}

func (s *ConfigMapWorkflowStore[T]) Delete(ctx context.Context, object T) error {
	if err := requireWorkflowStorageVersion(object); err != nil {
		return err
	}

	existing, err := s.client.CoreV1().
		ConfigMaps(s.namespace).
		Get(ctx, SessionConfigMapName(object.GetName()), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	// Ownership is asserted from the storage labels even when the stored
	// payload predates the current schema and can no longer be decoded.
	if existing.Labels[ManagedByLabel] != ManagedByValue ||
		existing.Labels[SessionKey] != object.GetName() {
		return workflowStoreConflict(
			"delete",
			"ConfigMap ownership does not match the workflow",
		)
	}

	if _, err := s.decode(existing, crclient.ObjectKeyFromObject(object)); err == nil {
		// A decodable record enforces the caller's fencing identity. Legacy
		// or corrupt payloads skip the check so deletion still converges.
		if err := checkWorkflowStorageVersion(object, existing); err != nil {
			return err
		}
	}

	// Records written by pre-refactor releases carry session-protection
	// finalizers in the legacy metadata domain. Strip them so deletion
	// converges without the old binary.
	if len(existing.Finalizers) > 0 {
		withoutProtection := make([]string, 0, len(existing.Finalizers))
		for _, finalizer := range existing.Finalizers {
			switch finalizer {
			case SessionFinalizer, LegacySessionFinalizer:
				continue
			default:
				withoutProtection = append(withoutProtection, finalizer)
			}
		}

		if len(withoutProtection) != len(existing.Finalizers) {
			existing.Finalizers = withoutProtection

			existing, err = s.client.CoreV1().
				ConfigMaps(s.namespace).
				Update(ctx, existing, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}
	}

	uid, version := existing.GetUID(), existing.GetResourceVersion()
	err = s.client.CoreV1().ConfigMaps(s.namespace).Delete(ctx, existing.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
	})

	return crclient.IgnoreNotFound(err)
}

func (s *ConfigMapWorkflowStore[T]) encode(object T) ([]byte, error) {
	snapshot := object.DeepCopyObject()
	snapshot.GetObjectKind().SetGroupVersionKind(s.gvk)
	return json.Marshal(snapshot)
}

func (s *ConfigMapWorkflowStore[T]) decode(
	cm *corev1.ConfigMap,
	key crclient.ObjectKey,
) (T, error) {
	var zero T
	if cm.Name != SessionConfigMapName(key.Name) || cm.Namespace != s.namespace ||
		cm.Labels[ManagedByLabel] != ManagedByValue || cm.Labels[SessionKey] != key.Name {
		return zero, workflowStoreConflict(
			"load",
			"ConfigMap ownership does not match the workflow",
		)
	}

	object := s.newObject()
	decoder := json.NewDecoder(bytes.NewBufferString(cm.Data[SessionDataKey]))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(object); err != nil {
		return zero, fmt.Errorf("decode %s workflow: %w", s.gvk.Kind, err)
	}

	if err := decoder.Decode(new(any)); err != io.EOF {
		return zero, errors.New("unexpected trailing workflow data")
	}

	if object.GetObjectKind().GroupVersionKind() != s.gvk || object.GetName() != key.Name ||
		(key.Namespace != "" && object.GetNamespace() != key.Namespace) {
		return zero, workflowStoreConflict(
			"load",
			"stored workflow kind or identity does not match the requested object",
		)
	}

	copyWorkflowStorageVersion(object, cm)

	return object, nil
}

func (s *ConfigMapWorkflowStore[T]) List(ctx context.Context, namespace string) ([]T, error) {
	items, err := s.client.CoreV1().
		ConfigMaps(s.namespace).
		List(ctx, metav1.ListOptions{LabelSelector: ManagedByLabel + "=" + ManagedByValue + "," + SessionKey})
	if err != nil {
		return nil, err
	}

	result := make([]T, 0)
	for i := range items.Items {
		cm := &items.Items[i]

		var header metav1.TypeMeta
		if err := json.Unmarshal([]byte(cm.Data[SessionDataKey]), &header); err != nil {
			return nil, err
		}

		if header.GroupVersionKind() != s.gvk {
			continue
		}

		object, err := s.decode(cm, crclient.ObjectKey{Name: cm.Labels[SessionKey]})
		if err != nil {
			return nil, err
		}

		if namespace == "" || object.GetNamespace() == namespace {
			result = append(result, object)
		}
	}

	return result, nil
}

// CRDWorkflowStore writes only the status subresource during execution. The
// same concrete object can be used with ConfigMapWorkflowStore by the CLI.
type CRDWorkflowStore[T crclient.Object] struct {
	client    crclient.Client
	newObject func() T
	gvk       schema.GroupVersionKind
}

func NewCRDWorkflowStore[T crclient.Object](
	client crclient.Client,
	newObject func() T,
) (*CRDWorkflowStore[T], error) {
	if newObject == nil {
		return nil, errors.New("workflow storage requires an object factory")
	}

	gvk, err := workflowStoreKind(newObject())
	if err != nil {
		return nil, err
	}

	if client == nil {
		return nil, errors.New("workflow storage requires a client")
	}

	return &CRDWorkflowStore[T]{client: client, newObject: newObject, gvk: gvk}, nil
}

func (s *CRDWorkflowStore[T]) Create(ctx context.Context, object T) error {
	if object.GetName() == "" || object.GetUID() != "" || object.GetResourceVersion() != "" {
		return workflowStoreConflict(
			"create",
			"a new workflow requires a name and no persistence identity",
		)
	}

	snapshot := s.newObject()
	if err := copyWorkflowObject(object, snapshot); err != nil {
		return err
	}

	snapshot.GetObjectKind().SetGroupVersionKind(s.gvk)
	snapshot.SetFinalizers(ensureSessionFinalizer(snapshot.GetFinalizers()))

	labels := snapshot.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[ManagedByLabel], labels[SessionKey] = ManagedByValue, object.GetName()
	snapshot.SetLabels(labels)

	if err := s.client.Create(ctx, snapshot); err != nil {
		return err
	}

	snapshot.GetObjectKind().SetGroupVersionKind(s.gvk)
	// API admission may default spec fields. Keep those exact values for
	// subsequent immutable-spec checks and execution-intent fingerprints.
	return copyWorkflowObject(snapshot, object)
}

// EnsureProtection protects declaratively submitted workflows before planning
// can mutate storage. Only metadata is updated; the caller's status is retained.
func (s *CRDWorkflowStore[T]) EnsureProtection(ctx context.Context, object T) error {
	if err := requireWorkflowStorageVersion(object); err != nil {
		return err
	}

	current, err := s.Load(ctx, crclient.ObjectKeyFromObject(object))
	if err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(object, current); err != nil {
		return err
	}

	if current.GetDeletionTimestamp() != nil {
		return workflowStoreConflict("protect", "workflow is being deleted")
	}

	finalizers := ensureSessionFinalizer(current.GetFinalizers())
	if !reflect.DeepEqual(finalizers, current.GetFinalizers()) {
		current.SetFinalizers(finalizers)

		if err := s.client.Update(ctx, current); err != nil {
			return err
		}
	}

	object.SetFinalizers(append([]string(nil), current.GetFinalizers()...))
	copyWorkflowStorageVersion(object, current)

	return nil
}

func (s *CRDWorkflowStore[T]) Load(ctx context.Context, key crclient.ObjectKey) (T, error) {
	object := s.newObject()

	err := s.client.Get(ctx, key, object)
	if err == nil {
		object.GetObjectKind().SetGroupVersionKind(s.gvk)
	}

	return object, err
}

func (s *CRDWorkflowStore[T]) List(ctx context.Context, namespace string) ([]T, error) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}

	value, err := scheme.New(s.gvk.GroupVersion().WithKind(s.gvk.Kind + "List"))
	if err != nil {
		return nil, err
	}

	list, ok := value.(crclient.ObjectList)
	if !ok {
		return nil, fmt.Errorf("%s has no registered object list", s.gvk.Kind)
	}

	if err := s.client.List(ctx, list, crclient.InNamespace(namespace)); err != nil {
		return nil, err
	}

	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, err
	}

	result := make([]T, 0, len(items))
	for _, item := range items {
		object, ok := item.(T)
		if !ok {
			return nil, fmt.Errorf("unexpected %T in %s list", item, s.gvk.Kind)
		}

		object.GetObjectKind().SetGroupVersionKind(s.gvk)
		result = append(result, object)
	}

	return result, nil
}

func (s *CRDWorkflowStore[T]) Save(ctx context.Context, object T) error {
	if err := requireWorkflowStorageVersion(object); err != nil {
		return err
	}

	previous, err := s.Load(ctx, crclient.ObjectKeyFromObject(object))
	if err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(object, previous); err != nil {
		return err
	}

	if err := unchangedWorkflowDefinition(previous, object); err != nil {
		return err
	}

	snapshot := s.newObject()
	if err := copyWorkflowObject(object, snapshot); err != nil {
		return err
	}

	if err := errors.Join(ctx.Err(), LeaseFenceError(ctx)); err != nil {
		return err
	}

	if err := s.client.Status().Update(ctx, snapshot); err != nil {
		return err
	}

	copyWorkflowStorageVersion(object, snapshot)

	return nil
}

func (s *CRDWorkflowStore[T]) Delete(ctx context.Context, object T) error {
	if err := requireWorkflowStorageVersion(object); err != nil {
		return err
	}

	previous, err := s.Load(ctx, crclient.ObjectKeyFromObject(object))
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if err := checkWorkflowStorageVersion(object, previous); err != nil {
		return err
	}
	// Callers finish resource and Lease cleanup before releasing protection.
	// A failed metadata update therefore leaves a recoverable workflow.
	previous.SetFinalizers(removeSessionFinalizer(previous.GetFinalizers()))

	if err := s.client.Update(ctx, previous); err != nil {
		return err
	}

	copyWorkflowStorageVersion(object, previous)

	if previous.GetDeletionTimestamp() != nil {
		return nil
	}

	uid, version := previous.GetUID(), previous.GetResourceVersion()
	err = s.client.Delete(ctx, previous, &crclient.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version},
	})

	return crclient.IgnoreNotFound(err)
}

func copyWorkflowStorageVersion(object, storage metav1.Object) {
	object.SetUID(storage.GetUID())
	object.SetResourceVersion(storage.GetResourceVersion())
	object.SetCreationTimestamp(storage.GetCreationTimestamp())
}

func requireWorkflowStorageVersion(object metav1.Object) error {
	if object.GetName() == "" || object.GetUID() == "" || object.GetResourceVersion() == "" {
		return workflowStoreConflict("write", "workflow name, UID and resourceVersion are required")
	}
	return nil
}

func checkWorkflowStorageVersion(object, current metav1.Object) error {
	if object.GetUID() != current.GetUID() ||
		object.GetResourceVersion() != current.GetResourceVersion() {
		return workflowStoreConflict("write", "workflow changed after it was loaded")
	}

	return nil
}

func unchangedWorkflowDefinition(previous, next crclient.Object) error {
	before, err := runtime.DefaultUnstructuredConverter.ToUnstructured(previous)
	if err != nil {
		return err
	}

	after, err := runtime.DefaultUnstructuredConverter.ToUnstructured(next)
	if err != nil {
		return err
	}

	if !apiequality.Semantic.DeepEqual(before["spec"], after["spec"]) {
		return workflowStoreConflict("save", "workflow spec changed during execution")
	}

	return nil
}

func copyWorkflowObject(source, destination crclient.Object) error {
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(source)
	if err != nil {
		return err
	}

	return runtime.DefaultUnstructuredConverter.FromUnstructured(data, destination)
}

func workflowStoreConflict(action, message string) error {
	return domain.NewError(domain.ErrorConflict, action+" workflow", message)
}

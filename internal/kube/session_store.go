package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	SessionDataKey    = "session.json"
	SessionNamePrefix = "pvc-migrate-session-"
)

type SessionStore interface {
	Create(context.Context, *domain.Session) error
	Get(context.Context, string, string) (*domain.Session, error)
	Update(context.Context, *domain.Session) error
	List(context.Context, string) ([]*domain.Session, error)
	Delete(context.Context, *domain.Session) error
}

// SessionLocker serializes mutating operations for one persisted session.
// Implementations must fence the lease holder when ownership is lost so that
// callers stop issuing Kubernetes mutations through the returned context.
type SessionLocker interface {
	AcquireSessionLock(context.Context, string, string) (SessionLock, error)
}

// SessionLeaseCleaner removes the persisted lock after a session is deleted.
// Implementations may keep leases for lock reuse when a session remains.
type SessionLeaseCleaner interface {
	DeleteSessionLease(context.Context, string, string) error
}

// SessionLock is a renewable, process-owned lock for one session.
type SessionLock interface {
	Context() context.Context
	Err() error
	Release(context.Context) error
}

type ConfigMapSessionStore struct {
	client kubernetes.Interface
}

func NewConfigMapSessionStore(client kubernetes.Interface) *ConfigMapSessionStore {
	return &ConfigMapSessionStore{client: client}
}

func SessionConfigMapName(id string) string {
	return SessionNamePrefix + id
}

func (s *ConfigMapSessionStore) Create(ctx context.Context, session *domain.Session) error {
	if err := session.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(session)
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "create session", "encode session", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SessionConfigMapName(session.ID),
			Namespace: session.Spec.SessionNamespace,
			Labels: map[string]string{
				ManagedByLabel: ManagedByValue,
				SessionKey:     session.ID,
			},
			Finalizers: []string{SessionFinalizer},
		},
		Data: map[string]string{SessionDataKey: string(data)},
	}
	created, err := s.client.CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return domain.WrapError(domain.ErrorConflict, "create session", fmt.Sprintf("session %s already exists", session.ID), err)
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "create session", "create ConfigMap", err)
	}
	session.ResourceVersion = created.ResourceVersion
	return nil
}

func (s *ConfigMapSessionStore) Get(ctx context.Context, namespace, id string) (*domain.Session, error) {
	cm, err := s.client.CoreV1().ConfigMaps(namespace).Get(ctx, SessionConfigMapName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, domain.WrapError(domain.ErrorValidation, "get session", fmt.Sprintf("session %s/%s does not exist", namespace, id), err)
	}
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "get session", "read ConfigMap", err)
	}
	return decodeSession(cm)
}

func (s *ConfigMapSessionStore) Update(ctx context.Context, session *domain.Session) error {
	if session.ResourceVersion == "" {
		return domain.NewError(domain.ErrorConflict, "update session", "session resourceVersion is empty")
	}
	if err := session.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(session)
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "update session", "encode session", err)
	}
	existing, err := s.client.CoreV1().ConfigMaps(session.Spec.SessionNamespace).Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorConflict, "update session", "session ConfigMap disappeared", err)
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "update session", "read ConfigMap metadata", err)
	}
	if _, err := decodeSession(existing); err != nil {
		return domain.WrapError(domain.ErrorConflict, "update session", "ConfigMap ownership does not match the session", err)
	}
	if existing.DeletionTimestamp != nil {
		return domain.NewError(domain.ErrorConflict, "update session", "session ConfigMap is pending deletion")
	}
	cm := existing.DeepCopy()
	cm.ResourceVersion = session.ResourceVersion
	if cm.Labels == nil {
		cm.Labels = map[string]string{}
	}
	cm.Labels[ManagedByLabel] = ManagedByValue
	cm.Labels[SessionKey] = session.ID
	cm.Finalizers = ensureSessionFinalizer(cm.Finalizers)
	cm.Data = map[string]string{SessionDataKey: string(data)}
	updated, err := s.client.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return domain.WrapError(domain.ErrorConflict, "update session", "session changed by another process", err)
	}
	if apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorConflict, "update session", "session ConfigMap disappeared", err)
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "update session", "update ConfigMap", err)
	}
	session.ResourceVersion = updated.ResourceVersion
	return nil
}

func (s *ConfigMapSessionStore) List(ctx context.Context, namespace string) ([]*domain.Session, error) {
	selector := labels.Set{ManagedByLabel: ManagedByValue}.String()
	items, err := s.client.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "list sessions", "list ConfigMaps", err)
	}
	result := make([]*domain.Session, 0, len(items.Items))
	for i := range items.Items {
		if _, exists := items.Items[i].Data[SessionDataKey]; !exists {
			continue
		}
		session, decodeErr := decodeSession(&items.Items[i])
		if decodeErr != nil {
			return nil, decodeErr
		}
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Status.StartedAt.Before(&result[j].Status.StartedAt)
	})
	return result, nil
}

func (s *ConfigMapSessionStore) Delete(ctx context.Context, session *domain.Session) error {
	cm, err := s.client.CoreV1().ConfigMaps(session.Spec.SessionNamespace).Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "delete session", "read ConfigMap", err)
	}
	if _, err := decodeSession(cm); err != nil {
		return domain.WrapError(domain.ErrorConflict, "delete session", "ConfigMap ownership does not match the session", err)
	}
	if containsString(cm.Finalizers, SessionFinalizer) {
		updated := cm.DeepCopy()
		updated.Finalizers = removeString(updated.Finalizers, SessionFinalizer)
		latest, err := s.client.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			return domain.WrapError(domain.ErrorConflict, "delete session", "session ConfigMap changed while removing protection finalizer", err)
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return domain.WrapError(domain.ErrorKubernetes, "delete session", "remove session protection finalizer", err)
		}
		if err == nil {
			cm = latest
		}
	}
	uid, resourceVersion := cm.UID, cm.ResourceVersion
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}}
	if err := s.client.CoreV1().ConfigMaps(cm.Namespace).Delete(ctx, cm.Name, options); err != nil && !apierrors.IsNotFound(err) {
		return domain.WrapError(domain.ErrorKubernetes, "delete session", fmt.Sprintf("delete ConfigMap UID %s", cm.UID), err)
	}
	return nil
}

func ensureSessionFinalizer(finalizers []string) []string {
	finalizers = append([]string(nil), finalizers...)
	if !containsString(finalizers, SessionFinalizer) {
		finalizers = append(finalizers, SessionFinalizer)
	}
	return finalizers
}

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func removeString(values []string, value string) []string {
	result := values[:0]
	for _, item := range values {
		if item != value {
			result = append(result, item)
		}
	}
	return result
}

func decodeSession(cm *corev1.ConfigMap) (*domain.Session, error) {
	raw, ok := cm.Data[SessionDataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, domain.NewError(domain.ErrorValidation, "decode session", fmt.Sprintf("ConfigMap %s/%s has no %s", cm.Namespace, cm.Name, SessionDataKey))
	}
	var session domain.Session
	if err := json.Unmarshal([]byte(raw), &session); err != nil {
		return nil, domain.WrapError(domain.ErrorValidation, "decode session", fmt.Sprintf("ConfigMap %s/%s contains invalid session JSON", cm.Namespace, cm.Name), err)
	}
	session.ResourceVersion = cm.ResourceVersion
	if err := session.Validate(); err != nil {
		return nil, err
	}
	if cm.Name != SessionConfigMapName(session.ID) || cm.Namespace != session.Spec.SessionNamespace || cm.Labels[ManagedByLabel] != ManagedByValue || cm.Labels[SessionKey] != session.ID {
		return nil, domain.NewError(domain.ErrorConflict, "decode session", fmt.Sprintf("ConfigMap %s/%s ownership metadata does not match session %q", cm.Namespace, cm.Name, session.ID))
	}
	return &session, nil
}

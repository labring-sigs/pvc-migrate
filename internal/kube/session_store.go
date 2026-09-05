package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

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
	StorageBackend() string
	Create(ctx context.Context, session *domain.Session) error
	Get(ctx context.Context, namespace, id string) (*domain.Session, error)
	GetByType(
		ctx context.Context,
		namespace string,
		id string,
		sessionType domain.SessionType,
	) (*domain.Session, error)
	Update(ctx context.Context, session *domain.Session) error
	List(ctx context.Context, namespace string) ([]*domain.Session, error)
	Delete(ctx context.Context, session *domain.Session) error
}

// SessionCreateValidator checks submission against API-server admission without
// persisting the workflow or acquiring execution permissions.
type SessionCreateValidator interface {
	ValidateCreate(ctx context.Context, session *domain.Session) error
}

// LockingSessionStore is the persistence contract for mutable workflows.
// Session writes and their cleanup must never silently run without fencing.
type LockingSessionStore interface {
	SessionStore
	SessionLocker
	SessionLeaseCleaner
}

// ControllerSessionStore exposes the CRD-specific operations required by the
// controller. Keeping them in one contract prevents controller behavior from
// changing with the concrete store supplied at runtime.
type ControllerSessionStore interface {
	LockingSessionStore
	GetByKind(
		ctx context.Context,
		namespace string,
		id string,
		kind domain.ControllerKind,
	) (*domain.Session, error)
	CheckWorkflowNameCollision(ctx context.Context, session *domain.Session) error
	EnsureSessionProtection(ctx context.Context, session *domain.Session) error
}

// SessionLocker serializes mutating operations for one persisted session.
// Implementations must fence the lease holder when ownership is lost so that
// callers stop issuing Kubernetes mutations through the returned context.
type SessionLocker interface {
	AcquireSessionLock(ctx context.Context, namespace, sessionID string) (SessionLock, error)
}

// ErrSessionLockContention marks a transient Lease race. Controllers should
// requeue these errors because another worker may be actively progressing the
// same session; they are not workflow failures.
var ErrSessionLockContention = errors.New("session lock contention")

// IsSessionLockContention reports whether an error means the session Lease is
// temporarily held or was concurrently claimed. The sentinel is attached by
// Lease implementations so callers never need to parse human-readable text.
func IsSessionLockContention(err error) bool {
	return errors.Is(err, ErrSessionLockContention)
}

// AcquireRequiredSessionLock enforces the SessionLocker contract at the
// boundary so an invalid implementation cannot turn a fencing operation into
// a nil-pointer panic.
func AcquireRequiredSessionLock(
	ctx context.Context,
	locker SessionLocker,
	namespace, sessionID string,
) (SessionLock, error) {
	if locker == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"session lock",
			"session locker is required",
		)
	}

	lock, err := locker.AcquireSessionLock(ctx, namespace, sessionID)
	if err != nil {
		return nil, err
	}

	if lock == nil {
		return nil, domain.NewError(
			domain.ErrorInternal,
			"session lock",
			"session locker returned a nil lock",
		)
	}

	return lock, nil
}

// SessionLeaseCleaner removes the persisted lock after a session is deleted.
// Implementations may keep leases for lock reuse when a session remains.
type SessionLeaseCleaner interface {
	DeleteSessionLease(ctx context.Context, namespace, sessionID string) error
}

func GetSessionByType(
	ctx context.Context,
	store SessionStore,
	namespace, id string,
	sessionType domain.SessionType,
) (*domain.Session, error) {
	return store.GetByType(ctx, namespace, id, sessionType)
}

func SessionLockID(session *domain.Session) string {
	if session == nil || session.ID == "" {
		return ""
	}

	return session.ID
}

// SessionLock is a renewable, process-owned lock for one session.
type SessionLock interface {
	Bind(ctx context.Context) (context.Context, context.CancelFunc)
	Err() error
	Release(ctx context.Context) error
	Delete(ctx context.Context) error
}

type ConfigMapSessionStore struct {
	client          kubernetes.Interface
	leaseDuration   time.Duration
	leaseRenewEvery time.Duration
}

func NewConfigMapSessionStore(client kubernetes.Interface) *ConfigMapSessionStore {
	return &ConfigMapSessionStore{
		client:          client,
		leaseDuration:   defaultSessionLeaseDuration,
		leaseRenewEvery: defaultSessionLeaseRenewEvery,
	}
}

var _ LockingSessionStore = (*ConfigMapSessionStore)(nil)

func (*ConfigMapSessionStore) StorageBackend() string { return SessionBackendConfigMap }

// WithLeaseTiming configures the per-store Lease timing. Keeping timing on
// the store isolates independent clients and prevents one workflow or test
// from changing the renewal behavior of locks already held by another.
func (s *ConfigMapSessionStore) WithLeaseTiming(
	duration, renewEvery time.Duration,
) *ConfigMapSessionStore {
	if s == nil {
		return s
	}

	if duration > 0 {
		s.leaseDuration = duration
	}

	if renewEvery > 0 {
		s.leaseRenewEvery = renewEvery
	}

	s.leaseDuration, s.leaseRenewEvery = normalizeLeaseTiming(
		s.leaseDuration,
		s.leaseRenewEvery,
	)

	return s
}

func (s *ConfigMapSessionStore) leaseTiming() (time.Duration, time.Duration) {
	duration, renewEvery := s.leaseDuration, s.leaseRenewEvery
	return normalizeLeaseTiming(duration, renewEvery)
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
		},
		Data: map[string]string{SessionDataKey: string(data)},
	}

	created, err := s.client.CoreV1().
		ConfigMaps(cm.Namespace).
		Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"create session",
			fmt.Sprintf("session %s already exists", session.ID),
			err,
		)
	}

	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "create session", "create ConfigMap", err)
	}

	session.ResourceVersion = created.ResourceVersion
	session.Backend = SessionBackendConfigMap

	return nil
}

func (s *ConfigMapSessionStore) Get(
	ctx context.Context,
	namespace, id string,
) (*domain.Session, error) {
	cm, err := s.client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, SessionConfigMapName(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, domain.WrapError(
			domain.ErrorValidation,
			"get session",
			fmt.Sprintf("session %s/%s does not exist", namespace, id),
			err,
		)
	}

	if err != nil {
		return nil, domain.WrapError(domain.ErrorKubernetes, "get session", "read ConfigMap", err)
	}

	session, decodeErr := decodeSession(cm)
	if decodeErr == nil {
		session.Backend = SessionBackendConfigMap
	}

	return session, decodeErr
}

// ConfigMaps have one storage object per session ID, so the caller's expected
// type does not affect lookup. Command-level validation still reports a
// mismatched workflow type with the loaded session context.
func (s *ConfigMapSessionStore) GetByType(
	ctx context.Context,
	namespace, id string,
	_ domain.SessionType,
) (*domain.Session, error) {
	return s.Get(ctx, namespace, id)
}

func (s *ConfigMapSessionStore) Update(ctx context.Context, session *domain.Session) error {
	if session.ResourceVersion == "" {
		return domain.NewError(
			domain.ErrorConflict,
			"update session",
			"session resourceVersion is empty",
		)
	}

	if err := session.Validate(); err != nil {
		return err
	}

	data, err := json.Marshal(session)
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "update session", "encode session", err)
	}

	existing, err := s.client.CoreV1().
		ConfigMaps(session.Spec.SessionNamespace).
		Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"update session",
			"session ConfigMap disappeared",
			err,
		)
	}

	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"update session",
			"read ConfigMap metadata",
			err,
		)
	}

	if _, err := decodeSession(existing); err != nil {
		return domain.WrapError(
			domain.ErrorConflict,
			"update session",
			"ConfigMap ownership does not match the session",
			err,
		)
	}

	if existing.DeletionTimestamp != nil {
		return domain.NewError(
			domain.ErrorConflict,
			"update session",
			"session ConfigMap is pending deletion",
		)
	}

	cm := existing.DeepCopy()

	cm.ResourceVersion = session.ResourceVersion
	if cm.Labels == nil {
		cm.Labels = map[string]string{}
	}

	cm.Labels[ManagedByLabel] = ManagedByValue
	cm.Labels[SessionKey] = session.ID
	cm.Data = map[string]string{SessionDataKey: string(data)}

	updated, err := s.client.CoreV1().
		ConfigMaps(cm.Namespace).
		Update(ctx, cm, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"update session",
			"session changed by another process",
			err,
		)
	}

	if apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorConflict,
			"update session",
			"session ConfigMap disappeared",
			err,
		)
	}

	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "update session", "update ConfigMap", err)
	}

	session.ResourceVersion = updated.ResourceVersion
	session.Backend = SessionBackendConfigMap

	return nil
}

func (s *ConfigMapSessionStore) List(
	ctx context.Context,
	namespace string,
) ([]*domain.Session, error) {
	selector := labels.Set{ManagedByLabel: ManagedByValue}.String()

	items, err := s.client.CoreV1().
		ConfigMaps(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, domain.WrapError(
			domain.ErrorKubernetes,
			"list sessions",
			"list ConfigMaps",
			err,
		)
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
	cm, err := s.client.CoreV1().
		ConfigMaps(session.Spec.SessionNamespace).
		Get(ctx, SessionConfigMapName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "delete session", "read ConfigMap", err)
	}

	if _, err := decodeSession(cm); err != nil {
		return domain.WrapError(
			domain.ErrorConflict,
			"delete session",
			"ConfigMap ownership does not match the session",
			err,
		)
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return domain.NewError(
			domain.ErrorConflict,
			"delete session",
			"session ConfigMap changed after it was loaded",
		)
	}

	uid, resourceVersion := cm.UID, cm.ResourceVersion

	options := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &resourceVersion},
	}
	if err := s.client.CoreV1().
		ConfigMaps(cm.Namespace).
		Delete(ctx, cm.Name, options); err != nil &&
		!apierrors.IsNotFound(err) {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"delete session",
			fmt.Sprintf("delete ConfigMap UID %s", cm.UID),
			err,
		)
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
	return slices.Contains(values, value)
}

func removeSessionFinalizer(values []string) []string {
	result := values[:0]
	for _, item := range values {
		if item != SessionFinalizer {
			result = append(result, item)
		}
	}

	return result
}

func decodeSession(cm *corev1.ConfigMap) (*domain.Session, error) {
	raw, ok := cm.Data[SessionDataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, domain.NewError(
			domain.ErrorValidation,
			"decode session",
			fmt.Sprintf("ConfigMap %s/%s has no %s", cm.Namespace, cm.Name, SessionDataKey),
		)
	}

	var session domain.Session
	if err := json.Unmarshal([]byte(raw), &session); err != nil {
		return nil, domain.WrapError(
			domain.ErrorValidation,
			"decode session",
			fmt.Sprintf("ConfigMap %s/%s contains invalid session JSON", cm.Namespace, cm.Name),
			err,
		)
	}

	session.ResourceVersion = cm.ResourceVersion

	session.Backend = SessionBackendConfigMap
	if err := session.Validate(); err != nil {
		return nil, err
	}

	if cm.Name != SessionConfigMapName(session.ID) ||
		cm.Namespace != session.Spec.SessionNamespace ||
		cm.Labels[ManagedByLabel] != ManagedByValue ||
		cm.Labels[SessionKey] != session.ID {
		return nil, domain.NewError(
			domain.ErrorConflict,
			"decode session",
			fmt.Sprintf(
				"ConfigMap %s/%s ownership metadata does not match session %q",
				cm.Namespace,
				cm.Name,
				session.ID,
			),
		)
	}

	return &session, nil
}

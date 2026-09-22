package crosscluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ErrReservationSession identifies the other operation stored under the same
// ConfigMap name. Callers that support the reservation-to-copy handoff can use
// it to select the typed reservation loader without masking malformed records.
var ErrReservationSession = errors.New("cross-cluster ConfigMap contains a reservation session")

func (s *Service) touch(
	session *CopySession,
) {
	session.Status.UpdatedAt = metav1.NewTime(s.now().UTC())
}

func (s *Service) touchReservation(session *ReservationSession) {
	session.Status.UpdatedAt = metav1.NewTime(s.now().UTC())
}

const sessionPrefix = "pvc-migrate-cross-cluster-"

type sessionLockContextKey struct{}

func sessionLockFromContext(ctx context.Context) (kube.SessionLock, bool) {
	lock, ok := ctx.Value(sessionLockContextKey{}).(kube.SessionLock)
	return lock, ok && lock != nil
}

func sessionName(id string) string { return sessionPrefix + id }

// requireSessionLease is the common write fence for cross-cluster execution.
// The session store is also used without a lock by planning, so it must check
// both cancellation and the optional enclosing lease at every mutation edge.
func requireSessionLease(ctx context.Context) error {
	return errors.Join(ctx.Err(), kube.LeaseFenceError(ctx))
}

func (s *Service) save(ctx context.Context, session *CopySession, create bool) error {
	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	if err := session.Validate(); err != nil {
		return err
	}

	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}

	cmClient := s.source.Kubernetes.CoreV1().ConfigMaps(session.Spec.SessionNamespace)
	if create {
		created, createErr := cmClient.Create(
			ctx,
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sessionName(session.ID),
					Namespace: session.Spec.SessionNamespace,
					Labels:    map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
				},
				Data: map[string]string{"session.json": string(raw)},
			},
			metav1.CreateOptions{},
		)
		if createErr == nil {
			session.ResourceVersion = created.ResourceVersion
		}

		err = errors.Join(createErr, ctx.Err(), kube.LeaseFenceError(ctx))

		return err
	}

	cm, err := cmClient.Get(ctx, sessionName(session.ID), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return errors.New("cross-cluster session changed while operation was running")
	}

	if cm.Labels[ManagedByLabel] != ManagedBy || cm.Labels[SessionKey] != session.ID {
		return errors.New("cross-cluster session ConfigMap ownership changed")
	}

	if err := errors.Join(ctx.Err(), kube.LeaseFenceError(ctx)); err != nil {
		return err
	}

	cm.Data = map[string]string{"session.json": string(raw)}

	updated, err := cmClient.Update(ctx, cm, metav1.UpdateOptions{})
	if err == nil {
		session.ResourceVersion = updated.ResourceVersion
	}

	return errors.Join(err, ctx.Err(), kube.LeaseFenceError(ctx))
}

func (s *Service) Get(ctx context.Context, namespace, id string) (*CopySession, error) {
	if s == nil || s.source == nil {
		return nil, errors.New("source client is required")
	}

	if err := ValidateSessionID(id); err != nil {
		return nil, err
	}

	cm, err := s.source.Kubernetes.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, sessionName(id), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var session CopySession
	if err := json.Unmarshal([]byte(cm.Data["session.json"]), &session); err != nil {
		return nil, err
	}

	if cm.Name != sessionName(id) || cm.Namespace != namespace ||
		cm.Labels[ManagedByLabel] != ManagedBy ||
		cm.Labels[SessionKey] != id ||
		session.ID != id ||
		session.Spec.SessionNamespace != namespace {
		return nil, fmt.Errorf(
			"cross-cluster session ConfigMap ownership does not match session %q",
			id,
		)
	}

	if session.Kind == ReservationKind {
		return nil, ErrReservationSession
	}

	session.ResourceVersion = cm.ResourceVersion

	return &session, session.Validate()
}

func (s *Service) GetReservation(
	ctx context.Context,
	namespace, id string,
) (*ReservationSession, error) {
	if s == nil || s.source == nil {
		return nil, errors.New("source client is required")
	}

	if err := ValidateSessionID(id); err != nil {
		return nil, err
	}

	cm, err := s.source.Kubernetes.CoreV1().ConfigMaps(namespace).
		Get(ctx, sessionName(id), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	var session ReservationSession
	if err := json.Unmarshal([]byte(cm.Data["session.json"]), &session); err != nil {
		return nil, err
	}

	if cm.Name != sessionName(id) || cm.Namespace != namespace ||
		cm.Labels[ManagedByLabel] != ManagedBy || cm.Labels[SessionKey] != id ||
		session.ID != id || session.Kind != ReservationKind ||
		session.Spec.SessionNamespace != namespace {
		return nil, fmt.Errorf(
			"cross-cluster reservation session ConfigMap ownership does not match session %q",
			id,
		)
	}

	session.ResourceVersion = cm.ResourceVersion

	return &session, session.Validate()
}

func (s *Service) delete(ctx context.Context, session *CopySession) error {
	cm, err := s.source.Kubernetes.CoreV1().
		ConfigMaps(session.Spec.SessionNamespace).
		Get(ctx, sessionName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if cm.UID == "" || cm.Name != sessionName(session.ID) ||
		cm.Namespace != session.Spec.SessionNamespace ||
		cm.Labels[ManagedByLabel] != ManagedBy ||
		cm.Labels[SessionKey] != session.ID {
		return errors.New(
			"cross-cluster session ConfigMap ownership changed; refusing to delete it",
		)
	}

	var persisted CopySession
	if err := json.Unmarshal(
		[]byte(cm.Data["session.json"]),
		&persisted,
	); err != nil || persisted.ID != session.ID || persisted.Kind != Kind ||
		persisted.APIVersion != APIVersion {
		return errors.New(
			"cross-cluster session ConfigMap contents do not match session; refusing to delete it",
		)
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return errors.New("cross-cluster session changed while deleting")
	}

	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	err = s.source.Kubernetes.CoreV1().
		ConfigMaps(cm.Namespace).
		Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID}})
	if err != nil {
		return err
	}

	return requireSessionLease(ctx)
}

func (s *Service) saveReservation(
	ctx context.Context,
	session *ReservationSession,
	create bool,
) error {
	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	if err := session.Validate(); err != nil {
		return err
	}

	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}

	cmClient := s.source.Kubernetes.CoreV1().ConfigMaps(session.Spec.SessionNamespace)
	if create {
		created, createErr := cmClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: sessionName(session.ID), Namespace: session.Spec.SessionNamespace,
				Labels: map[string]string{ManagedByLabel: ManagedBy, SessionKey: session.ID},
			},
			Data: map[string]string{"session.json": string(raw)},
		}, metav1.CreateOptions{})
		if createErr == nil {
			session.ResourceVersion = created.ResourceVersion
		}

		return errors.Join(createErr, ctx.Err(), kube.LeaseFenceError(ctx))
	}

	cm, err := cmClient.Get(ctx, sessionName(session.ID), metav1.GetOptions{})
	if err != nil {
		return err
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return errors.New("cross-cluster reservation session changed while operation was running")
	}

	if cm.Labels[ManagedByLabel] != ManagedBy || cm.Labels[SessionKey] != session.ID {
		return errors.New("cross-cluster reservation session ConfigMap ownership changed")
	}

	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	cm.Data = map[string]string{"session.json": string(raw)}

	updated, err := cmClient.Update(ctx, cm, metav1.UpdateOptions{})
	if err == nil {
		session.ResourceVersion = updated.ResourceVersion
	}

	return errors.Join(err, ctx.Err(), kube.LeaseFenceError(ctx))
}

func (s *Service) deleteReservation(ctx context.Context, session *ReservationSession) error {
	cm, err := s.source.Kubernetes.CoreV1().ConfigMaps(session.Spec.SessionNamespace).
		Get(ctx, sessionName(session.ID), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return err
	}

	if cm.UID == "" || cm.Name != sessionName(session.ID) ||
		cm.Namespace != session.Spec.SessionNamespace ||
		cm.Labels[ManagedByLabel] != ManagedBy || cm.Labels[SessionKey] != session.ID {
		return errors.New(
			"cross-cluster reservation session ConfigMap ownership changed; refusing to delete it",
		)
	}

	var persisted ReservationSession
	if err := json.Unmarshal([]byte(cm.Data["session.json"]), &persisted); err != nil ||
		persisted.ID != session.ID || persisted.Kind != ReservationKind ||
		persisted.APIVersion != APIVersion {
		return errors.New(
			"cross-cluster reservation session ConfigMap contents do not match session; refusing to delete it",
		)
	}

	if session.ResourceVersion != "" && cm.ResourceVersion != session.ResourceVersion {
		return errors.New("cross-cluster reservation session changed while deleting")
	}

	if err := requireSessionLease(ctx); err != nil {
		return err
	}

	err = s.source.Kubernetes.CoreV1().ConfigMaps(cm.Namespace).Delete(
		ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID}},
	)

	return errors.Join(err, requireSessionLease(ctx))
}

func (s *Service) withLock(
	ctx context.Context,
	session *CopySession,
	fn func(context.Context) error,
) error {
	if session == nil {
		return errors.New("cross-cluster session is required")
	}

	if s == nil || s.locker == nil {
		return errors.New("cross-cluster session store is required")
	}

	lock, err := kube.AcquireRequiredSessionLock(
		ctx,
		s.locker,
		session.Spec.SessionNamespace,
		session.ID,
	)
	if err != nil {
		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	operationCtx = context.WithValue(
		kube.WithLeaseFence(operationCtx, lock),
		sessionLockContextKey{},
		lock,
	)
	operationErr := fn(operationCtx)

	cancelOperation()

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()

	releaseErr := lock.Release(releaseCtx)

	return errors.Join(operationErr, lock.Err(), releaseErr)
}

func (s *Service) withReservationLock(
	ctx context.Context,
	session *ReservationSession,
	fn func(context.Context) error,
) error {
	if session == nil {
		return errors.New("cross-cluster reservation session is required")
	}

	if s == nil || s.locker == nil {
		return errors.New("cross-cluster session store is required")
	}

	lock, err := kube.AcquireRequiredSessionLock(
		ctx, s.locker, session.Spec.SessionNamespace, session.ID,
	)
	if err != nil {
		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	operationCtx = context.WithValue(
		kube.WithLeaseFence(operationCtx, lock),
		sessionLockContextKey{},
		lock,
	)
	operationErr := fn(operationCtx)

	cancelOperation()

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()

	return errors.Join(operationErr, lock.Err(), lock.Release(releaseCtx))
}

package crosscluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Service) touch(
	session *Session,
) {
	session.Status.UpdatedAt = metav1.NewTime(s.now().UTC())
}

const sessionPrefix = "pvc-migrate-cross-cluster-"

func sessionName(id string) string { return sessionPrefix + id }
func (s *Service) save(ctx context.Context, session *Session, create bool) error {
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

		err = createErr

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

	cm.Data = map[string]string{"session.json": string(raw)}

	updated, err := cmClient.Update(ctx, cm, metav1.UpdateOptions{})
	if err == nil {
		session.ResourceVersion = updated.ResourceVersion
	}

	return err
}

func (s *Service) Get(ctx context.Context, namespace, id string) (*Session, error) {
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

	var session Session
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

	session.ResourceVersion = cm.ResourceVersion

	return &session, session.Validate()
}

func (s *Service) delete(ctx context.Context, session *Session) error {
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

	var persisted Session
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

	return s.source.Kubernetes.CoreV1().
		ConfigMaps(cm.Namespace).
		Delete(ctx, cm.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &cm.UID}})
}

func (s *Service) withLock(
	ctx context.Context,
	session *Session,
	fn func(context.Context) error,
) error {
	if session == nil {
		return errors.New("cross-cluster session is required")
	}

	lock, err := s.store.AcquireSessionLock(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		return err
	}

	operationCtx, cancelOperation := lock.Bind(ctx)
	operationErr := fn(operationCtx)

	cancelOperation()

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRelease()

	releaseErr := lock.Release(releaseCtx)

	return errors.Join(operationErr, lock.Err(), releaseErr)
}

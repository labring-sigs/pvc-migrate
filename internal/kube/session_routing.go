package kube

import (
	"context"
	"errors"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	SessionBackendConfigMap = "configmap"
	SessionBackendCRD       = "crd"
)

// ControllerSessionSupported describes the single-cluster controller contract.
// Workflow resources live in the control/session namespace and carry explicit
// source and destination references. The controller ClusterRole is therefore
// able to operate on more than one namespace in the same cluster without
// making a namespace implicit in the resource identity. Cross-cluster
// workflows remain on their independent ConfigMap/session backend because
// they require a second API-server connection and credentials.
func ControllerSessionSupported(session *domain.Session) bool {
	if session == nil || session.Spec.SourceNamespace == "" || session.Spec.SessionNamespace == "" {
		return false
	}

	switch session.Spec.Type {
	case domain.SessionTypeBackup:
		return session.Spec.Backup != nil
	case domain.SessionTypeRestore:
		return session.Spec.Restore != nil && session.Spec.DestinationNamespace != ""
	case domain.SessionTypeMove:
		return session.Spec.Move != nil && session.Spec.DestinationNamespace != "" &&
			session.Spec.SourceNamespace != session.Spec.DestinationNamespace
	case domain.SessionTypeReserve, domain.SessionTypeMigrate,
		domain.SessionTypeMigratePod, domain.SessionTypeCopy,
		domain.SessionTypeRename:
		return session.Spec.DestinationNamespace != ""
	default:
		return false
	}
}

// RoutingSessionStore provides auto mode. New eligible sessions go to their
// operation-specific workflow CRD, while unsupported workflows continue using
// ConfigMaps.
type RoutingSessionStore struct {
	crd             *CRDSessionStore
	configMap       *ConfigMapSessionStore
	controllerKinds map[domain.ControllerKind]struct{}
}

var (
	_ SessionLocker       = (*RoutingSessionStore)(nil)
	_ SessionLeaseCleaner = (*RoutingSessionStore)(nil)
)

func NewSessionStoreRouter(
	configMap *ConfigMapSessionStore,
	crd *CRDSessionStore,
) *RoutingSessionStore {
	return &RoutingSessionStore{configMap: configMap, crd: crd}
}

// WithControllerKinds configures the set of operation CRDs discovered on the
// cluster. Auto mode can then use a newly installed kind immediately while
// preserving ConfigMap persistence for operations whose CRD is absent.
func (s *RoutingSessionStore) WithControllerKinds(
	kinds []domain.ControllerKind,
) *RoutingSessionStore {
	if s == nil || len(kinds) == 0 {
		return s
	}

	s.controllerKinds = make(map[domain.ControllerKind]struct{}, len(kinds))
	for _, kind := range kinds {
		s.controllerKinds[kind] = struct{}{}
	}

	return s
}

func (s *RoutingSessionStore) controllerSupports(session *domain.Session) bool {
	if s == nil || s.crd == nil || !ControllerSessionSupported(session) {
		return false
	}

	if len(s.controllerKinds) == 0 {
		return true
	}

	workflow, ok := domain.ControllerWorkflowForType(session.Spec.Type)
	if !ok {
		return false
	}

	_, ok = s.controllerKinds[workflow.Kind]

	return ok
}

func (s *RoutingSessionStore) Create(ctx context.Context, session *domain.Session) error {
	if s.controllerSupports(session) {
		return s.crd.Create(ctx, session)
	}
	return s.configMap.Create(ctx, session)
}

func (s *RoutingSessionStore) Get(
	ctx context.Context,
	namespace, id string,
) (*domain.Session, error) {
	if s == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"get session",
			"session store router is not configured",
		)
	}

	if s.crd == nil {
		if s.configMap == nil {
			return nil, domain.NewError(
				domain.ErrorKubernetes,
				"get session",
				"no session backend is configured",
			)
		}

		return s.configMap.Get(ctx, namespace, id)
	}

	if s.configMap == nil {
		return s.crd.Get(ctx, namespace, id)
	}

	// CRD and ConfigMap reads are independent. Keep the CRD result first when
	// both backends contain a record, matching the routing and List semantics.
	type getResult struct {
		session *domain.Session
		err     error
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	crdResult := make(chan getResult, 1)
	configMapResult := make(chan getResult, 1)

	go func() {
		session, err := s.crd.Get(readCtx, namespace, id)
		crdResult <- getResult{session: session, err: err}
	}()
	go func() {
		session, err := s.configMap.Get(readCtx, namespace, id)
		configMapResult <- getResult{session: session, err: err}
	}()

	var (
		crd       getResult
		configMap getResult
		gotCRD    bool
		gotConfig bool
	)
	for !gotCRD || !gotConfig {
		select {
		case crd = <-crdResult:
			gotCRD = true

			if crd.err == nil {
				return crd.session, nil
			}

			if !IsSessionNotFound(crd.err) {
				return nil, crd.err
			}
		case configMap = <-configMapResult:
			gotConfig = true
		}
	}

	return configMap.session, configMap.err
}

func (s *RoutingSessionStore) Update(ctx context.Context, session *domain.Session) error {
	if session != nil && session.Backend == SessionBackendCRD && s.crd != nil {
		return s.crd.Update(ctx, session)
	}
	return s.configMap.Update(ctx, session)
}

func (s *RoutingSessionStore) List(
	ctx context.Context,
	namespace string,
) ([]*domain.Session, error) {
	if s.crd == nil {
		return s.configMap.List(ctx, namespace)
	}

	var results [2][]*domain.Session

	var errors [2]error
	parallel.For(2, func(index int) {
		if index == 0 {
			results[index], errors[index] = s.configMap.List(ctx, namespace)
			return
		}

		results[index], errors[index] = s.crd.List(ctx, namespace)
	})

	// Keep the historical error precedence: ConfigMap failures are reported
	// before CRD failures even though both reads run concurrently.
	if errors[0] != nil {
		return nil, errors[0]
	}

	if errors[1] != nil {
		return nil, errors[1]
	}

	configSessions := results[0]
	crdSessions := results[1]

	// Get checks the CRD backend first. Preserve that same precedence when
	// listing so a session that was explicitly moved to controller storage is
	// represented once instead of appearing as two independent records.
	result := make([]*domain.Session, 0, len(configSessions)+len(crdSessions))

	byID := make(map[string]int, len(configSessions)+len(crdSessions))
	for _, session := range configSessions {
		if session == nil {
			continue
		}

		byID[session.ID] = len(result)
		result = append(result, session)
	}

	for _, session := range crdSessions {
		if session == nil {
			continue
		}

		if index, exists := byID[session.ID]; exists {
			result[index] = session
			continue
		}

		byID[session.ID] = len(result)
		result = append(result, session)
	}

	return result, nil
}

func (s *RoutingSessionStore) Delete(ctx context.Context, session *domain.Session) error {
	if session != nil && session.Backend == SessionBackendCRD && s.crd != nil {
		return s.crd.Delete(ctx, session)
	}
	return s.configMap.Delete(ctx, session)
}

// AcquireSessionLock keeps auto mode's storage routing transparent to the
// service-level fencing protocol. Both backends use the same Lease name, so
// selecting the configured Kubernetes client is sufficient even before a
// session has been persisted and acquired a backend marker.
func (s *RoutingSessionStore) AcquireSessionLock(
	ctx context.Context,
	namespace, id string,
) (SessionLock, error) {
	if s == nil {
		return nil, domain.NewError(
			domain.ErrorKubernetes,
			"acquire session lock",
			"session store router is not configured",
		)
	}

	if s.crd != nil && s.crd.leaseClient != nil {
		return s.crd.AcquireSessionLock(ctx, namespace, id)
	}

	if s.configMap != nil {
		return s.configMap.AcquireSessionLock(ctx, namespace, id)
	}

	if s.crd != nil {
		return s.crd.AcquireSessionLock(ctx, namespace, id)
	}

	return nil, domain.NewError(
		domain.ErrorKubernetes,
		"acquire session lock",
		"no session backend is configured",
	)
}

func (s *RoutingSessionStore) DeleteSessionLease(
	ctx context.Context,
	namespace, id string,
) error {
	if s == nil {
		return domain.NewError(
			domain.ErrorKubernetes,
			"delete session lock",
			"session store router is not configured",
		)
	}

	if s.crd != nil && s.crd.leaseClient != nil {
		return s.crd.DeleteSessionLease(ctx, namespace, id)
	}

	if s.configMap != nil {
		return s.configMap.DeleteSessionLease(ctx, namespace, id)
	}

	if s.crd != nil {
		return s.crd.DeleteSessionLease(ctx, namespace, id)
	}

	return domain.NewError(
		domain.ErrorKubernetes,
		"delete session lock",
		"no session backend is configured",
	)
}

func IsSessionNotFound(err error) bool {
	if apierrors.IsNotFound(err) {
		return true
	}

	var typed *domain.Error
	if !errors.As(err, &typed) {
		return false
	}

	return typed.Category == domain.ErrorValidation &&
		strings.Contains(strings.ToLower(typed.Message), "does not exist")
}

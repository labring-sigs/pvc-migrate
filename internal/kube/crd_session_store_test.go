package kube

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type createErrorClient struct {
	crclient.Client
	err error
}

type updateCountingClient struct {
	crclient.Client
	updates int
}

type concurrentListClient struct {
	crclient.Client
	active atomic.Int32
	max    atomic.Int32
	delay  time.Duration
}

func (c *concurrentListClient) List(
	ctx context.Context,
	list crclient.ObjectList,
	options ...crclient.ListOption,
) error {
	active := c.active.Add(1)
	for {
		currentMax := c.max.Load()
		if active <= currentMax || c.max.CompareAndSwap(currentMax, active) {
			break
		}
	}

	defer c.active.Add(-1)

	time.Sleep(c.delay)

	return c.Client.List(ctx, list, options...)
}

func (c *updateCountingClient) Update(
	ctx context.Context,
	object crclient.Object,
	options ...crclient.UpdateOption,
) error {
	c.updates++
	return c.Client.Update(ctx, object, options...)
}

func (c createErrorClient) Create(
	ctx context.Context,
	object crclient.Object,
	options ...crclient.CreateOption,
) error {
	return c.err
}

var (
	_ SessionLocker       = (*CRDSessionStore)(nil)
	_ SessionLeaseCleaner = (*CRDSessionStore)(nil)
)

func TestCRDSessionStoreResourceRegistryIsolatedPerCall(t *testing.T) {
	store := NewCRDSessionStore(newCRDTestClient())

	resources := store.resources()
	if len(resources) == 0 {
		t.Fatal("CRD resource registry is empty")
	}

	original := resources[0].kind
	resources[0].kind = domain.ControllerKind("mutated")

	fresh := store.resources()
	if fresh[0].kind != original {
		t.Fatalf(
			"resource registry was mutated through returned slice: got %q want %q",
			fresh[0].kind,
			original,
		)
	}
}

func TestCRDSessionStoreRoundTripAndStatusUpdate(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(newCRDTestClient())

	session := storeTestSession()

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if session.Backend != SessionBackendCRD {
		t.Fatalf("create metadata backend=%q", session.Backend)
	}

	loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Backend != SessionBackendCRD || loaded.Status.Phase != domain.PhasePlanned {
		t.Fatalf("loaded session backend=%q phase=%q", loaded.Backend, loaded.Status.Phase)
	}

	if err := loaded.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, loaded); err != nil {
		t.Fatal(err)
	}

	listed, err := store.List(ctx, session.Spec.SessionNamespace)
	if err != nil {
		t.Fatal(err)
	}

	if len(listed) != 1 || listed[0].Status.Phase != domain.PhaseReserving {
		t.Fatalf("listed sessions=%#v", listed)
	}
}

func TestCRDSessionStoreListReadsWorkflowKindsConcurrently(t *testing.T) {
	client := &concurrentListClient{
		Client: newCRDTestClient(),
		delay:  10 * time.Millisecond,
	}

	if _, err := NewCRDSessionStore(client).List(context.Background(), "system"); err != nil {
		t.Fatal(err)
	}

	if client.max.Load() < 2 {
		t.Fatalf("maximum concurrent workflow List calls=%d, want at least 2", client.max.Load())
	}
}

func TestCRDSessionStoreUpdateKeepsVolumeStatusPointersStable(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(newCRDTestClient())
	session := storeTestSession()

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	status := &session.Status.Volumes[0]
	status.Sync.Attempts++

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	completed := metav1.Now()
	status.Sync.FinalCompletedAt = &completed

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.Volumes[0].Sync.Attempts != 1 ||
		loaded.Status.Volumes[0].Sync.FinalCompletedAt == nil {
		t.Fatalf("status pointer update was lost: %#v", loaded.Status.Volumes[0].Sync)
	}
}

func TestCRDSessionStorePersistsDestinationIdentityOnlyInStatus(t *testing.T) {
	ctx := context.Background()
	client := &updateCountingClient{Client: newCRDTestClient()}
	store := NewCRDSessionStore(client)
	session := storeTestSession()
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		session.Spec.SessionCommon,
		true,
		domain.SessionWorkflowOptions{},
	)
	session = domain.NewSession(session.ID, session.Spec, time.Now())

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if client.updates != 0 {
		t.Fatalf("main-resource updates after create=%d, want 0", client.updates)
	}

	volume := &session.Spec.Volumes[0]
	volume.DestinationPVC.UID = "destination-pvc-uid"
	volume.DestinationPVC.ResourceVersion = "17"
	volume.DestinationPV = domain.ObjectReference{
		Name: "destination-pv", UID: "destination-pv-uid", ResourceVersion: "19",
	}
	volume.DestinationPolicy = corev1.PersistentVolumeReclaimRetain
	session.Status.Volumes[0].Reserved = true

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	if client.updates != 0 {
		t.Fatalf("runtime checkpoint caused %d main-resource updates", client.updates)
	}

	var object v1alpha1.Copy
	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: session.Spec.SessionNamespace, Name: session.ID},
		&object,
	); err != nil {
		t.Fatal(err)
	}

	if object.Spec.Volumes[0].DestinationPVC.UID != "" ||
		object.Status.Volumes[0].DestinationPVC.UID != "destination-pvc-uid" ||
		object.Status.Volumes[0].DestinationPV.Name != "destination-pv" {
		t.Fatalf("unexpected Copy spec/status checkpoint: %#v %#v", object.Spec, object.Status)
	}

	loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Spec.Volumes[0].DestinationPVC.UID != "destination-pvc-uid" ||
		loaded.Spec.Volumes[0].DestinationPV.UID != "destination-pv-uid" ||
		loaded.Spec.Volumes[0].DestinationPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("destination checkpoint was not restored: %#v", loaded.Spec.Volumes[0])
	}
}

func TestCRDSessionStorePersistsCurrentPodIdentityOnlyInStatus(t *testing.T) {
	ctx := context.Background()
	client := &updateCountingClient{Client: newCRDTestClient()}
	store := NewCRDSessionStore(client)
	base := storeTestSession()
	spec := domain.NewPodMigrationSessionSpec(
		base.Spec.SessionCommon,
		domain.WorkloadSpec{
			Adapter: domain.WorkloadStandalone,
			Pod: domain.ObjectReference{
				Namespace: "app", Name: "writer", UID: "original-pod-uid",
			},
			AffectedPods: []domain.ObjectReference{{
				Namespace: "app", Name: "writer", UID: "original-pod-uid",
			}},
		},
		domain.SessionWorkflowOptions{},
		1,
		false,
	)
	session := domain.NewSession(base.ID, spec, time.Now())

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	workload := session.Spec.WorkloadPtr()
	workload.Pod.UID = "resumed-pod-uid"
	workload.Pod.ResourceVersion = "23"
	workload.AffectedPods[0] = workload.Pod

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	if client.updates != 0 {
		t.Fatalf("current Pod checkpoint caused %d main-resource updates", client.updates)
	}

	var object v1alpha1.PodMigration
	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: session.Spec.SessionNamespace, Name: session.ID},
		&object,
	); err != nil {
		t.Fatal(err)
	}

	if object.Spec.Workload.Pod.UID != "original-pod-uid" || object.Status.Workload == nil ||
		object.Status.Workload.Pod.UID != "resumed-pod-uid" {
		t.Fatalf(
			"unexpected PodMigration spec/status workload: %#v %#v",
			object.Spec,
			object.Status,
		)
	}

	loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Spec.Workload().Pod.UID != "resumed-pod-uid" ||
		loaded.Spec.Workload().AffectedPods[0].UID != "resumed-pod-uid" {
		t.Fatalf("current Pod checkpoint was not restored: %#v", loaded.Spec.Workload())
	}
}

func TestCRDSessionStoreCreateReportsAPIServerCause(t *testing.T) {
	cause := errors.New("spec.volumes[0] was rejected")
	store := NewCRDSessionStore(createErrorClient{Client: newCRDTestClient(), err: cause})

	err := store.Create(context.Background(), storeTestSession())
	if domain.CategoryOf(err) != domain.ErrorKubernetes ||
		!strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("create error=%v, want Kubernetes error with API server cause", err)
	}
}

func TestDecodeMigrationInitializesDeclarativeResource(t *testing.T) {
	session := storeTestSession()
	object := sessionObject(session)
	object.Labels = nil
	object.Status = v1alpha1.MigrationStatus{}
	object.ResourceVersion = "17"
	object.Generation = 3

	decoded, err := DecodeMigration(object)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Backend != SessionBackendCRD || decoded.Status.Phase != domain.PhasePlanned {
		t.Fatalf("decoded declarative migration=%#v", decoded)
	}

	if decoded.Generation != object.Generation ||
		decoded.ResourceVersion != object.ResourceVersion {
		t.Fatalf(
			"decoded metadata generation=%d resourceVersion=%q",
			decoded.Generation,
			decoded.ResourceVersion,
		)
	}
}

func TestRoutingSessionStoreUsesMoveCRDForCrossNamespaceWorkflow(t *testing.T) {
	ctx := context.Background()
	configMap := NewConfigMapSessionStore(clientfake.NewClientset())
	crd := NewCRDSessionStore(newCRDTestClient())
	router := NewSessionStoreRouter(configMap, crd)
	session := storeTestSession()
	session.Spec = domain.NewSessionSpec(domain.OperationMove, domain.SessionCommon{
		SourceNamespace:      "app",
		TemporaryNamespace:   "system",
		DestinationNamespace: "archive",
		SessionNamespace:     "system",
		Volumes:              session.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	session.Status = domain.NewSession(session.ID, session.Spec, time.Now()).Status

	if err := router.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if session.Backend != SessionBackendCRD || session.BackendResource != "Move" {
		t.Fatalf("move workflow backend=%q resource=%q", session.Backend, session.BackendResource)
	}

	if _, err := crd.Get(ctx, "system", session.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRoutingSessionStoreFallsBackPerOperationWhenCRDIsMissing(t *testing.T) {
	ctx := context.Background()
	configMap := NewConfigMapSessionStore(clientfake.NewClientset())
	crd := NewCRDSessionStore(newCRDTestClient()).
		WithSupportedKinds([]domain.ControllerKind{domain.ControllerKindMigration})
	router := NewSessionStoreRouter(configMap, crd).
		WithControllerKinds([]domain.ControllerKind{domain.ControllerKindMigration})

	migration := storeTestSession()
	if err := router.Create(ctx, migration); err != nil {
		t.Fatal(err)
	}

	if migration.Backend != SessionBackendCRD || migration.BackendResource != "Migration" {
		t.Fatalf("migration backend=%q resource=%q", migration.Backend, migration.BackendResource)
	}

	copySession := storeTestSession()
	copySession.ID = "copy-fallback"
	copySession.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: migration.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})

	copySession.Status = domain.NewSession(copySession.ID, copySession.Spec, time.Now()).Status
	if err := crd.Create(ctx, copySession); domain.CategoryOf(err) != domain.ErrorPrecondition {
		t.Fatalf("missing Copy CRD category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := router.Create(ctx, copySession); err != nil {
		t.Fatal(err)
	}

	if copySession.Backend != SessionBackendConfigMap {
		t.Fatalf("copy fallback backend=%q", copySession.Backend)
	}

	loaded, err := router.Get(ctx, "system", copySession.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Backend != SessionBackendConfigMap || loaded.Spec.Type != domain.SessionTypeCopy {
		t.Fatalf("loaded fallback backend=%q type=%q", loaded.Backend, loaded.Spec.Type)
	}
}

func TestRoutingSessionStoreGetReturnsCRDWithoutWaitingForConfigMap(t *testing.T) {
	configMapClient := clientfake.NewClientset()
	configMapStarted := make(chan struct{})
	configMapRelease := make(chan struct{})
	configMapDone := make(chan struct{})
	configMapClient.PrependReactor(
		"get",
		"configmaps",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			close(configMapStarted)
			<-configMapRelease
			close(configMapDone)

			return true, nil, apierrors.NewNotFound(
				schema.GroupResource{Resource: "configmaps"},
				SessionConfigMapName("alpha"),
			)
		},
	)

	crd := NewCRDSessionStore(newCRDTestClient())

	session := storeTestSession()
	if err := crd.Create(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	router := NewSessionStoreRouter(NewConfigMapSessionStore(configMapClient), crd)

	type getResponse struct {
		session *domain.Session
		err     error
	}

	response := make(chan getResponse, 1)
	go func() {
		loaded, err := router.Get(context.Background(), "system", session.ID)
		response <- getResponse{session: loaded, err: err}
	}()

	select {
	case <-configMapStarted:
	case <-time.After(time.Second):
		t.Fatal("ConfigMap fallback read did not start")
	}

	select {
	case result := <-response:
		if result.err != nil {
			t.Fatal(result.err)
		}

		if result.session == nil || result.session.Backend != SessionBackendCRD {
			t.Fatalf("router returned %#v, want CRD session", result.session)
		}
	case <-time.After(time.Second):
		t.Fatal("CRD hit waited for blocked ConfigMap fallback")
	}

	close(configMapRelease)

	select {
	case <-configMapDone:
	case <-time.After(time.Second):
		t.Fatal("blocked ConfigMap fallback did not finish after release")
	}
}

func TestCRDSessionStoreRoundTripsEveryWorkflowKind(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		kind domain.ControllerKind
		make func() *domain.Session
	}{
		{name: "migration", kind: domain.ControllerKindMigration, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec.DestinationNamespace = "archive"
			session.Spec.Volumes[0].DestinationPVC.Namespace = "archive"
			return session
		}},
		{
			name: "pod migration",
			kind: domain.ControllerKindPodMigration,
			make: func() *domain.Session {
				session := storeTestSession()
				session.Spec = domain.NewPodMigrationSessionSpec(domain.SessionCommon{
					SourceNamespace:      "app",
					TemporaryNamespace:   "system",
					DestinationNamespace: "archive",
					SessionNamespace:     "system",
					Volumes:              session.Spec.Volumes,
				}, domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{
					Namespace: "app", Name: "workload", UID: "pod-uid",
				}}, domain.SessionWorkflowOptions{}, 1, false)
				session.Spec.Volumes[0].DestinationPVC.Namespace = "archive"

				return domain.NewSession(session.ID, session.Spec, time.Now())
			},
		},
		{name: "reservation", kind: domain.ControllerKindReservation, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
				SourceNamespace:      "app",
				TemporaryNamespace:   "system",
				DestinationNamespace: "archive",
				SessionNamespace:     "system",
				Volumes:              session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})

			return domain.NewSession(session.ID, session.Spec, time.Now())
		}},
		{name: "copy", kind: domain.ControllerKindCopy, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
				SourceNamespace:      "app",
				TemporaryNamespace:   "system",
				DestinationNamespace: "archive",
				SessionNamespace:     "system",
				Volumes:              session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})

			return domain.NewSession(session.ID, session.Spec, time.Now())
		}},
		{name: "backup", kind: domain.ControllerKindBackup, make: func() *domain.Session {
			spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
				SourceNamespace: "app", SessionNamespace: "system", DestinationNamespace: "app",
			}, false, domain.SessionWorkflowOptions{})
			spec.Backup.SourcePVC = domain.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       "pvc-uid",
			}
			spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
			spec.Backup.Backend, spec.Backup.Bucket, spec.Backup.Name = "s3", "backups", "daily"

			return domain.NewSession("alpha", spec, time.Now())
		}},
		{name: "restore", kind: domain.ControllerKindRestore, make: func() *domain.Session {
			spec := domain.NewSessionSpec(domain.OperationRestore, domain.SessionCommon{
				SourceNamespace: "app", SessionNamespace: "system", DestinationNamespace: "app",
			}, false, domain.SessionWorkflowOptions{})
			spec.Restore.DestinationPVC = domain.ObjectReference{
				Namespace: "app",
				Name:      "data",
				UID:       "pvc-uid",
			}
			spec.Restore.Backend, spec.Restore.Bucket, spec.Restore.Name = "s3", "backups", "daily"

			return domain.NewSession("alpha", spec, time.Now())
		}},
		{name: "rename", kind: domain.ControllerKindRename, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationRename, domain.SessionCommon{
				SourceNamespace: "app", TemporaryNamespace: "app", DestinationNamespace: "app",
				SessionNamespace: "system", Volumes: session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})

			return domain.NewSession(session.ID, session.Spec, time.Now())
		}},
		{name: "move", kind: domain.ControllerKindMove, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationMove, domain.SessionCommon{
				SourceNamespace:      "app",
				TemporaryNamespace:   "archive",
				DestinationNamespace: "archive",
				SessionNamespace:     "system",
				Volumes:              session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})
			session.Spec.Volumes[0].DestinationPVC.Namespace = "archive"

			return domain.NewSession(session.ID, session.Spec, time.Now())
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewCRDSessionStore(newCRDTestClient())

			session := test.make()
			if got := workflowCRDKind(session.Spec.Type); got != test.kind {
				t.Fatalf("workflow kind=%q, want %q", got, test.kind)
			}

			if err := store.Create(ctx, session); err != nil {
				t.Fatal(err)
			}

			loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
			if err != nil {
				t.Fatal(err)
			}

			if loaded.BackendResource != test.kind || loaded.Spec.Type != session.Spec.Type {
				t.Fatalf("loaded resource=%q type=%q", loaded.BackendResource, loaded.Spec.Type)
			}

			listed, err := store.List(ctx, session.Spec.SessionNamespace)
			if err != nil {
				t.Fatal(err)
			}

			if len(listed) != 1 || listed[0].BackendResource != test.kind {
				t.Fatalf("listed=%#v", listed)
			}
		})
	}
}

func TestDecodeWorkflowDerivesTypeFromKind(t *testing.T) {
	session := storeTestSession()

	object, ok := sessionObjectForKind(session, "Migration")
	if !ok {
		t.Fatal("failed to construct Migration object")
	}

	if _, ok := object.(*v1alpha1.Migration); !ok {
		t.Fatalf("workflow type=%T, want *v1alpha1.Migration", object)
	}

	decoded, err := DecodeWorkflow(object)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Spec.Type != domain.SessionTypeMigrate {
		t.Fatalf("decoded type=%q, want %q", decoded.Spec.Type, domain.SessionTypeMigrate)
	}
}

func TestCRDSessionStoreRejectsDuplicateNameAcrossKinds(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(newCRDTestClient())

	first := storeTestSession()
	if err := store.Create(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := storeTestSession()

	second.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: first.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	if err := store.Create(ctx, second); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("duplicate category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCRDSessionStoreGetRejectsDuplicateNameAcrossKinds(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)

	first := storeTestSession()
	if err := store.Create(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := storeTestSession()
	second.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: first.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})

	object := sessionObjectFor(second)
	if object == nil {
		t.Fatal("failed to construct duplicate workflow")
	}

	if err := client.Create(ctx, object); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Get(
		ctx,
		"system",
		first.ID,
	); domain.CategoryOf(
		err,
	) != domain.ErrorConflict {
		t.Fatalf("duplicate get category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestCRDSessionStoreRebindsReservationToCopy(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)
	session := storeTestSession()

	session.Spec = domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: session.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	session.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: session.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	if err := session.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseReserved, "reserved", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Get(ctx, "system", session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.BackendResource != "Copy" || loaded.Spec.Type != domain.SessionTypeCopy ||
		loaded.Status.Phase != domain.PhaseReserved {
		t.Fatalf(
			"rebound session resource=%q type=%q phase=%q",
			loaded.BackendResource,
			loaded.Spec.Type,
			loaded.Status.Phase,
		)
	}

	copyObject := &v1alpha1.Copy{}
	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		copyObject,
	); err != nil {
		t.Fatal(err)
	}

	if !containsString(copyObject.Finalizers, SessionFinalizer) {
		t.Fatalf("rebound Copy lost session protection: %v", copyObject.Finalizers)
	}

	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		&v1alpha1.Reservation{},
	); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("old Reservation still exists, error=%v", err)
	}
}

func TestCRDSessionStoreRebindRollbackRemovesTargetFinalizer(t *testing.T) {
	ctx := context.Background()
	base := newCRDTestClient()

	baseWithWatch, ok := base.(crclient.WithWatch)
	if !ok {
		t.Fatal("fake CRD client does not support Watch")
	}

	client := interceptor.NewClient(baseWithWatch, interceptor.Funcs{
		SubResourceUpdate: func(
			ctx context.Context,
			underlying crclient.Client,
			subResource string,
			object crclient.Object,
			options ...crclient.SubResourceUpdateOption,
		) error {
			if subResource == "status" {
				if _, isCopy := object.(*v1alpha1.Copy); isCopy {
					return errors.New("injected target status failure")
				}
			}

			return underlying.SubResource(subResource).Update(ctx, object, options...)
		},
	})

	store := NewCRDSessionStore(client)
	session := storeTestSession()

	session.Spec = domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: session.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseReserved, "reserved", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	session.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "app", TemporaryNamespace: "system", DestinationNamespace: "app",
		SessionNamespace: "system", Volumes: session.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})

	err := store.Update(ctx, session)
	if err == nil || !strings.Contains(err.Error(), "initialize Copy status") {
		t.Fatalf("rebind error=%v", err)
	}

	target := &v1alpha1.Copy{}

	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		target,
	); !apierrors.IsNotFound(err) {
		t.Fatalf("rollback target still exists, error=%v finalizers=%v", err, target.Finalizers)
	}

	old := &v1alpha1.Reservation{}

	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		old,
	); err != nil {
		t.Fatal(err)
	}

	if !containsString(old.Finalizers, SessionFinalizer) {
		t.Fatalf("rollback removed source protection: %v", old.Finalizers)
	}
}

func TestRoutingSessionStoreListPrefersCRDForDuplicateID(t *testing.T) {
	ctx := context.Background()
	configMapClient := clientfake.NewClientset()
	configMapStore := NewConfigMapSessionStore(configMapClient)
	crdStore := NewCRDSessionStore(newCRDTestClient())
	router := NewSessionStoreRouter(configMapStore, crdStore)

	configSession := storeTestSession()
	if err := configMapStore.Create(ctx, configSession); err != nil {
		t.Fatal(err)
	}

	crdSession := storeTestSession()
	if err := crdStore.Create(ctx, crdSession); err != nil {
		t.Fatal(err)
	}

	listed, err := router.List(ctx, "system")
	if err != nil {
		t.Fatal(err)
	}

	if len(listed) != 1 || listed[0].Backend != SessionBackendCRD {
		t.Fatalf("router list=%#v", listed)
	}
}

func newCRDTestClient() crclient.Client {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	return crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&v1alpha1.Migration{}, &v1alpha1.PodMigration{}, &v1alpha1.Reservation{},
			&v1alpha1.Copy{}, &v1alpha1.Backup{}, &v1alpha1.Restore{}, &v1alpha1.Rename{}, &v1alpha1.Move{},
		).
		Build()
}

func TestControllerSessionSupportedBoundaries(t *testing.T) {
	session := storeTestSession()
	if !ControllerSessionSupported(session) {
		t.Fatal("same-namespace migrate should be controller compatible")
	}

	session.Spec.DestinationNamespace = "archive"

	session.Spec.Volumes[0].DestinationPVC.Namespace = "archive"
	if !ControllerSessionSupported(session) {
		t.Fatal("cross-namespace migrate should be controller compatible")
	}
}

func TestCRDSessionStoreRequiresLeaseClient(t *testing.T) {
	store := NewCRDSessionStore(newCRDTestClient())
	if _, err := store.AcquireSessionLock(
		context.Background(),
		"system",
		"session",
	); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("acquire without lease client category=%s error=%v", domain.CategoryOf(err), err)
	}

	if err := store.DeleteSessionLease(
		context.Background(),
		"system",
		"session",
	); domain.CategoryOf(err) != domain.ErrorKubernetes {
		t.Fatalf("delete without lease client category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestRoutingSessionStoreForwardsSessionLease(t *testing.T) {
	store := NewSessionStoreRouter(
		NewConfigMapSessionStore(newSessionLeaseTestClient()),
		NewCRDSessionStore(newCRDTestClient()),
	)

	lock, err := store.AcquireSessionLock(context.Background(), "system", "router-lock")
	if err != nil {
		t.Fatal(err)
	}

	if lock == nil {
		t.Fatal("router returned a nil session lock")
	}

	if err := lock.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := store.DeleteSessionLease(context.Background(), "system", "router-lock"); err != nil {
		t.Fatal(err)
	}
}

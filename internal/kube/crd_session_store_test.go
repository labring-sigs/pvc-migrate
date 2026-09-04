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
	"k8s.io/apimachinery/pkg/types"
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

func TestWorkflowObjectForKindCoversControllerRegistry(t *testing.T) {
	for _, workflow := range domain.ControllerWorkflows() {
		for _, kind := range []domain.ControllerKind{workflow.Kind, workflow.ClusterKind} {
			if kind == "" {
				continue
			}

			object := WorkflowObjectForKind(kind)
			if object == nil || workflowKind(object) != kind {
				t.Fatalf(
					"workflow kind %q object=%T resolves to %q",
					kind,
					object,
					workflowKind(object),
				)
			}
		}
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

func TestCRDSessionStoreUpdatesSameNamespaceClusterWorkflow(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(newCRDTestClient())
	session := storeTestSession()
	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		session.Spec.SessionCommon,
		false,
		domain.SessionWorkflowOptions{},
	)
	session.BackendResource = domain.ControllerKindClusterCopy

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.GetByKind(ctx, "", session.ID, domain.ControllerKindClusterCopy)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.BackendResource != domain.ControllerKindClusterCopy ||
		loaded.Status.Phase != domain.PhaseReserving {
		t.Fatalf(
			"loaded cluster workflow resource=%q phase=%q",
			loaded.BackendResource,
			loaded.Status.Phase,
		)
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

func TestCRDSessionStorePersistsClusterDestinationIdentityOnlyInStatus(t *testing.T) {
	ctx := context.Background()
	client := &updateCountingClient{Client: newCRDTestClient()}
	store := NewCRDSessionStore(client)
	base := storeTestSession()
	common := base.Spec.SessionCommon
	common.SourceNamespace = "source"
	common.TemporaryNamespace = "destination"
	common.DestinationNamespace = "destination"
	common.SessionNamespace = "system"
	common.Volumes[0].SourcePVC.Namespace = "source"
	common.Volumes[0].DestinationPVC.Namespace = "destination"
	spec := domain.NewSessionSpec(
		domain.OperationCopy,
		common,
		true,
		domain.SessionWorkflowOptions{},
	)
	session := domain.NewSession(base.ID, spec, time.Now())

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
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
		t.Fatalf("cluster runtime checkpoint caused %d main-resource updates", client.updates)
	}

	var object v1alpha1.ClusterCopy
	if err := client.Get(ctx, crclient.ObjectKey{Name: session.ID}, &object); err != nil {
		t.Fatal(err)
	}

	if object.Spec.Volumes[0].DestinationPVC.UID != "" ||
		object.Status.Volumes[0].DestinationPVC.UID != "destination-pvc-uid" ||
		object.Status.Volumes[0].DestinationPV.Name != "destination-pv" {
		t.Fatalf(
			"unexpected ClusterCopy spec/status checkpoint: %#v %#v",
			object.Spec,
			object.Status,
		)
	}

	loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Spec.Volumes[0].DestinationPVC.UID != "destination-pvc-uid" ||
		loaded.Spec.Volumes[0].DestinationPV.UID != "destination-pv-uid" ||
		loaded.Spec.Volumes[0].DestinationPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Fatalf("cluster destination checkpoint was not restored: %#v", loaded.Spec.Volumes[0])
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
				APIVersion: domain.CoreAPIVersion, Kind: domain.KindPod,
				Namespace: "system", Name: "writer", UID: "original-pod-uid",
			},
			AffectedPods: []domain.ObjectReference{{
				APIVersion: domain.CoreAPIVersion, Kind: domain.KindPod,
				Namespace: "system", Name: "writer", UID: "original-pod-uid",
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

func TestDecodeWorkflowInitializesDeclarativeResource(t *testing.T) {
	session := storeTestSession()

	objectValue := sessionObjectFor(session)
	if objectValue == nil {
		t.Fatal("failed to build workflow object")
	}

	object, ok := objectValue.(*v1alpha1.Migration)
	if !ok {
		t.Fatalf("workflow object type=%T", objectValue)
	}

	object.Labels = nil
	object.Status = v1alpha1.MigrationStatus{}
	object.ResourceVersion = "17"
	object.Generation = 3

	decoded, err := DecodeWorkflow(object)
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

func TestCRDSessionStoreAddsProtectionToDeclarativeWorkflow(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)
	session := storeTestSession()

	objectValue := sessionObjectFor(session)
	if objectValue == nil {
		t.Fatal("failed to build workflow object")
	}

	object, ok := objectValue.(*v1alpha1.Migration)
	if !ok {
		t.Fatalf("workflow object type=%T", objectValue)
	}

	object.SetFinalizers(nil)
	object.SetLabels(nil)

	object.Status = v1alpha1.MigrationStatus{}
	if err := client.Create(ctx, object); err != nil {
		t.Fatal(err)
	}

	decoded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.EnsureSessionProtection(ctx, decoded); err != nil {
		t.Fatal(err)
	}

	var current v1alpha1.Migration
	if err := client.Get(ctx, crclient.ObjectKeyFromObject(object), &current); err != nil {
		t.Fatal(err)
	}

	if !containsString(current.GetFinalizers(), SessionFinalizer) {
		t.Fatalf(
			"declarative workflow did not receive protection finalizer: %v",
			current.GetFinalizers(),
		)
	}

	if decoded.ResourceVersion != current.GetResourceVersion() {
		t.Fatalf(
			"session resourceVersion was not refreshed after finalizer update: session=%q object=%q",
			decoded.ResourceVersion,
			current.GetResourceVersion(),
		)
	}

	if err := store.Update(ctx, decoded); err != nil {
		t.Fatalf("session update after finalizer protection should not conflict: %v", err)
	}
}

func TestCRDSessionStoreGetFiltersClusterWorkflowBySessionNamespace(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(newCRDTestClient())
	session := storeTestSession()
	session.Spec.SourceNamespace = "source"
	session.Spec.TemporaryNamespace = "destination"
	session.Spec.DestinationNamespace = "destination"
	session.Spec.SessionNamespace = "control-a"
	session.Spec.Volumes[0].SourcePVC.Namespace = "source"
	session.Spec.Volumes[0].DestinationPVC.Namespace = "destination"

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Get(ctx, "control-b", session.ID); !IsSessionNotFound(err) {
		t.Fatalf("cross-namespace cluster lookup error=%v", err)
	}

	loaded, err := store.Get(ctx, "control-a", session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.BackendResource != domain.ControllerKindClusterMigration {
		t.Fatalf("cluster workflow resource=%q", loaded.BackendResource)
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
			return session
		}},
		{
			name: "pod migration",
			kind: domain.ControllerKindPodMigration,
			make: func() *domain.Session {
				session := storeTestSession()
				session.Spec = domain.NewPodMigrationSessionSpec(domain.SessionCommon{
					SourceNamespace:      "system",
					TemporaryNamespace:   "system",
					DestinationNamespace: "system",
					SessionNamespace:     "system",
					Volumes:              session.Spec.Volumes,
				}, domain.WorkloadSpec{Adapter: domain.WorkloadStandalone, Pod: domain.ObjectReference{
					APIVersion: domain.CoreAPIVersion, Kind: domain.KindPod,
					Namespace: "system", Name: "workload", UID: "pod-uid",
				}}, domain.SessionWorkflowOptions{}, 1, false)

				return domain.NewSession(session.ID, session.Spec, time.Now())
			},
		},
		{name: "reservation", kind: domain.ControllerKindReservation, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
				SourceNamespace:      "system",
				TemporaryNamespace:   "system",
				DestinationNamespace: "system",
				SessionNamespace:     "system",
				Volumes:              session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})

			return domain.NewSession(session.ID, session.Spec, time.Now())
		}},
		{name: "copy", kind: domain.ControllerKindCopy, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
				SourceNamespace:      "system",
				TemporaryNamespace:   "system",
				DestinationNamespace: "system",
				SessionNamespace:     "system",
				Volumes:              session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})

			return domain.NewSession(session.ID, session.Spec, time.Now())
		}},
		{name: "backup", kind: domain.ControllerKindBackup, make: func() *domain.Session {
			spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
				SourceNamespace:      "system",
				SessionNamespace:     "system",
				DestinationNamespace: "system",
			}, false, domain.SessionWorkflowOptions{})
			spec.Backup.SourcePVC = domain.ObjectReference{
				Namespace: "system",
				Name:      "data",
				UID:       "pvc-uid",
			}
			spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
			spec.Backup.Name = "daily"
			spec.Backup.BackupRepository = "default"

			return domain.NewSession("alpha", spec, time.Now())
		}},
		{name: "restore", kind: domain.ControllerKindRestore, make: func() *domain.Session {
			spec := domain.NewSessionSpec(domain.OperationRestore, domain.SessionCommon{
				SourceNamespace:      "system",
				SessionNamespace:     "system",
				DestinationNamespace: "system",
			}, false, domain.SessionWorkflowOptions{})
			spec.Restore.DestinationPVC = domain.ObjectReference{
				Namespace: "system",
				Name:      "data",
				UID:       "pvc-uid",
			}
			spec.Restore.Name = "daily"
			spec.Restore.BackupRepository = "default"

			return domain.NewSession("alpha", spec, time.Now())
		}},
		{name: "rename", kind: domain.ControllerKindRename, make: func() *domain.Session {
			session := storeTestSession()
			session.Spec = domain.NewSessionSpec(domain.OperationRename, domain.SessionCommon{
				SourceNamespace:      "system",
				TemporaryNamespace:   "system",
				DestinationNamespace: "system",
				SessionNamespace:     "system",
				Volumes:              session.Spec.Volumes,
			}, false, domain.SessionWorkflowOptions{})

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

func TestDecodeWorkflowMarksDeletionRequests(t *testing.T) {
	session := storeTestSession()

	object, ok := sessionObjectForKind(session, domain.ControllerKindMigration)
	if !ok {
		t.Fatal("failed to construct Migration object")
	}

	object.SetDeletionTimestamp(&metav1.Time{Time: time.Now()})

	decoded, err := DecodeWorkflow(object)
	if err != nil {
		t.Fatal(err)
	}

	if !decoded.Deleting {
		t.Fatal("deletion timestamp was not propagated to the controller session")
	}
}

func TestCRDSessionStoreRejectsSameNameAcrossKinds(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(newCRDTestClient())

	first := storeTestSession()
	if err := store.Create(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := storeTestSession()

	second.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
		SessionNamespace: "system", Volumes: first.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	if err := store.Create(ctx, second); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf(
			"same-name different-kind create category=%s error=%v",
			domain.CategoryOf(err),
			err,
		)
	}
}

func TestCRDSessionStoreDetectsDeclarativeCollisionAcrossNamespaceRoles(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)

	namespaced := storeTestSession()
	namespaced.Spec.SessionNamespace = "source"
	namespaced.Spec.SourceNamespace = "source"
	namespaced.Spec.TemporaryNamespace = "source"
	namespaced.Spec.DestinationNamespace = "source"
	namespaced.Spec.Volumes[0].SourcePVC.Namespace = "source"
	namespaced.Spec.Volumes[0].DestinationPVC.Namespace = "source"

	if err := store.Create(ctx, namespaced); err != nil {
		t.Fatal(err)
	}

	cluster := storeTestSession()
	cluster.Spec = domain.NewSessionSpec(domain.OperationMove, domain.SessionCommon{
		SourceNamespace:      "source",
		TemporaryNamespace:   "destination",
		DestinationNamespace: "destination",
		SessionNamespace:     "system",
		Volumes:              cluster.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	cluster.Spec.Volumes[0].SourcePVC.Namespace = "source"
	cluster.Spec.Volumes[0].DestinationPVC.Namespace = "destination"

	err := store.CheckWorkflowNameCollision(ctx, cluster)
	if domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf(
			"cross-boundary collision category=%s error=%v",
			domain.CategoryOf(err),
			err,
		)
	}
}

func TestCRDSessionLockIDIsSharedAcrossWorkflowKinds(t *testing.T) {
	migration := storeTestSession()
	migration.Backend = SessionBackendCRD
	migration.BackendResource = domain.ControllerKindMigration
	copySession := *migration
	copySession.BackendResource = domain.ControllerKindCopy

	migrationID := SessionLockID(migration)
	copyID := SessionLockID(&copySession)

	if migrationID != migration.ID || copyID != copySession.ID || migrationID != copyID {
		t.Fatalf("CRD lock IDs differ: migration=%q copy=%q", migrationID, copyID)
	}

	unpersisted := storeTestSession()
	if got := SessionLockID(unpersisted); got != unpersisted.ID {
		t.Fatalf("unpersisted CRD lock ID=%q want=%q", got, unpersisted.ID)
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
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
		SessionNamespace: "system", Volumes: first.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})

	object := sessionObjectFor(second)
	if object == nil {
		t.Fatal("failed to construct duplicate workflow")
	}

	if err := client.Create(ctx, object); err != nil {
		t.Fatal(err)
	}

	listed, err := store.List(ctx, "system")
	if err != nil || len(listed) != 2 {
		t.Fatalf("list same-name workflows=%#v error=%v", listed, err)
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

func TestCRDSessionStorePreservesWorkflowMetadata(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)
	session := storeTestSession()

	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	workflow := &v1alpha1.Migration{}
	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		workflow,
	); err != nil {
		t.Fatal(err)
	}

	workflow.Labels["tenant.example/owner"] = "team-a"
	workflow.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "owner",
		UID:        types.UID("owner-uid"),
	}}

	if err := client.Update(ctx, workflow); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.GetByKind(ctx, "system", session.ID, domain.ControllerKindMigration)
	if err != nil {
		t.Fatal(err)
	}

	if err := loaded.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, loaded); err != nil {
		t.Fatal(err)
	}

	workflow = &v1alpha1.Migration{}
	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		workflow,
	); err != nil {
		t.Fatal(err)
	}

	if workflow.Labels["tenant.example/owner"] != "team-a" || len(workflow.OwnerReferences) != 1 {
		t.Fatalf(
			"workflow metadata was replaced: labels=%v owners=%v",
			workflow.Labels,
			workflow.OwnerReferences,
		)
	}

	loaded.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
		SessionNamespace: "system", Volumes: loaded.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})

	if err := store.Update(ctx, loaded); err != nil {
		t.Fatal(err)
	}

	copyWorkflow := &v1alpha1.Copy{}
	if err := client.Get(
		ctx,
		crclient.ObjectKey{Namespace: "system", Name: session.ID},
		copyWorkflow,
	); err != nil {
		t.Fatal(err)
	}

	if copyWorkflow.Labels["tenant.example/owner"] != "team-a" ||
		len(copyWorkflow.OwnerReferences) != 1 {
		t.Fatalf(
			"rebound metadata was replaced: labels=%v owners=%v",
			copyWorkflow.Labels,
			copyWorkflow.OwnerReferences,
		)
	}
}

func TestCRDSessionStoreRebindsReservationToCopy(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)
	session := storeTestSession()

	session.Spec = domain.NewSessionSpec(domain.OperationReserve, domain.SessionCommon{
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
		SessionNamespace: "system", Volumes: session.Spec.Volumes,
	}, false, domain.SessionWorkflowOptions{})
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	session.Spec = domain.NewSessionSpec(domain.OperationCopy, domain.SessionCommon{
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
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

func TestCRDSessionStoreRebindsClusterReservationToClusterCopy(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)
	session := storeTestSession()
	common := domain.SessionCommon{
		SourceNamespace:      "source",
		TemporaryNamespace:   "control",
		DestinationNamespace: "destination",
		SessionNamespace:     "control",
		Volumes:              session.Spec.Volumes,
	}
	common.Volumes[0].SourcePVC.Namespace = common.SourceNamespace
	common.Volumes[0].DestinationPVC.Namespace = common.TemporaryNamespace

	session.Spec = domain.NewSessionSpec(
		domain.OperationReserve,
		common,
		false,
		domain.SessionWorkflowOptions{},
	)
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	session.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		common,
		false,
		domain.SessionWorkflowOptions{},
	)
	if err := session.Transition(domain.PhaseReserving, "reserving", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := session.Transition(domain.PhaseReserved, "reserved", time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Get(ctx, common.SessionNamespace, session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.BackendResource != domain.ControllerKindClusterCopy ||
		loaded.Spec.Type != domain.SessionTypeCopy {
		t.Fatalf(
			"cluster rebind resource=%q type=%q",
			loaded.BackendResource,
			loaded.Spec.Type,
		)
	}

	if err := client.Get(
		ctx,
		crclient.ObjectKey{Name: session.ID},
		&v1alpha1.ClusterCopy{},
	); err != nil {
		t.Fatal(err)
	}

	if err := client.Get(
		ctx,
		crclient.ObjectKey{Name: session.ID},
		&v1alpha1.ClusterReservation{},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("old ClusterReservation still exists: %v", err)
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
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
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
		SourceNamespace: "system", TemporaryNamespace: "system", DestinationNamespace: "system",
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

func newCRDTestClient() crclient.Client {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	return crfake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&v1alpha1.Migration{}, &v1alpha1.PodMigration{}, &v1alpha1.Reservation{},
			&v1alpha1.Copy{}, &v1alpha1.Backup{}, &v1alpha1.Restore{}, &v1alpha1.Rename{},
			&v1alpha1.ClusterMigration{}, &v1alpha1.ClusterPodMigration{}, &v1alpha1.ClusterReservation{},
			&v1alpha1.ClusterCopy{}, &v1alpha1.Move{},
		).
		Build()
}

func tenantScopedCRDClient(t *testing.T) crclient.Client {
	t.Helper()

	base := newCRDTestClient()

	withWatch, ok := base.(crclient.WithWatch)
	if !ok {
		t.Fatal("fake CRD client does not support Watch")
	}

	forbidden := func(kind, name string) error {
		return apierrors.NewForbidden(
			schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: kind},
			name,
			errors.New("tenant RoleBinding does not grant this workflow Kind"),
		)
	}

	workflowListKind := func(list crclient.ObjectList) (domain.ControllerKind, bool) {
		switch list.(type) {
		case *v1alpha1.MigrationList:
			return domain.ControllerKindMigration, true
		case *v1alpha1.PodMigrationList:
			return domain.ControllerKindPodMigration, true
		case *v1alpha1.ReservationList:
			return domain.ControllerKindReservation, true
		case *v1alpha1.CopyList:
			return domain.ControllerKindCopy, true
		case *v1alpha1.BackupList:
			return domain.ControllerKindBackup, true
		case *v1alpha1.RestoreList:
			return domain.ControllerKindRestore, true
		case *v1alpha1.RenameList:
			return domain.ControllerKindRename, true
		default:
			return "", false
		}
	}

	return interceptor.NewClient(withWatch, interceptor.Funcs{
		Get: func(
			ctx context.Context,
			underlying crclient.WithWatch,
			key crclient.ObjectKey,
			object crclient.Object,
			options ...crclient.GetOption,
		) error {
			if kind := workflowKind(object); kind != domain.ControllerKindMigration {
				return forbidden(strings.ToLower(string(kind)), key.Name)
			}

			return underlying.Get(ctx, key, object, options...)
		},
		List: func(
			ctx context.Context,
			underlying crclient.WithWatch,
			list crclient.ObjectList,
			options ...crclient.ListOption,
		) error {
			kind, ok := workflowListKind(list)
			if !ok || kind != domain.ControllerKindMigration {
				return forbidden(strings.ToLower(string(kind)), "")
			}

			return underlying.List(ctx, list, options...)
		},
		SubResourceUpdate: func(
			ctx context.Context,
			underlying crclient.Client,
			subResource string,
			object crclient.Object,
			options ...crclient.SubResourceUpdateOption,
		) error {
			if subResource == "status" {
				return forbidden(strings.ToLower(string(workflowKind(object))), object.GetName())
			}

			return underlying.SubResource(subResource).Update(ctx, object, options...)
		},
	})
}

func TestCRDSessionStoreReportsWhenEveryKindIsForbidden(t *testing.T) {
	base := newCRDTestClient()

	withWatch, ok := base.(crclient.WithWatch)
	if !ok {
		t.Fatal("fake CRD client does not support Watch")
	}

	client := interceptor.NewClient(withWatch, interceptor.Funcs{
		Get: func(
			_ context.Context,
			_ crclient.WithWatch,
			key crclient.ObjectKey,
			object crclient.Object,
			_ ...crclient.GetOption,
		) error {
			return apierrors.NewForbidden(
				schema.GroupResource{
					Group:    v1alpha1.GroupVersion.Group,
					Resource: strings.ToLower(string(workflowKind(object))),
				},
				key.Name,
				errors.New("denied"),
			)
		},
	})

	_, err := NewCRDSessionStore(client).Get(context.Background(), "tenant", "missing")
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "no workflow kind") {
		t.Fatalf("category=%q error=%v", domain.CategoryOf(err), err)
	}
}

func TestCRDSessionStoreSupportsLeastPrivilegeTenantAccess(t *testing.T) {
	ctx := context.Background()
	store := NewCRDSessionStore(tenantScopedCRDClient(t))
	session := storeTestSession()

	if err := store.Create(ctx, session); err != nil {
		t.Fatalf("tenant create failed: %v", err)
	}

	// The tenant can create the workflow and read its spec, while status is
	// controller-owned. The store must return a derived Planned checkpoint until
	// the controller writes the status subresource.
	if session.Status.Phase != domain.PhasePlanned {
		t.Fatalf("tenant create phase=%q, want Planned", session.Status.Phase)
	}

	loaded, err := store.Get(ctx, session.Spec.SessionNamespace, session.ID)
	if err != nil {
		t.Fatalf("tenant get failed: %v", err)
	}

	if loaded.Status.Phase != domain.PhasePlanned {
		t.Fatalf("tenant get phase=%q, want Planned", loaded.Status.Phase)
	}

	loaded, err = store.GetByType(
		ctx,
		session.Spec.SessionNamespace,
		session.ID,
		domain.SessionTypeMigrate,
	)
	if err != nil || loaded.Spec.Type != domain.SessionTypeMigrate {
		t.Fatalf("tenant get by type=%#v error=%v", loaded, err)
	}

	listed, err := store.List(ctx, session.Spec.SessionNamespace)
	if err != nil {
		t.Fatalf("tenant list failed: %v", err)
	}

	if len(listed) != 1 || listed[0].ID != session.ID {
		t.Fatalf("tenant list=%#v, want one migration", listed)
	}
}

func TestCRDSessionStoreGetByKindDisambiguatesSameNameWorkflows(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)

	migration := storeTestSession()
	copySession := storeTestSession()
	copySession.Spec = domain.NewSessionSpec(
		domain.OperationCopy,
		migration.Spec.SessionCommon,
		false,
		domain.SessionWorkflowOptions{},
	)
	copySession.Spec.Copy.Online = false

	if err := client.Create(ctx, sessionObjectFor(migration)); err != nil {
		t.Fatalf("create migration: %v", err)
	}

	if err := client.Create(ctx, sessionObjectFor(copySession)); err != nil {
		t.Fatalf("create copy: %v", err)
	}

	loadedMigration, err := store.GetByKind(
		ctx,
		migration.Spec.SessionNamespace,
		migration.ID,
		domain.ControllerKindMigration,
	)
	if err != nil {
		t.Fatalf("get migration: %v", err)
	}

	if loadedMigration.Spec.Type != domain.SessionTypeMigrate {
		t.Fatalf("migration type=%s", loadedMigration.Spec.Type)
	}

	loadedCopy, err := store.GetByKind(
		ctx,
		copySession.Spec.SessionNamespace,
		copySession.ID,
		domain.ControllerKindCopy,
	)
	if err != nil {
		t.Fatalf("get copy: %v", err)
	}

	if loadedCopy.Spec.Type != domain.SessionTypeCopy {
		t.Fatalf("copy type=%s", loadedCopy.Spec.Type)
	}

	byType, err := store.GetByType(
		ctx,
		"system",
		copySession.ID,
		domain.SessionTypeCopy,
	)
	if err != nil || byType.Spec.Type != domain.SessionTypeCopy {
		t.Fatalf("get by type copy=%#v error=%v", byType, err)
	}
}

func TestCRDSessionStoreGetByTypeSupportsClusterOnlyWorkflow(t *testing.T) {
	ctx := context.Background()
	client := newCRDTestClient()
	store := NewCRDSessionStore(client)
	session := storeTestSession()
	session.Spec = domain.NewSessionSpec(
		domain.OperationMove,
		domain.SessionCommon{
			SourceNamespace:      "source",
			TemporaryNamespace:   "destination",
			DestinationNamespace: "destination",
			SessionNamespace:     "system",
			Volumes:              session.Spec.Volumes,
		},
		false,
		domain.SessionWorkflowOptions{},
	)

	if err := client.Create(ctx, sessionObjectFor(session)); err != nil {
		t.Fatalf("create Move: %v", err)
	}

	loaded, err := store.GetByType(ctx, "system", session.ID, domain.SessionTypeMove)
	if err != nil {
		t.Fatalf("get Move by type: %v", err)
	}

	if loaded.Spec.Type != domain.SessionTypeMove ||
		loaded.BackendResource != domain.ControllerKindMove {
		t.Fatalf("loaded Move=%#v", loaded)
	}
}

func TestControllerSessionSupportedBoundaries(t *testing.T) {
	session := storeTestSession()
	if !ControllerSessionSupported(session) {
		t.Fatal("same-namespace migrate should be controller compatible")
	}

	// A cluster-scoped CR is valid even when all namespace roles are equal. The
	// CLI keeps namespaced resources as its tenant-local default, while an
	// administrator may submit the cluster API explicitly.
	sameNamespaceCluster := storeTestSession()

	sameNamespaceCluster.BackendResource = domain.ControllerKindClusterMigration
	if !ControllerSessionSupported(sameNamespaceCluster) {
		t.Fatal("same-namespace cluster migration should be controller compatible")
	}

	session.Spec.DestinationNamespace = "archive"
	if !ControllerSessionSupported(session) {
		t.Fatal("cross-namespace migrate should use the cluster workflow")
	}

	resource, ok := domain.ControllerResourceForSession(session)
	if !ok || resource.Kind != domain.ControllerKindClusterMigration || !resource.Cluster {
		t.Fatalf("cross-namespace resource=%#v, found=%t", resource, ok)
	}

	session.BackendResource = domain.ControllerKindMigration
	if ControllerSessionSupported(session) {
		t.Fatal("persisted namespaced workflow must reject cross-namespace spec mutation")
	}

	session.BackendResource = domain.ControllerKindClusterMigration
	if !ControllerSessionSupported(session) {
		t.Fatal("persisted cluster workflow should accept qualified cross-namespace references")
	}
}

func TestControllerSessionSupportedRejectsCrossNamespaceBackup(t *testing.T) {
	spec := domain.NewSessionSpec(domain.OperationBackup, domain.SessionCommon{
		SourceNamespace: "tenant", SessionNamespace: "system",
	}, false, domain.SessionWorkflowOptions{})
	spec.Backup.SourcePVC = domain.ObjectReference{
		Namespace: "tenant",
		Name:      "data",
		UID:       "pvc-uid",
	}
	spec.Backup.SourcePV = domain.ObjectReference{Name: "pv-data", UID: "pv-uid"}
	spec.Backup.Name = "daily"
	spec.Backup.BackupRepository = "archive"
	session := domain.NewSession("cross-namespace-backup", spec, time.Now())

	if ControllerSessionSupported(session) {
		t.Fatal("cross-namespace backup must fall back because Backup has no cluster-scoped API")
	}

	if _, ok := domain.ControllerResourceForSession(session); ok {
		t.Fatal("cross-namespace backup unexpectedly resolved a controller resource")
	}
}

func TestDecodeWorkflowDerivesTenantBoundaryFromMetadataNamespace(t *testing.T) {
	objectValue := sessionObjectFor(storeTestSession())
	if objectValue == nil {
		t.Fatal("failed to build workflow object")
	}

	object, ok := objectValue.(*v1alpha1.Migration)
	if !ok {
		t.Fatalf("workflow object type=%T", objectValue)
	}

	object.SetNamespace("other")

	decoded, err := DecodeWorkflow(object)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Spec.SourceNamespace != "other" ||
		decoded.Spec.DestinationNamespace != "other" ||
		decoded.Spec.SessionNamespace != "other" ||
		decoded.Spec.Volumes[0].SourcePVC.Namespace != "other" ||
		decoded.Spec.Volumes[0].DestinationPVC.Namespace != "other" {
		t.Fatalf("metadata namespace was not applied consistently: %#v", decoded.Spec)
	}
}

func TestControllerNamespaceBoundaryRejectsNamespacedPVReferences(t *testing.T) {
	session := storeTestSession()
	session.Spec.Volumes[0].SourcePV.Namespace = "system"

	err := ControllerNamespaceBoundaryError(session)
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "PV references") {
		t.Fatalf("namespaced source PV error=%v category=%s", err, domain.CategoryOf(err))
	}

	session = storeTestSession()
	session.Spec.Volumes[0].DestinationPV = domain.ObjectReference{
		Name: "pv-destination",
		UID:  "destination-uid",
	}

	session.Spec.Volumes[0].DestinationPV.Namespace = "system"
	if err := ControllerNamespaceBoundaryError(
		session,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("namespaced destination PV error=%v category=%s", err, domain.CategoryOf(err))
	}
}

func TestControllerNamespaceBoundaryAllowsOnlySupportedWorkloadGVKs(t *testing.T) {
	ref := func(apiVersion, kind string) domain.ObjectReference {
		return domain.ObjectReference{
			APIVersion: apiVersion,
			Kind:       kind,
			Namespace:  "system",
			Name:       "source",
			UID:        "source-uid",
		}
	}
	newSession := func(workload domain.WorkloadSpec) *domain.Session {
		spec := domain.NewPodMigrationSessionSpec(
			domain.SessionCommon{
				SourceNamespace:      "system",
				TemporaryNamespace:   "system",
				DestinationNamespace: "system",
				SessionNamespace:     "system",
			},
			workload,
			domain.SessionWorkflowOptions{},
			1,
			false,
		)

		return domain.NewSession("workload", spec, time.Unix(100, 0))
	}

	valid := []struct {
		name     string
		workload domain.WorkloadSpec
	}{
		{
			name: "standalone pod",
			workload: domain.WorkloadSpec{
				Adapter: domain.WorkloadStandalone,
				Pod:     ref(domain.CoreAPIVersion, domain.KindPod),
			},
		},
		{
			name: "deployment",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadDeployment,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref(domain.AppsAPIVersion, domain.KindDeployment),
			},
		},
		{
			name: "statefulset",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadStatefulSet,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref(domain.AppsAPIVersion, domain.KindStatefulSet),
			},
		},
		{
			name: "victoria logs",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadVictoriaLogs,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref(domain.AppsAPIVersion, domain.KindStatefulSet),
			},
		},
		{
			name: "vmcluster",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadVMCluster,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref(domain.AppsAPIVersion, domain.KindStatefulSet),
				VMCluster: &domain.VMClusterSpec{
					APIVersion: "operator.victoriametrics.com/v1beta1",
					Name:       "vm",
					UID:        "vm-uid",
				},
			},
		},
		{
			name: "grafana",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadGrafana,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref(domain.AppsAPIVersion, domain.KindDeployment),
				Grafana: &domain.GrafanaSpec{
					APIVersion: "grafana.integreatly.org/v1beta1",
					Name:       "grafana",
					UID:        "grafana-uid",
				},
			},
		},
		{
			name: "kubeblocks instanceset",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadKubeBlocks,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref("workloads.kubeblocks.io/v1alpha1", domain.KindInstanceSet),
				KubeBlocks: &domain.KubeBlocksSpec{
					Cluster:       "cluster",
					ClusterUID:    "cluster-uid",
					OpsAPIVersion: "operations.kubeblocks.io/v1alpha1",
				},
			},
		},
		{
			name: "kubeblocks mongo statefulset",
			workload: domain.WorkloadSpec{
				Adapter:    domain.WorkloadKubeBlocks,
				Pod:        ref(domain.CoreAPIVersion, domain.KindPod),
				Controller: ref(domain.AppsAPIVersion, domain.KindStatefulSet),
				KubeBlocks: &domain.KubeBlocksSpec{
					Cluster:       "cluster",
					ClusterUID:    "cluster-uid",
					OpsAPIVersion: "apps.kubeblocks.io/v1alpha1",
				},
			},
		},
	}
	for _, test := range valid {
		t.Run(test.name, func(t *testing.T) {
			if err := ControllerNamespaceBoundaryError(newSession(test.workload)); err != nil {
				t.Fatalf("valid workload rejected: %v", err)
			}
		})
	}

	t.Run("rejects arbitrary controller GVK", func(t *testing.T) {
		workload := valid[1].workload

		workload.Controller = ref("v1", "Secret")
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("arbitrary controller GVK category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("rejects arbitrary pod GVK", func(t *testing.T) {
		workload := valid[0].workload

		workload.Pod = ref("apps/v1", "Deployment")
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("arbitrary pod GVK category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("rejects arbitrary affected Pod GVK", func(t *testing.T) {
		workload := valid[1].workload

		workload.AffectedPods = []domain.ObjectReference{ref("v1", "Secret")}
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("arbitrary affected Pod GVK category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("rejects incomplete workload identity", func(t *testing.T) {
		workload := valid[0].workload

		workload.Pod.APIVersion = ""
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf(
				"incomplete workload identity category=%s error=%v",
				domain.CategoryOf(err),
				err,
			)
		}
	})
	t.Run("rejects arbitrary KubeBlocks OpsRequest version", func(t *testing.T) {
		workload := valid[6].workload

		workload.KubeBlocks.OpsAPIVersion = "operations.kubeblocks.io/v1beta9"
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("arbitrary KubeBlocks API category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("rejects arbitrary VMCluster version", func(t *testing.T) {
		workload := valid[4].workload

		workload.VMCluster.APIVersion = "operator.victoriametrics.com/v1"
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("arbitrary VMCluster API category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
	t.Run("rejects arbitrary Grafana version", func(t *testing.T) {
		workload := valid[5].workload

		workload.Grafana.APIVersion = "grafana.integreatly.org/v1"
		if err := ControllerNamespaceBoundaryError(
			newSession(workload),
		); domain.CategoryOf(
			err,
		) != domain.ErrorPrecondition {
			t.Fatalf("arbitrary Grafana API category=%s error=%v", domain.CategoryOf(err), err)
		}
	})
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

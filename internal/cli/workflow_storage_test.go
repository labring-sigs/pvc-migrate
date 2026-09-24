package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
	"github.com/labring-sigs/pvc-migrate/internal/planner"
	"github.com/spf13/cobra"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newWorkflowLookupRuntime(t *testing.T, objects ...crclient.Object) *commandRuntime {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	return &commandRuntime{clients: &kube.Clients{
		Kubernetes: kubefake.NewClientset(),
		Runtime:    crfake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
	}}
}

// sessionRecordClient builds a fake cluster holding one bound tenant PVC and
// a reactor that records every ConfigMap create and then fails it, so a run
// command stops exactly at its record persistence step.
func sessionRecordClient(t *testing.T) (*kubefake.Clientset, *[]string) {
	t.Helper()

	mode := corev1.PersistentVolumeFilesystem
	client := kubefake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default", UID: "namespace"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "data",
				UID:       types.UID("pvc"),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:  "pv-data",
				VolumeMode:  &mode,
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-data", UID: types.UID("pv")},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
				ClaimRef: &corev1.ObjectReference{
					Namespace: "default",
					Name:      "data",
					UID:       types.UID("pvc"),
				},
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
	)

	created := &[]string{}
	client.PrependReactor(
		"create",
		"configmaps",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(k8stesting.CreateActionImpl)
			if !ok {
				return true, nil, errors.New("unexpected configmap action")
			}

			*created = append(*created, create.Namespace)

			return true, nil, errors.New("session record create unavailable")
		},
	)
	client.PrependReactor(
		"create",
		"selfsubjectaccessreviews",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			review, ok := action.(k8stesting.CreateActionImpl).Object.(*authorizationv1.SelfSubjectAccessReview)
			if !ok {
				return true, nil, errors.New("unexpected access review object")
			}

			review = review.DeepCopy()
			review.Status.Allowed = true

			return true, review, nil
		},
	)

	return client, created
}

// TestBackupRunStoresRecordInSessionNamespace pins the session backup run's
// record location: the store must target the session storage namespace, never
// the tenant namespace the command's -n addresses, or the lifecycle verbs —
// which resolve records through the session namespace — cannot find it.
func TestBackupRunStoresRecordInSessionNamespace(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	client, created := sessionRecordClient(t)

	var stderr bytes.Buffer

	command := NewRoot(Options{
		Version: "v1.2.3",
		In:      strings.NewReader(""),
		Out:     io.Discard,
		ErrOut:  &stderr,
		runtimeFactory: func(state *rootState) (*commandRuntime, error) {
			return &commandRuntime{
				clients: &kube.Clients{Kubernetes: client},
				planner: planner.New(client, nil),
				printer: printerFor(state),
			}, nil
		},
		objectStoreFactory: func(_ context.Context, cfg objectstore.Config) (*objectstore.Store, error) {
			return objectstore.NewWithClient(
				&testObjectStoreClient{},
				cfg,
				objectstore.Credentials{AccessKey: "test", SecretKey: "test"},
			)
		},
	})
	command.SetArgs([]string{
		"backup",
		"--source-pvc", "data",
		"--backend", "s3",
		"--bucket", "backups",
		"--name", "daily",
		"--yes", "--dry-run=false",
	})

	if err := command.Execute(); err == nil {
		t.Fatal("expected the record create failure to surface")
	}

	if len(*created) != 1 {
		t.Fatalf("record creates = %v, want exactly one", *created)
	}

	if (*created)[0] != "pvc-migrate-system" {
		t.Fatalf("record create namespace = %q, want the session namespace", (*created)[0])
	}

	if !strings.Contains(stderr.String(), "--namespace pvc-migrate-system get configmap") {
		t.Fatalf("creation hint must inspect the session namespace: %s", stderr.String())
	}

	if strings.Contains(stderr.String(), "--namespace default get configmap") {
		t.Fatalf("creation must not hint the tenant namespace: %s", stderr.String())
	}
}

// TestRenameCreationHintInspectsSessionNamespace pins the rename create
// failure hint: the record persists in the session storage namespace, so the
// kubectl inspection must point there instead of the tenant namespace.
func TestRenameCreationHintInspectsSessionNamespace(t *testing.T) {
	client, created := sessionRecordClient(t)

	var stderr bytes.Buffer

	command := NewRoot(Options{
		Version: "v1.2.3",
		In:      strings.NewReader(""),
		Out:     io.Discard,
		ErrOut:  &stderr,
		runtimeFactory: func(state *rootState) (*commandRuntime, error) {
			return &commandRuntime{
				clients: &kube.Clients{Kubernetes: client},
				planner: planner.New(client, nil),
				printer: printerFor(state),
			}, nil
		},
	})
	command.SetArgs([]string{
		"rename",
		"-n", "default",
		"--source-pvc", "data",
		"--destination-pvc", "renamed",
		"--yes", "--dry-run=false",
	})

	if err := command.Execute(); err == nil {
		t.Fatal("expected the record create failure to surface")
	}

	if len(*created) != 1 {
		t.Fatalf("record creates = %v, want exactly one", *created)
	}

	if (*created)[0] != "pvc-migrate-system" {
		t.Fatalf("record create namespace = %q, want the session namespace", (*created)[0])
	}

	if !strings.Contains(stderr.String(), "--namespace pvc-migrate-system get configmap") {
		t.Fatalf("creation hint must inspect the session namespace: %s", stderr.String())
	}

	if strings.Contains(stderr.String(), "--namespace default get configmap") {
		t.Fatalf("creation must not hint the tenant namespace: %s", stderr.String())
	}
}

func TestLoadBackupAddressesCRDFromCommandNamespace(t *testing.T) {
	object := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-1", Namespace: "tenant-a"},
	}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{sessionNamespace: "sessions"}}

	command := &cobra.Command{}
	command.Flags().String("namespace", "tenant-a", "")

	loaded, store, backend, err := state.loadBackup(
		t.Context(),
		command,
		runtime,
		object.Name,
		sourceController,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Namespace != object.Namespace || loaded.Name != object.Name {
		t.Fatalf(
			"loaded workflow = %s/%s, want %s/%s",
			loaded.Namespace,
			loaded.Name,
			object.Namespace,
			object.Name,
		)
	}

	if backend != backendCRD {
		t.Fatalf("backend = %q, want %q", backend, backendCRD)
	}

	if _, ok := store.(*kube.CRDWorkflowStore[*v1alpha1.Backup]); !ok {
		t.Fatalf("store = %T, want CRD store", store)
	}
}

func TestLoadBackupAddressesDefaultNamespaceCRD(t *testing.T) {
	object := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-default", Namespace: "default"},
	}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{sessionNamespace: "sessions"}}

	command := &cobra.Command{}
	command.Flags().String("namespace", "default", "")

	loaded, _, backend, err := state.loadBackup(
		t.Context(),
		command,
		runtime,
		object.Name,
		sourceController,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Namespace != "default" || backend != backendCRD {
		t.Fatalf(
			"loaded workflow = %s/%s from %q, want default/%s from CRD",
			loaded.Namespace,
			loaded.Name,
			backend,
			object.Name,
		)
	}
}

func TestLoadRestoreAddressesCRDFromCommandNamespace(t *testing.T) {
	object := &v1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-1", Namespace: "tenant-a"},
	}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{sessionNamespace: "sessions"}}

	command := &cobra.Command{}
	command.Flags().String("namespace", "tenant-a", "")

	loaded, store, backend, err := state.loadRestore(
		t.Context(),
		command,
		runtime,
		object.Name,
		sourceController,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Namespace != object.Namespace || loaded.Name != object.Name {
		t.Fatalf(
			"loaded workflow = %s/%s, want %s/%s",
			loaded.Namespace,
			loaded.Name,
			object.Namespace,
			object.Name,
		)
	}

	if backend != backendCRD {
		t.Fatalf("backend = %q, want %q", backend, backendCRD)
	}

	if _, ok := store.(*kube.CRDWorkflowStore[*v1alpha1.Restore]); !ok {
		t.Fatalf("store = %T, want CRD store", store)
	}
}

func TestLoadMoveUsesClusterScopedCRDKey(t *testing.T) {
	object := &v1alpha1.Move{ObjectMeta: metav1.ObjectMeta{Name: "move-1"}}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{sessionNamespace: "sessions"}}

	loaded, store, backend, err := state.loadMove(
		t.Context(),
		&cobra.Command{},
		runtime,
		object.Name,
		sourceClusterController,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Name != object.Name || loaded.Namespace != "" {
		t.Fatalf(
			"loaded workflow = %q/%q, want cluster-scoped object",
			loaded.Namespace,
			loaded.Name,
		)
	}

	if backend != backendCRD {
		t.Fatalf("backend = %q, want %q", backend, backendCRD)
	}

	if _, ok := store.(*kube.CRDWorkflowStore[*v1alpha1.Move]); !ok {
		t.Fatalf("store = %T, want CRD store", store)
	}
}

func TestLoadWorkflowWithoutRuntimeClientReturnsNotFound(t *testing.T) {
	runtime := &commandRuntime{clients: &kube.Clients{
		Kubernetes: kubefake.NewClientset(),
	}}
	state := &rootState{global: globals{sessionNamespace: "sessions"}}

	_, _, err := state.loadWorkflowWithBackend(
		t.Context(),
		&cobra.Command{},
		runtime,
		"sessions",
		"missing",
		map[domain.ControllerKind]crclient.Object{
			domain.ControllerKindBackup: &v1alpha1.Backup{},
		},
		sourceSession,
	)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("load error = %v, want not found", err)
	}
}

func TestSessionRuntimeDoesNotProbeCRDsWhenDiscoveryFindsNone(t *testing.T) {
	runtime := &commandRuntime{
		clients:                     &kube.Clients{Runtime: crfake.NewClientBuilder().Build()},
		controllerDiscoveryComplete: true,
	}

	if crdListable(runtime) {
		t.Fatal("runtime with no discovered workflow CRDs must not probe CRD storage")
	}
}

func TestControllerWorkflowRequiresDiscoveredCRD(t *testing.T) {
	runtime := &commandRuntime{controllerDiscoveryComplete: true}

	if controllerWorkflowAvailable(runtime, domain.SessionTypeBackup) {
		t.Fatal("controller workflow reported available without a discovered CRD")
	}

	if err := requireControllerWorkflow(runtime, domain.SessionTypeBackup); err == nil {
		t.Fatal("controller workflow requirement unexpectedly passed without a CRD")
	}
}

func TestWorkflowBackendHelpersPairLockerAndNamespace(t *testing.T) {
	client := kubefake.NewClientset()
	runtime := &commandRuntime{clients: &kube.Clients{Kubernetes: client}}
	object := &v1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a"}}

	if got := workflowLeaseNamespace(backendCRD, "sessions", object); got != "tenant-a" {
		t.Fatalf("CRD lease namespace = %q, want tenant-a", got)
	}

	if got := workflowLeaseNamespace(backendConfigMap, "sessions", object); got != "sessions" {
		t.Fatalf("ConfigMap lease namespace = %q, want sessions", got)
	}

	if _, ok := cliWorkflowLockerForBackend(runtime, backendCRD).(*kube.CRDWorkflowLocker); !ok {
		t.Fatal("CRD backend did not select CRD workflow locker")
	}

	if _, ok := cliWorkflowLockerForBackend(runtime, backendConfigMap).(*kube.ConfigMapWorkflowLocker); !ok {
		t.Fatal("ConfigMap backend did not select ConfigMap workflow locker")
	}
}

func TestSaveCLIPlannedWorkflowFreezesExecutionIntent(t *testing.T) {
	client := kubefake.NewClientset()

	store, err := kube.NewConfigMapWorkflowStore(
		client,
		"sessions",
		func() *v1alpha1.Backup { return &v1alpha1.Backup{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	object := &v1alpha1.Backup{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.GroupVersion.String(), Kind: "Backup"},
		ObjectMeta: metav1.ObjectMeta{Name: "backup-intent", Namespace: "tenant"},
		Spec: v1alpha1.BackupSpec{
			SourcePVC: v1alpha1.LocalResourceReference{Name: "source"},
			Name:      "point",
		},
		Status: v1alpha1.BackupStatus{
			WorkflowStatus: v1alpha1.WorkflowStatus{Phase: domain.PhasePlanned},
		},
	}

	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().ConfigMaps("sessions").Create(
		t.Context(),
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            kube.SessionConfigMapName(object.Name),
				Namespace:       "sessions",
				UID:             "backup-uid",
				ResourceVersion: "1",
				Labels: map[string]string{
					kube.ManagedByLabel:    kube.ManagedByValue,
					kube.SessionKey:        object.Name,
					kube.WorkflowKindLabel: "Backup",
				},
			},
			Data: map[string]string{kube.SessionDataKey: string(data)},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	object.UID = "backup-uid"
	object.ResourceVersion = "1"

	if err := saveCLIPlannedWorkflow(
		t.Context(),
		store,
		object,
		&object.Status.WorkflowStatus,
	); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load(t.Context(), crclient.ObjectKeyFromObject(object))
	if err != nil {
		t.Fatal(err)
	}

	want, err := kube.WorkflowExecutionIntentHash(loaded)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Status.ExecutionIntentHash != want || loaded.Status.ExecutionIntentHash == "" {
		t.Fatalf("execution intent hash = %q, want %q", loaded.Status.ExecutionIntentHash, want)
	}
}

func TestPodMigrationStatusListsSessionObjects(t *testing.T) {
	clients := newWorkflowLookupRuntime(t)
	object := &v1alpha1.PodMigration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "PodMigration",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "pod-migration-1", Namespace: "app"},
	}

	sessionStore, err := kube.NewConfigMapWorkflowStore(
		clients.clients.Kubernetes,
		"sessions",
		func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := sessionStore.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	namespacedStore, err := kube.NewCRDWorkflowStore(
		clients.clients.Runtime,
		func() *v1alpha1.PodMigration { return &v1alpha1.PodMigration{} },
	)
	if err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer

	command := NewRoot(Options{
		Out: &stdout, ErrOut: io.Discard,
		runtimeFactory: func(state *rootState) (*commandRuntime, error) {
			return &commandRuntime{
				clients:                  clients.clients,
				printer:                  printerFor(state),
				podMigrationSessionStore: sessionStore,
				podMigrationStore:        namespacedStore,
			}, nil
		},
	})
	command.SetArgs([]string{"--output", "json", "migrate-pod", "status"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	var listed []v1alpha1.PodMigration
	if err := json.Unmarshal(stdout.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}

	if len(listed) != 1 || listed[0].Name != object.Name {
		t.Fatalf("status list = %#v, want one session %q", listed, object.Name)
	}
}

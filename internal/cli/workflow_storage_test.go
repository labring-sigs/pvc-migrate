package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
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

func TestLoadBackupProbesWorkflowNamespaceForCRD(t *testing.T) {
	object := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-1", Namespace: "tenant-a"},
	}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{
		sessionNamespace:  "sessions",
		workflowNamespace: "tenant-a",
	}}

	loaded, store, backend, err := state.loadBackup(
		t.Context(),
		&cobra.Command{},
		runtime,
		object.Name,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Namespace != object.Namespace || loaded.Name != object.Name {
		t.Fatalf("loaded workflow = %s/%s, want %s/%s", loaded.Namespace, loaded.Name, object.Namespace, object.Name)
	}
	if backend != backendCRD {
		t.Fatalf("backend = %q, want %q", backend, backendCRD)
	}
	if _, ok := store.(*kube.CRDWorkflowStore[*v1alpha1.Backup]); !ok {
		t.Fatalf("store = %T, want CRD store", store)
	}
}

func TestLoadBackupProbesDefaultWorkflowNamespaceForCRD(t *testing.T) {
	object := &v1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-default", Namespace: "default"},
	}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{
		sessionNamespace:  "sessions",
		workflowNamespace: "default",
	}}

	loaded, _, backend, err := state.loadBackup(
		t.Context(),
		&cobra.Command{},
		runtime,
		object.Name,
	)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Namespace != "default" || backend != backendCRD {
		t.Fatalf("loaded workflow = %s/%s from %q, want default/%s from CRD", loaded.Namespace, loaded.Name, backend, object.Name)
	}
}

func TestCRDProbeNamespacesDeduplicatesAndKeepsDefault(t *testing.T) {
	command := &cobra.Command{}
	command.Flags().String("namespace", "default", "")
	command.Flags().String("source-namespace", "default", "")
	state := &rootState{global: globals{workflowNamespace: "default"}}

	got := state.crdProbeNamespaces(command)
	if len(got) != 1 || got[0] != "default" {
		t.Fatalf("probe namespaces = %#v, want [default]", got)
	}
}

func TestLoadRestoreProbesWorkflowNamespaceForCRD(t *testing.T) {
	object := &v1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-1", Namespace: "tenant-a"},
	}
	runtime := newWorkflowLookupRuntime(t, object)
	state := &rootState{global: globals{
		sessionNamespace:  "sessions",
		workflowNamespace: "tenant-a",
	}}

	loaded, store, backend, err := state.loadRestore(
		t.Context(),
		&cobra.Command{},
		runtime,
		object.Name,
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Namespace != object.Namespace || loaded.Name != object.Name {
		t.Fatalf("loaded workflow = %s/%s, want %s/%s", loaded.Namespace, loaded.Name, object.Namespace, object.Name)
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
	)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Name != object.Name || loaded.Namespace != "" {
		t.Fatalf("loaded workflow = %q/%q, want cluster-scoped object", loaded.Namespace, loaded.Name)
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

func TestPodMigrationStatusListsClusterScopedSessionObjects(t *testing.T) {
	clients := newWorkflowLookupRuntime(t)
	object := &v1alpha1.ClusterPodMigration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "ClusterPodMigration",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "pod-migration-1"},
	}

	sessionStore, err := kube.NewConfigMapWorkflowStore(
		clients.clients.Kubernetes,
		"sessions",
		func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := sessionStore.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}

	clusterStore, err := kube.NewCRDWorkflowStore(
		clients.clients.Runtime,
		func() *v1alpha1.ClusterPodMigration { return &v1alpha1.ClusterPodMigration{} },
	)
	if err != nil {
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
				clients:                         clients.clients,
				printer:                         printerFor(state),
				clusterPodMigrationSessionStore: sessionStore,
				clusterPodMigrationStore:        clusterStore,
				podMigrationStore:               namespacedStore,
			}, nil
		},
	})
	command.SetArgs([]string{"--output", "json", "migrate-pod", "status"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	var listed []v1alpha1.ClusterPodMigration
	if err := json.Unmarshal(stdout.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Name != object.Name {
		t.Fatalf("status list = %#v, want one cluster session %q", listed, object.Name)
	}
}

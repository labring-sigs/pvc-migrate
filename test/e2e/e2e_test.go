//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/retry"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	sessionLabel       = kube.MetadataDomain + "/session"
	roleLabel          = kube.MetadataDomain + "/role"
	sessionKey         = "session.json"
	workflowGroup      = "migrate.sealos.io"
	workflowVersion    = "v1alpha1"
	controllerLease    = "pvc-migrate-controller"
	controllerPoll     = time.Second
	controllerStartup  = 2 * time.Minute
	podSecurityVersion = "v1.25"
)

func TestBackupRepositoryAdmission(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}

	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}

	clients, err := kube.NewClients(kubeconfig, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-repo-" + suffix
	defer func() {
		propagation := metav1.DeletePropagationBackground
		if err := clients.Kubernetes.CoreV1().Namespaces().Delete(
			context.Background(),
			namespace,
			metav1.DeleteOptions{PropagationPolicy: &propagation},
		); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete repository admission namespace: %v", err)
		}
	}()

	if _, err := clients.Kubernetes.CoreV1().Namespaces().Create(
		ctx,
		e2eNamespace(namespace, nil),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	valid := []*v1alpha1.BackupRepository{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: namespace},
			Spec: v1alpha1.BackupRepositorySpec{
				Type: v1alpha1.BackupRepositoryTypeS3,
				S3: &v1alpha1.S3BackupRepositorySpec{
					Bucket: "backups",
					CredentialsSecret: v1alpha1.BackupRepositorySecretReference{
						Name: "credentials",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: namespace},
			Spec: v1alpha1.BackupRepositorySpec{
				Type: v1alpha1.BackupRepositoryTypePVC,
				PVC: &v1alpha1.PVCBackupRepositorySpec{
					ClaimRef: v1alpha1.LocalObjectReference{Name: "archive"},
					SubPath:  "snapshots",
				},
			},
		},
	}
	for _, repository := range valid {
		if err := clients.Runtime.Create(ctx, repository); err != nil {
			t.Fatalf("create valid %s repository: %v", repository.Spec.Type, err)
		}
	}

	invalid := []*v1alpha1.BackupRepository{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "mismatched", Namespace: namespace},
			Spec: v1alpha1.BackupRepositorySpec{
				Type: v1alpha1.BackupRepositoryTypeS3,
				PVC: &v1alpha1.PVCBackupRepositorySpec{
					ClaimRef: v1alpha1.LocalObjectReference{Name: "archive"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "insecure", Namespace: namespace},
			Spec: v1alpha1.BackupRepositorySpec{
				Type: v1alpha1.BackupRepositoryTypeS3,
				S3: &v1alpha1.S3BackupRepositorySpec{
					Bucket:   "backups",
					Endpoint: "http://object-store.example.test",
					CredentialsSecret: v1alpha1.BackupRepositorySecretReference{
						Name: "credentials",
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "path-traversal", Namespace: namespace},
			Spec: v1alpha1.BackupRepositorySpec{
				Type: v1alpha1.BackupRepositoryTypePVC,
				PVC: &v1alpha1.PVCBackupRepositorySpec{
					ClaimRef: v1alpha1.LocalObjectReference{Name: "archive"},
					SubPath:  "snapshots/../private",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "path-empty-segment", Namespace: namespace},
			Spec: v1alpha1.BackupRepositorySpec{
				Type: v1alpha1.BackupRepositoryTypePVC,
				PVC: &v1alpha1.PVCBackupRepositorySpec{
					ClaimRef: v1alpha1.LocalObjectReference{Name: "archive"},
					SubPath:  "snapshots//daily",
				},
			},
		},
	}
	for _, repository := range invalid {
		if err := clients.Runtime.Create(ctx, repository); !apierrors.IsInvalid(err) {
			t.Fatalf("invalid repository %s admission error=%v", repository.Name, err)
		}
	}
}

func TestControllerS3TransportFailureIsActionableAndRedacted(t *testing.T) {
	config, adminClient, kubeconfig := capacityE2EClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := e2eSuffix()
	namespace := "pvc-migrate-s3-error-" + suffix
	sessionID := "s3-error-" + suffix
	defer cleanupTestResources(t, config, adminClient, namespace, sessionID)
	createE2ENamespace(t, ctx, adminClient, namespace, sessionID)

	clients, err := kube.NewClients(kubeconfig, "")
	if err != nil {
		t.Fatal(err)
	}
	credentials := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: namespace},
		StringData: map[string]string{
			"accessKey": "e2e-access",
			"secretKey": "e2e-secret",
		},
	}
	if _, err := adminClient.CoreV1().Secrets(namespace).Create(
		ctx,
		credentials,
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	const endpointHost = "s3-controller-probe.invalid"
	repository := &v1alpha1.BackupRepository{
		ObjectMeta: metav1.ObjectMeta{Name: "unreachable", Namespace: namespace},
		Spec: v1alpha1.BackupRepositorySpec{
			Type: v1alpha1.BackupRepositoryTypeS3,
			S3: &v1alpha1.S3BackupRepositorySpec{
				Bucket:         "e2e-backups",
				Prefix:         "failure-tests",
				Endpoint:       "https://" + endpointHost,
				Region:         "us-east-1",
				ForcePathStyle: true,
				CredentialsSecret: v1alpha1.BackupRepositorySecretReference{
					Name: credentials.Name,
				},
			},
		},
	}
	if err := clients.Runtime.Create(ctx, repository); err != nil {
		t.Fatal(err)
	}

	binary := e2eBinary(t, ctx)
	controllerProcess := startE2EController(
		t,
		ctx,
		adminClient,
		binary,
		kubeconfig,
		namespace,
		"controller",
	)
	defer controllerProcess.Stop(t)

	storageClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	cliArgs := appendE2EToolImage([]string{
		"--kubeconfig", kubeconfig,
		"--mode", "controller",
		"--session-namespace", namespace,
		"--controller-namespace", namespace,
		"--timeout", "2m",
		"--output", "json",
		"--log-format", "json",
		"--yes",
	})
	cliArgs = append(
		cliArgs,
		"restore",
		"--id", sessionID,
		"--namespace", namespace,
		"--destination-pvc", "restore-data",
		"--create-pvc",
		"--destination-storage-class", storageClass,
		"--destination-access-mode", string(corev1.ReadWriteOnce),
		"--destination-capacity", "32Mi",
		"--backup-repository", repository.Name,
		"--name", "missing-recovery-point",
		"--dry-run=false",
	)
	output := runCLIExpectExitCode(
		t,
		ctx,
		binary,
		1,
		cliArgs...,
	)

	const expectedMessage = "S3 endpoint DNS resolution failed; verify the endpoint hostname and cluster DNS"
	outputText := string(output)
	if !strings.Contains(outputText, expectedMessage) {
		t.Fatalf("controller CLI output lacks actionable DNS guidance:\n%s", outputText)
	}
	for _, secret := range []string{endpointHost, "e2e-access", "e2e-secret"} {
		if strings.Contains(outputText, secret) {
			t.Fatalf("controller CLI output exposed repository detail %q", secret)
		}
	}

	workflow := &v1alpha1.Restore{}
	if err := clients.Runtime.Get(
		ctx,
		types.NamespacedName{Namespace: namespace, Name: sessionID},
		workflow,
	); err != nil {
		t.Fatal(err)
	}
	if workflow.Status.Phase != v1alpha1.WorkflowPhase(domain.PhaseFailed) ||
		workflow.Status.ObservedGeneration != workflow.Generation ||
		!strings.Contains(workflow.Status.Message, expectedMessage) {
		t.Fatalf("restore failure status=%#v", workflow.Status)
	}
	if workflow.Status.Repository == nil || workflow.Status.Repository.S3 == nil ||
		workflow.Status.Repository.UID == "" ||
		workflow.Status.Repository.S3.CredentialsSecretUID == "" {
		t.Fatalf("restore repository checkpoint=%#v", workflow.Status.Repository)
	}
	statusJSON, err := json.Marshal(workflow.Status)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{endpointHost, "e2e-access", "e2e-secret"} {
		if bytes.Contains(statusJSON, []byte(secret)) {
			t.Fatalf("Restore status exposed repository detail %q", secret)
		}
	}

	if _, err := adminClient.CoreV1().PersistentVolumeClaims(namespace).Get(
		ctx,
		"restore-data",
		metav1.GetOptions{},
	); !apierrors.IsNotFound(err) {
		t.Fatalf("destination PVC was created before manifest validation: %v", err)
	}
}

func TestControllerRBACEnsuresOnlyManagedTransferServiceAccount(t *testing.T) {
	config, adminClient, _ := capacityE2EClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := e2eSuffix()
	namespace := "pvc-migrate-identity-" + suffix
	sessionID := "identity-" + suffix
	defer cleanupTestResources(t, config, adminClient, namespace, sessionID)
	createE2ENamespace(t, ctx, adminClient, namespace, sessionID)

	controllerConfig := rest.CopyConfig(config)
	controllerConfig.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:pvc-migrate-system:pvc-migrate",
	}
	controllerClient, err := kubernetes.NewForConfig(controllerConfig)
	if err != nil {
		t.Fatal(err)
	}

	if err := kube.EnsureTransferServiceAccount(ctx, controllerClient, namespace); err != nil {
		t.Fatalf("controller creates transfer ServiceAccount: %v", err)
	}

	account, err := adminClient.CoreV1().
		ServiceAccounts(namespace).
		Get(ctx, kube.TransferServiceAccountName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if account.AutomountServiceAccountToken == nil || *account.AutomountServiceAccountToken {
		t.Fatalf("created ServiceAccount automount=%v", account.AutomountServiceAccountToken)
	}

	broken := account.DeepCopy()
	broken.AutomountServiceAccountToken = new(true)
	if _, err := adminClient.CoreV1().
		ServiceAccounts(namespace).
		Update(ctx, broken, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := kube.EnsureTransferServiceAccount(ctx, controllerClient, namespace); err != nil {
		t.Fatalf("controller repairs transfer ServiceAccount: %v", err)
	}

	repaired, err := adminClient.CoreV1().
		ServiceAccounts(namespace).
		Get(ctx, kube.TransferServiceAccountName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if repaired.AutomountServiceAccountToken == nil || *repaired.AutomountServiceAccountToken {
		t.Fatalf("repaired ServiceAccount automount=%v", repaired.AutomountServiceAccountToken)
	}

	if err := adminClient.CoreV1().ServiceAccounts(namespace).Delete(
		ctx,
		kube.TransferServiceAccountName,
		metav1.DeleteOptions{},
	); err != nil {
		t.Fatal(err)
	}
	unownedAutomount := true
	if _, err := adminClient.CoreV1().ServiceAccounts(namespace).Create(
		ctx,
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      kube.TransferServiceAccountName,
				Namespace: namespace,
			},
			AutomountServiceAccountToken: &unownedAutomount,
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	if err := kube.EnsureTransferServiceAccount(
		ctx,
		controllerClient,
		namespace,
	); domain.CategoryOf(err) != domain.ErrorConflict {
		t.Fatalf("unowned ServiceAccount category=%q error=%v", domain.CategoryOf(err), err)
	}
	if err := controllerClient.CoreV1().ServiceAccounts(namespace).Delete(
		ctx,
		kube.TransferServiceAccountName,
		metav1.DeleteOptions{},
	); !apierrors.IsForbidden(err) {
		t.Fatalf("controller ServiceAccount delete error=%v, want forbidden", err)
	}

	unowned, err := adminClient.CoreV1().
		ServiceAccounts(namespace).
		Get(ctx, kube.TransferServiceAccountName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if unowned.AutomountServiceAccountToken == nil || !*unowned.AutomountServiceAccountToken ||
		len(unowned.Labels) != 0 {
		t.Fatalf("unowned ServiceAccount was modified: %#v", unowned)
	}
}

func TestWorkflowScopeAdmissionAndStatusMatrix(t *testing.T) {
	config, client, _ := capacityE2EClients(t)
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := e2eSuffix()
	namespace := "pvc-migrate-scope-" + suffix
	sessionID := "scope-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)
	createE2ENamespace(t, ctx, client, namespace, sessionID)

	type workflowCase struct {
		name     string
		resource string
		kind     string
		cluster  bool
		volumes  bool
		warm     bool
		spec     map[string]any
	}
	localIdentity := func() map[string]any {
		return map[string]any{
			"sourcePVC":      map[string]any{"name": "source"},
			"sourcePV":       map[string]any{"name": "source-pv"},
			"destinationPVC": map[string]any{"name": "destination"},
			"sourceTemplate": map[string]any{"spec": map[string]any{}},
		}
	}
	clusterNamespaces := func() map[string]any {
		return map[string]any{
			"sourceNamespace":      namespace,
			"temporaryNamespace":   namespace,
			"destinationNamespace": namespace + "-destination",
			"sessionNamespace":     namespace,
		}
	}
	clusterPodNamespaces := func() map[string]any {
		return map[string]any{
			"sourceNamespace":      namespace,
			"temporaryNamespace":   namespace,
			"destinationNamespace": "must-be-pruned",
			"sessionNamespace":     namespace,
		}
	}
	clusterDestinationNamespaces := func() map[string]any {
		return map[string]any{
			"sourceNamespace":      namespace,
			"temporaryNamespace":   "must-be-pruned",
			"destinationNamespace": namespace + "-destination",
			"sessionNamespace":     namespace,
		}
	}
	merge := func(base map[string]any, values map[string]any) map[string]any {
		for key, value := range values {
			base[key] = value
		}

		return base
	}

	tests := []workflowCase{
		{resource: "migrations", kind: "Migration", volumes: true, spec: map[string]any{
			"sourceNamespace": "must-be-pruned",
		}},
		{
			resource: "podmigrations", kind: "PodMigration", volumes: true, warm: true,
			spec: map[string]any{
				"precopyPasses": int64(0),
				"workload":      map[string]any{"adapter": "StandalonePod"},
			},
		},
		{resource: "reservations", kind: "Reservation", volumes: true, spec: map[string]any{}},
		{resource: "copies", kind: "Copy", volumes: true, spec: map[string]any{}},
		{
			resource: "backups", kind: "Backup", spec: map[string]any{
				"sourcePVC":     map[string]any{"name": "source"},
				"sourcePV":      map[string]any{"name": "source-pv"},
				"name":          "archive",
				"repositoryRef": map[string]any{"name": "repository"},
			},
		},
		{
			resource: "restores", kind: "Restore", spec: map[string]any{
				"destinationPVC": map[string]any{"name": "destination"},
				"name":           "archive",
				"repositoryRef":  map[string]any{"name": "repository"},
			},
		},
		{resource: "renames", kind: "Rename", volumes: true, spec: localIdentity()},
		{
			resource: "clustermigrations", kind: "ClusterMigration", cluster: true, volumes: true,
			spec: clusterNamespaces(),
		},
		{
			resource: "clusterpodmigrations", kind: "ClusterPodMigration", cluster: true,
			volumes: true, warm: true,
			spec: merge(clusterPodNamespaces(), map[string]any{
				"precopyPasses": int64(0),
				"workload":      map[string]any{"adapter": "StandalonePod"},
			}),
		},
		{
			resource: "clusterreservations", kind: "ClusterReservation", cluster: true,
			volumes: true, spec: clusterDestinationNamespaces(),
		},
		{
			resource: "clustercopies", kind: "ClusterCopy", cluster: true,
			volumes: true, spec: clusterDestinationNamespaces(),
		},
		{
			name: "same namespace", resource: "moves", kind: "Move", cluster: true, volumes: true,
			spec: map[string]any{
				"sourceNamespace":      namespace,
				"destinationNamespace": namespace,
				"sessionNamespace":     namespace,
				"identity":             localIdentity(),
			},
		},
		{
			name: "cross namespace", resource: "moves", kind: "Move", cluster: true, volumes: true,
			spec: map[string]any{
				"sourceNamespace":      namespace,
				"destinationNamespace": namespace + "-destination",
				"sessionNamespace":     namespace,
				"identity":             localIdentity(),
			},
		},
	}

	for _, test := range tests {
		testName := test.kind
		objectName := sessionID
		if test.name != "" {
			testName += " " + test.name
			objectName += "-" + strings.ReplaceAll(test.name, " ", "-")
		}

		t.Run(testName, func(t *testing.T) {
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": workflowGroup + "/" + workflowVersion,
				"kind":       test.kind,
				"metadata": map[string]any{
					"name":      objectName,
					"namespace": namespace,
					"labels":    map[string]any{sessionLabel: sessionID},
				},
				"spec": test.spec,
			}}
			var resources dynamic.ResourceInterface
			if test.cluster {
				resources = dynamicClient.Resource(workflowGVR(test.resource))
				object.SetNamespace("")
			} else {
				resources = dynamicClient.Resource(workflowGVR(test.resource)).Namespace(namespace)
			}

			created, err := resources.Create(ctx, object, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if test.cluster == (created.GetNamespace() != "") {
				t.Fatalf("resource namespace=%q cluster=%t", created.GetNamespace(), test.cluster)
			}
			if test.kind == "Migration" {
				if _, found, nestedErr := unstructured.NestedFieldNoCopy(
					created.Object,
					"spec",
					"sourceNamespace",
				); nestedErr != nil || found {
					t.Fatalf("namespaced namespace selector was not pruned: found=%t err=%v", found, nestedErr)
				}
			}
			if test.kind == "ClusterPodMigration" {
				if _, found, nestedErr := unstructured.NestedFieldNoCopy(
					created.Object,
					"spec",
					"destinationNamespace",
				); nestedErr != nil || found {
					t.Fatalf(
						"derived ClusterPodMigration destination namespace was not pruned: found=%t err=%v",
						found,
						nestedErr,
					)
				}
			}
			if test.kind == "ClusterReservation" || test.kind == "ClusterCopy" {
				if _, found, nestedErr := unstructured.NestedFieldNoCopy(
					created.Object,
					"spec",
					"temporaryNamespace",
				); nestedErr != nil || found {
					t.Fatalf(
						"unused %s temporary namespace was not pruned: found=%t err=%v",
						test.kind,
						found,
						nestedErr,
					)
				}
			}

			now := time.Now().UTC().Format(time.RFC3339)
			status := map[string]any{
				"phase":     "Planned",
				"startedAt": now,
				"updatedAt": now,
			}
			if test.volumes {
				status["volumes"] = []any{}
			}
			if test.warm {
				status["warmPassesCompleted"] = int64(0)
			}
			created.Object["status"] = status
			updated, err := resources.UpdateStatus(ctx, created, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			phase, found, err := unstructured.NestedString(updated.Object, "status", "phase")
			if err != nil || !found || phase != "Planned" {
				t.Fatalf("status phase=%q found=%t err=%v", phase, found, err)
			}
		})
	}

	invalidCluster := []struct {
		name     string
		resource string
		kind     string
		spec     map[string]any
	}{
		{
			name: "missing namespace", resource: "clustermigrations", kind: "ClusterMigration",
			spec: map[string]any{
				"sourceNamespace": namespace, "temporaryNamespace": namespace,
				"sessionNamespace": namespace,
			},
		},
		{
			name: "invalid namespace", resource: "clustercopies", kind: "ClusterCopy",
			spec: merge(clusterNamespaces(), map[string]any{"sourceNamespace": "Invalid_Namespace"}),
		},
	}
	for index, test := range invalidCluster {
		t.Run(test.name, func(t *testing.T) {
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": workflowGroup + "/" + workflowVersion,
				"kind":       test.kind,
				"metadata":   map[string]any{"name": fmt.Sprintf("invalid-%d-%s", index, suffix)},
				"spec":       test.spec,
			}}
			_, err := dynamicClient.Resource(workflowGVR(test.resource)).
				Create(ctx, object, metav1.CreateOptions{})
			if !apierrors.IsInvalid(err) {
				t.Fatalf("invalid cluster workflow error=%v", err)
			}
		})
	}

	for _, resource := range []string{"clusterbackups", "clusterrestores", "clusterrenames"} {
		resources := dynamicClient.Resource(workflowGVR(resource))
		_, err = resources.List(ctx, metav1.ListOptions{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("removed resource %s is still served: %v", resource, err)
		}
	}

	_, err = dynamicClient.Resource(workflowGVR("moves")).Namespace(namespace).
		List(ctx, metav1.ListOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("namespaced Move endpoint is still served: %v", err)
	}
}

func TestControllerWorkflowCollisionAndNamespaceDeletion(t *testing.T) {
	config, adminClient, kubeconfig := capacityE2EClients(t)
	clients, err := kube.NewClients(kubeconfig, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	suffix := e2eSuffix()
	controlNamespace := "pvc-migrate-safety-control-" + suffix
	workflowNamespace := "pvc-migrate-safety-workflow-" + suffix
	sessionID := "safety-" + suffix
	defer cleanupTestResources(t, config, adminClient, controlNamespace, sessionID)
	defer cleanupTestResources(t, config, adminClient, workflowNamespace, sessionID)
	createE2ENamespace(t, ctx, adminClient, controlNamespace, sessionID)
	createE2ENamespace(t, ctx, adminClient, workflowNamespace, sessionID)

	volume := domain.VolumeSpec{
		SourcePVC: domain.ObjectReference{
			APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: workflowNamespace,
			Name: "source", UID: types.UID("source-pvc-uid"),
		},
		SourcePV: domain.ObjectReference{
			APIVersion: "v1", Kind: "PersistentVolume",
			Name: "source-pv", UID: types.UID("source-pv-uid"),
		},
		SourceReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		SourcePVCSpec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
		DestinationPVC: domain.ObjectReference{
			APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: workflowNamespace,
			Name: "destination",
		},
		Capacity:    "1Gi",
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		VolumeMode:  corev1.PersistentVolumeFilesystem,
	}
	common := domain.SessionCommon{
		SourceNamespace:      workflowNamespace,
		TemporaryNamespace:   workflowNamespace,
		DestinationNamespace: workflowNamespace,
		SessionNamespace:     workflowNamespace,
		Volumes:              []domain.VolumeSpec{volume},
	}
	renameSpec := domain.NewSessionSpec(
		domain.OperationRename,
		common,
		false,
		domain.SessionWorkflowOptions{},
	)
	copySpec := domain.NewSessionSpec(
		domain.OperationCopy,
		common,
		false,
		domain.SessionWorkflowOptions{},
	)
	rename := &v1alpha1.Rename{
		ObjectMeta: metav1.ObjectMeta{Name: sessionID, Namespace: workflowNamespace},
		Spec:       v1alpha1.RenameSpecFromDomain(renameSpec),
	}
	copyWorkflow := &v1alpha1.Copy{
		ObjectMeta: metav1.ObjectMeta{Name: sessionID, Namespace: workflowNamespace},
		Spec:       v1alpha1.CopySpecFromDomain(copySpec),
	}
	if err := clients.Runtime.Create(ctx, rename); err != nil {
		t.Fatal(err)
	}
	if err := clients.Runtime.Create(ctx, copyWorkflow); err != nil {
		t.Fatal(err)
	}

	buildCtx, buildCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer buildCancel()
	binary := e2eBinary(t, buildCtx)
	controllerProcess := startE2EController(
		t,
		ctx,
		adminClient,
		binary,
		kubeconfig,
		controlNamespace,
		"controller",
	)

	stableUntil := time.Now().Add(6 * time.Second)
	for time.Now().Before(stableUntil) {
		for _, object := range []crclient.Object{rename, copyWorkflow} {
			if err := clients.Runtime.Get(
				ctx,
				crclient.ObjectKeyFromObject(object),
				object,
			); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(object.GetFinalizers(), kube.SessionFinalizer) {
				t.Fatalf("colliding %T unexpectedly received session protection", object)
			}
		}
		if rename.Status.Phase != "" || copyWorkflow.Status.Phase != "" {
			t.Fatalf(
				"colliding workflows advanced: rename=%q copy=%q",
				rename.Status.Phase,
				copyWorkflow.Status.Phase,
			)
		}
		time.Sleep(250 * time.Millisecond)
	}
	controllerProcess.Stop(t)

	if err := clients.Runtime.Delete(ctx, copyWorkflow); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		30*time.Second,
		true,
		func(waitCtx context.Context) (bool, error) {
			current := &v1alpha1.Copy{}
			err := clients.Runtime.Get(
				waitCtx,
				crclient.ObjectKey{Namespace: workflowNamespace, Name: sessionID},
				current,
			)
			return apierrors.IsNotFound(err), nil
		},
	); err != nil {
		t.Fatal(err)
	}

	if err := clients.Runtime.Get(
		ctx,
		crclient.ObjectKey{Namespace: workflowNamespace, Name: sessionID},
		rename,
	); err != nil {
		t.Fatal(err)
	}
	rename.Finalizers = []string{kube.SessionFinalizer}
	if err := clients.Runtime.Update(ctx, rename); err != nil {
		t.Fatal(err)
	}
	completed := domain.NewSession(sessionID, renameSpec, time.Now())
	completed.Status.Phase = domain.PhaseCompleted
	completed.Status.ObservedGeneration = rename.Generation
	completed.Status.Message = "completed E2E fixture"
	completedAt := metav1.Now()
	completed.Status.UpdatedAt = completedAt
	completed.Status.CompletedAt = &completedAt
	rename.Status = v1alpha1.RenameStatusFromDomain(completed.Status)
	if err := clients.Runtime.Status().Update(ctx, rename); err != nil {
		t.Fatal(err)
	}

	controllerProcess = startE2EController(
		t,
		ctx,
		adminClient,
		binary,
		kubeconfig,
		controlNamespace,
		"controller",
	)
	defer controllerProcess.Stop(t)
	time.Sleep(2 * controllerPoll)

	propagation := metav1.DeletePropagationBackground
	if err := adminClient.CoreV1().Namespaces().Delete(
		ctx,
		workflowNamespace,
		metav1.DeleteOptions{PropagationPolicy: &propagation},
	); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(
		ctx,
		controllerPoll,
		2*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			_, err := adminClient.CoreV1().Namespaces().Get(
				waitCtx,
				workflowNamespace,
				metav1.GetOptions{},
			)
			return apierrors.IsNotFound(err), nil
		},
	); err != nil {
		t.Fatalf("workflow namespace remained terminating: %v", err)
	}
}

func TestPVCIdentityScopeAndLifecycle(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}

	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}

	mode := e2eMode(t)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-e2e-identity"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	buildCtx, buildCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer buildCancel()
	binary := e2eBinary(t, buildCtx)

	tests := []struct {
		name             string
		command          string
		resource         string
		kind             string
		crossNamespace   bool
		destinationClaim string
	}{
		{
			name:             "namespaced rename",
			command:          "rename",
			resource:         domain.RenameResource,
			kind:             string(domain.ControllerKindRename),
			destinationClaim: "renamed",
		},
		{
			name:             "same namespace Move",
			command:          "move",
			resource:         domain.MoveResource,
			kind:             string(domain.ControllerKindMove),
			destinationClaim: "moved-local",
		},
		{
			name:             "cross namespace Move",
			command:          "move",
			resource:         domain.MoveResource,
			kind:             string(domain.ControllerKindMove),
			crossNamespace:   true,
			destinationClaim: "moved-remote",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			defer cancel()

			suffix := e2eSuffix()
			sourceNamespace := "pvc-migrate-identity-" + suffix
			destinationNamespace := sourceNamespace
			if test.crossNamespace {
				destinationNamespace += "-destination"
			}
			sessionID := "identity-" + suffix
			defer cleanupTestResources(t, config, client, sourceNamespace, sessionID)
			createE2ENamespace(t, ctx, client, sourceNamespace, sessionID)
			if test.crossNamespace {
				defer cleanupTestResources(t, config, client, destinationNamespace, sessionID)
				createE2ENamespace(t, ctx, client, destinationNamespace, sessionID)
			}

			sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
			claimName := "data"
			_, err := client.CoreV1().PersistentVolumeClaims(sourceNamespace).Create(
				ctx,
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: sourceNamespace},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						StorageClassName: &sourceClass,
						Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("64Mi"),
						}},
					},
				},
				metav1.CreateOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}

			marker := "identity-e2e-" + suffix
			initializerName := "initializer"
			_, err = client.CoreV1().Pods(sourceNamespace).Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: initializerName, Namespace: sourceNamespace},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyAlways,
					Containers: []corev1.Container{{
						Name:  "writer",
						Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
						Command: []string{
							"sh", "-c",
							"set -eu; printf '%s\\n' \"$1\" > /data/payload; sync; touch /data/ready; exec sleep 86400",
							"initializer", marker,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
								Command: []string{"test", "-f", "/data/ready"},
							}},
							PeriodSeconds: 1,
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: claimName,
							},
						},
					}},
				},
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			initializer := waitForReadyPod(t, ctx, client, sourceNamespace, initializerName, "")
			sourceNode := initializer.Spec.NodeName
			sourceDigest := podDigest(t, ctx, config, client, sourceNamespace, initializerName)
			sourcePVC := waitForBoundPVC(t, ctx, client, sourceNamespace, claimName)
			sourcePV := sourcePVC.Spec.VolumeName
			deletePod(t, ctx, client, sourceNamespace, initializerName)

			common := []string{
				"--kubeconfig", kubeconfig,
				"--mode", mode,
				"--session-namespace", sourceNamespace,
				"--timeout", "12m",
				"--output", "json",
			}
			controllerProcess := startE2EController(
				t, ctx, client, binary, kubeconfig, sourceNamespace, mode,
			)
			defer controllerProcess.Stop(t)

			operation := []string{
				test.command,
				"--session", sessionID,
				"--source-namespace", sourceNamespace,
				"--source-pvc", claimName,
				"--destination-pvc", test.destinationClaim,
			}
			if test.command == "move" {
				operation = append(
					operation,
					"--destination-namespace", destinationNamespace,
				)
			}

			runCLI(
				t,
				ctx,
				binary,
				append(append([]string{}, common...), append(operation, "--dry-run")...)...,
			)
			assertSessionRecordNotFound(
				t, ctx, config, client, mode, test.resource, sourceNamespace, sessionID,
			)
			runCLI(
				t,
				ctx,
				binary,
				append(
					append([]string{}, common...),
					append([]string{"--yes"}, append(operation, "--dry-run=false")...)...,
				)...,
			)
			waitForSessionPhase(
				t,
				ctx,
				config,
				client,
				mode,
				test.resource,
				sourceNamespace,
				sessionID,
				"Completed",
			)
			controllerProcess.Stop(t)

			if mode == "controller" {
				workflow := readWorkflowObject(
					t, ctx, config, test.resource, sourceNamespace, sessionID,
				)
				if workflow.GetKind() != test.kind {
					t.Fatalf("workflow kind=%q want=%q", workflow.GetKind(), test.kind)
				}
				if test.command == "move" && workflow.GetNamespace() != "" {
					t.Fatalf("Move unexpectedly has namespace %q", workflow.GetNamespace())
				}
				if test.command == "rename" && workflow.GetNamespace() != sourceNamespace {
					t.Fatalf("Rename namespace=%q want=%q", workflow.GetNamespace(), sourceNamespace)
				}
			}

			destinationPVC := waitForBoundPVC(
				t, ctx, client, destinationNamespace, test.destinationClaim,
			)
			if destinationPVC.Spec.VolumeName != sourcePV {
				t.Fatalf(
					"destination PVC volume=%q want retained source PV %q",
					destinationPVC.Spec.VolumeName,
					sourcePV,
				)
			}
			readerName := "destination-reader"
			createPVCReader(
				t,
				ctx,
				client,
				destinationNamespace,
				readerName,
				sourceNode,
				[]string{test.destinationClaim},
			)
			waitForReadyPod(t, ctx, client, destinationNamespace, readerName, sourceNode)
			if digest := readerDigest(
				t, ctx, config, client, destinationNamespace, readerName,
			); digest != sourceDigest {
				t.Fatalf("destination digest=%s want=%s", digest, sourceDigest)
			}
			deletePod(t, ctx, client, destinationNamespace, readerName)

			runCLI(
				t,
				ctx,
				binary,
				append(
					append([]string{}, common...),
					"--yes", test.command, "rollback", sessionID, "--dry-run=false",
				)...,
			)
			assertSessionPhase(
				t, ctx, config, client, mode, test.resource, sourceNamespace, sessionID, "RolledBack",
			)

			rolledBackPVC := waitForBoundPVC(t, ctx, client, sourceNamespace, claimName)
			if rolledBackPVC.Spec.VolumeName != sourcePV {
				t.Fatalf(
					"rolled-back PVC volume=%q want=%q",
					rolledBackPVC.Spec.VolumeName,
					sourcePV,
				)
			}
			readerName = "rollback-reader"
			createPVCReader(
				t, ctx, client, sourceNamespace, readerName, sourceNode, []string{claimName},
			)
			waitForReadyPod(t, ctx, client, sourceNamespace, readerName, sourceNode)
			if digest := readerDigest(
				t, ctx, config, client, sourceNamespace, readerName,
			); digest != sourceDigest {
				t.Fatalf("rollback digest=%s want=%s", digest, sourceDigest)
			}
			deletePod(t, ctx, client, sourceNamespace, readerName)

			runCLI(
				t,
				ctx,
				binary,
				append(
					append([]string{}, common...),
					"--yes", test.command, "cleanup", sessionID,
					"--finalize", "--delete-session", "--dry-run=false",
				)...,
			)
			assertSessionRecordNotFound(
				t, ctx, config, client, mode, test.resource, sourceNamespace, sessionID,
			)
		})
	}
}

func TestControllerPodMigrationStatusCheckpointRoundTrip(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}
	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := kube.NewClients(kubeconfig, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-status-" + suffix
	sessionID := "status-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)

	if _, err := client.CoreV1().Namespaces().Create(
		ctx,
		e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	spec := domain.NewPodMigrationSessionSpec(
		domain.SessionCommon{
			SourceNamespace:      namespace,
			TemporaryNamespace:   namespace,
			DestinationNamespace: namespace,
			SessionNamespace:     namespace,
			Volumes: []domain.VolumeSpec{{
				SourcePVC: domain.ObjectReference{
					Namespace: namespace, Name: "data", UID: "source-pvc-uid",
				},
				SourcePV: domain.ObjectReference{Name: "source-pv", UID: "source-pv-uid"},
				DestinationPVC: domain.ObjectReference{
					Namespace: namespace, Name: "data-migrated",
				},
				Capacity:       "1Gi",
				SourceCapacity: "1Gi",
				StorageClass:   "example",
				AccessModes:    []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeMode:     corev1.PersistentVolumeFilesystem,
			}},
		},
		domain.WorkloadSpec{
			Adapter: domain.WorkloadStandalone,
			Pod: domain.ObjectReference{
				APIVersion: "v1",
				Kind:       "Pod",
				Namespace:  namespace,
				Name:       "writer",
				UID:        "pod-uid",
			},
		},
		domain.SessionWorkflowOptions{},
		1,
		false,
	)
	session := domain.NewSession(sessionID, spec, time.Now())
	store := kube.NewCRDSessionStore(clients.Runtime)
	if err := store.Create(ctx, session); err != nil {
		t.Fatal(err)
	}

	completed := metav1.Now()
	session.Status.Phase = domain.PhaseFinalSynced
	session.Status.Volumes[0].Sync.FinalCompletedAt = &completed
	if err := store.Update(ctx, session); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Get(ctx, namespace, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status.Phase != domain.PhaseFinalSynced ||
		loaded.Status.Volumes[0].Sync.FinalCompletedAt == nil {
		t.Fatalf("checkpoint was not persisted: %#v", loaded.Status)
	}
}

func TestControllerRepositoryStatusCheckpointRoundTrip(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}

	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := kube.NewClients(kubeconfig, "")
	if err != nil {
		t.Fatal(err)
	}
	store := kube.NewCRDSessionStore(clients.Runtime)

	tests := []struct {
		name      string
		operation domain.Operation
		binding   *domain.BackupRepositoryBindingStatus
	}{
		{
			name:      "backup-s3",
			operation: domain.OperationBackup,
			binding: &domain.BackupRepositoryBindingStatus{
				Type:       domain.BackupRepositoryTypeS3,
				UID:        "repository-uid",
				Generation: 3,
				S3: &domain.S3BackupRepositoryBindingStatus{
					CredentialsSecretUID: "secret-uid",
				},
			},
		},
		{
			name:      "restore-pvc",
			operation: domain.OperationRestore,
			binding: &domain.BackupRepositoryBindingStatus{
				Type:       domain.BackupRepositoryTypePVC,
				UID:        "repository-uid",
				Generation: 5,
				PVC: &domain.PVCBackupRepositoryBindingStatus{
					ClaimUID: "claim-uid",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
			if len(suffix) > 10 {
				suffix = suffix[len(suffix)-10:]
			}
			namespace := "pvc-migrate-repo-status-" + suffix
			sessionID := tt.name + "-" + suffix
			defer cleanupTestResources(t, config, client, namespace, sessionID)

			if _, err := client.CoreV1().Namespaces().Create(
				ctx,
				e2eNamespace(namespace, nil),
				metav1.CreateOptions{},
			); err != nil {
				t.Fatal(err)
			}

			spec := domain.NewSessionSpec(
				tt.operation,
				domain.SessionCommon{
					SourceNamespace:      namespace,
					DestinationNamespace: namespace,
					SessionNamespace:     namespace,
				},
				false,
				domain.SessionWorkflowOptions{},
			)
			switch tt.operation {
			case domain.OperationBackup:
				spec.Backup.SourcePVC = domain.ObjectReference{
					Namespace: namespace,
					Name:      "source",
					UID:       "source-pvc-uid",
				}
				spec.Backup.SourcePV = domain.ObjectReference{Name: "source-pv", UID: "source-pv-uid"}
				spec.Backup.Name = "daily"
				spec.Backup.BackupRepository = "archive"
			case domain.OperationRestore:
				spec.Restore.DestinationPVC = domain.ObjectReference{
					Namespace: namespace,
					Name:      "destination",
				}
				spec.Restore.Name = "daily"
				spec.Restore.BackupRepository = "archive"
			}

			session := domain.NewSession(sessionID, spec, time.Now())
			if err := store.Create(ctx, session); err != nil {
				t.Fatal(err)
			}
			session.Status.BackupRepository = tt.binding
			if tt.operation == domain.OperationRestore {
				session.Spec.Restore.DestinationPVC = domain.ObjectReference{
					APIVersion:      domain.CoreAPIVersion,
					Kind:            domain.KindPersistentVolumeClaim,
					Namespace:       namespace,
					Name:            "destination",
					UID:             "destination-pvc-uid",
					ResourceVersion: "destination-pvc-version",
				}
				session.Spec.Restore.DestinationPV = domain.ObjectReference{
					APIVersion:      domain.CoreAPIVersion,
					Kind:            domain.KindPersistentVolume,
					Name:            "destination-pv",
					UID:             "destination-pv-uid",
					ResourceVersion: "destination-pv-version",
				}
			}
			if err := store.Update(ctx, session); err != nil {
				t.Fatal(err)
			}

			loaded, err := store.Get(ctx, namespace, sessionID)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Status.BackupRepository == nil ||
				loaded.Status.BackupRepository.Type != tt.binding.Type ||
				loaded.Status.BackupRepository.UID != tt.binding.UID ||
				loaded.Status.BackupRepository.Generation != tt.binding.Generation {
				t.Fatalf("repository checkpoint was not persisted: %#v", loaded.Status.BackupRepository)
			}

			switch tt.binding.Type {
			case domain.BackupRepositoryTypeS3:
				if loaded.Status.BackupRepository.S3 == nil ||
					loaded.Status.BackupRepository.S3.CredentialsSecretUID !=
						tt.binding.S3.CredentialsSecretUID {
					t.Fatalf("S3 checkpoint was not persisted: %#v", loaded.Status.BackupRepository)
				}
			case domain.BackupRepositoryTypePVC:
				if loaded.Status.BackupRepository.PVC == nil ||
					loaded.Status.BackupRepository.PVC.ClaimUID != tt.binding.PVC.ClaimUID {
					t.Fatalf("PVC checkpoint was not persisted: %#v", loaded.Status.BackupRepository)
				}
			}

			if tt.operation == domain.OperationRestore {
				if loaded.Spec.Restore.DestinationPVC.UID != "destination-pvc-uid" ||
					loaded.Spec.Restore.DestinationPVC.ResourceVersion != "destination-pvc-version" ||
					loaded.Spec.Restore.DestinationPV.UID != "destination-pv-uid" ||
					loaded.Spec.Restore.DestinationPV.ResourceVersion != "destination-pv-version" {
					t.Fatalf("restore destination checkpoint was not persisted: %#v", loaded.Spec.Restore)
				}

				workflow := &v1alpha1.Restore{}
				if err := clients.Runtime.Get(
					ctx,
					types.NamespacedName{Namespace: namespace, Name: sessionID},
					workflow,
				); err != nil {
					t.Fatal(err)
				}
				if workflow.Spec.DestinationPVC.UID != "" ||
					workflow.Spec.DestinationPVC.ResourceVersion != "" {
					t.Fatalf("controller-owned destination identity leaked into restore spec: %#v", workflow.Spec.DestinationPVC)
				}
				if workflow.Status.DestinationPVC == nil ||
					workflow.Status.DestinationPVC.UID != "destination-pvc-uid" ||
					workflow.Status.DestinationPV == nil ||
					workflow.Status.DestinationPV.UID != "destination-pv-uid" {
					t.Fatalf("restore status destination checkpoint=%#v", workflow.Status)
				}
			}
		})
	}
}

func TestStorageClassAllowedTopologiesRejectsBeforeMutation(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}
	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}
	mode := e2eMode(t)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-e2e"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-topology-" + suffix
	sessionID := "topology-" + suffix
	storageClassName := "pvc-migrate-topology-" + suffix

	rejectedTarget := chooseTargetNode(t, ctx, client, "")
	allowedTarget := chooseTargetNode(t, ctx, client, rejectedTarget)
	rejectedNode, err := client.CoreV1().Nodes().Get(ctx, rejectedTarget, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	allowedNode, err := client.CoreV1().Nodes().Get(ctx, allowedTarget, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rejectedHostname := rejectedNode.Labels[corev1.LabelHostname]
	allowedHostname := allowedNode.Labels[corev1.LabelHostname]
	if rejectedHostname == "" || allowedHostname == "" {
		t.Fatal("selected topology test nodes must have kubernetes.io/hostname labels")
	}
	baseClassName := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	baseClass, err := client.StorageV1().StorageClasses().Get(
		ctx,
		baseClassName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	parameters := make(map[string]string, len(baseClass.Parameters))
	for key, value := range baseClass.Parameters {
		parameters[key] = value
	}
	testClass, err := client.StorageV1().StorageClasses().Create(
		ctx,
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: storageClassName,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
					sessionLabel:                   sessionID,
				},
			},
			Provisioner:          baseClass.Provisioner,
			Parameters:           parameters,
			ReclaimPolicy:        baseClass.ReclaimPolicy,
			MountOptions:         append([]string(nil), baseClass.MountOptions...),
			AllowVolumeExpansion: baseClass.AllowVolumeExpansion,
			VolumeBindingMode:    baseClass.VolumeBindingMode,
			AllowedTopologies: []corev1.TopologySelectorTerm{{
				MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{
					Key: corev1.LabelHostname, Values: []string{allowedHostname},
				}},
			}},
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupTestResources(t, config, client, namespace, sessionID)
		uid := testClass.UID
		if err := client.StorageV1().StorageClasses().Delete(
			context.Background(),
			testClass.Name,
			metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
		); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("delete E2E StorageClass %s: %v", testClass.Name, err)
		}
	}()

	if _, err := client.CoreV1().Namespaces().Create(
		ctx,
		e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	storage := resource.MustParse("32Mi")
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(
		ctx,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &sourceClass,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: storage},
				},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods(namespace).Create(
		ctx,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: namespace},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				NodeSelector:  map[string]string{corev1.LabelHostname: rejectedHostname},
				Containers: []corev1.Container{
					{
						Name:  "writer",
						Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
						Command: []string{
							"sh",
							"-c",
							"echo topology > /data/payload; exec sleep 86400",
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "data", MountPath: "/data",
						}},
					},
				},
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data",
						},
					},
				}},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	waitForReadyPod(t, ctx, client, namespace, "writer", rejectedTarget)

	binary := e2eBinary(t, ctx)
	args := []string{
		"--kubeconfig", kubeconfig,
		"--mode", mode,
		"--session-namespace", namespace,
		"--timeout", "2m",
		"--output", "json",
		"--yes",
		"copy",
		"--session", sessionID,
		"--source-namespace", namespace,
		"--temporary-namespace", namespace,
		"--source-pvc", "data",
		"--target-node", rejectedTarget,
		"--destination-storage-class", storageClassName,
		"--strategy", "clusterip",
		"--online",
		"--dry-run=false",
	}
	output := runCLIExpectExitCode(t, ctx, binary, 3, args...)
	if !strings.Contains(string(output), "allowedTopologies") ||
		!strings.Contains(string(output), storageClassName) ||
		!strings.Contains(string(output), rejectedTarget) {
		t.Fatalf("topology rejection omitted actionable details: %s", output)
	}

	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
	sourcePVC, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sourcePVC.Labels[sessionLabel] != "" || sourcePVC.Annotations[sessionLabel] != "" {
		t.Fatalf("topology rejection changed source PVC ownership: %#v", sourcePVC.ObjectMeta)
	}
	ownedPVCs, err := client.CoreV1().PersistentVolumeClaims(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: sessionLabel + "=" + sessionID})
	if err != nil {
		t.Fatal(err)
	}
	ownedPods, err := client.CoreV1().Pods(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: sessionLabel + "=" + sessionID})
	if err != nil {
		t.Fatal(err)
	}
	if len(ownedPVCs.Items) != 0 || len(ownedPods.Items) != 0 {
		t.Fatalf(
			"topology rejection created managed resources: PVCs=%d Pods=%d",
			len(ownedPVCs.Items),
			len(ownedPods.Items),
		)
	}
}

func TestInitialDestinationCapacityDoesNotRequireVolumeExpansion(t *testing.T) {
	config, client, kubeconfig := capacityE2EClients(t)
	mode := e2eMode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	suffix := e2eSuffix()
	namespace := "pvc-migrate-capacity-" + suffix
	sessionID := "capacity-" + suffix
	storageClassName := "pvc-migrate-no-expand-" + suffix
	baseClass := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	testClass := createE2EStorageClass(
		t,
		ctx,
		client,
		baseClass,
		storageClassName,
		sessionID,
		false,
	)
	defer func() {
		cleanupTestResources(t, config, client, namespace, sessionID)
		deleteE2EStorageClass(t, client, testClass)
	}()

	createE2ENamespace(t, ctx, client, namespace, sessionID)
	sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	source := createCapacitySource(
		t,
		ctx,
		config,
		client,
		namespace,
		sourceClass,
		resource.MustParse("64Mi"),
		"initial-capacity-"+suffix,
	)
	deletePod(t, ctx, client, namespace, source.pod.Name)

	targetNode := chooseStorageClassNode(t, ctx, client, storageClassName, source.node)
	destinationCapacity := source.pv.Spec.Capacity[corev1.ResourceStorage].DeepCopy()
	destinationCapacity.Add(resource.MustParse("32Mi"))
	binary := e2eBinary(t, ctx)
	snapshot := runCapacityCopy(
		t,
		ctx,
		config,
		client,
		binary,
		kubeconfig,
		mode,
		namespace,
		sessionID,
		source.node,
		targetNode,
		storageClassName,
		destinationCapacity.String(),
	)
	assertCapacityCopyDestination(
		t,
		ctx,
		config,
		client,
		namespace,
		targetNode,
		snapshot,
		destinationCapacity,
		source.digest,
	)
}

func TestIncompleteSourceExpansionRejectsBeforeMutation(t *testing.T) {
	config, client, kubeconfig := capacityE2EClients(t)
	mode := e2eMode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	suffix := e2eSuffix()
	namespace := "pvc-migrate-resize-pending-" + suffix
	sessionID := "resize-pending-" + suffix
	storageClassName := "pvc-migrate-static-expand-" + suffix
	pvName := "pvc-migrate-static-expand-" + suffix
	allowExpansion := true
	volumeBindingMode := storagev1.VolumeBindingImmediate
	storageClass, err := client.StorageV1().StorageClasses().Create(
		ctx,
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: storageClassName,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
					sessionLabel:                   sessionID,
				},
			},
			Provisioner:          "migrate.sealos.io/e2e-static",
			AllowVolumeExpansion: &allowExpansion,
			VolumeBindingMode:    &volumeBindingMode,
		},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupTestResources(t, config, client, namespace, sessionID)
		deleteE2EStorageClass(t, client, storageClass)
	}()

	createE2ENamespace(t, ctx, client, namespace, sessionID)
	filesystem := corev1.PersistentVolumeFilesystem
	directoryOrCreate := corev1.HostPathDirectoryOrCreate
	initialCapacity := resource.MustParse("64Mi")
	if _, err := client.CoreV1().PersistentVolumes().Create(
		ctx,
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{corev1.ResourceStorage: initialCapacity},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/tmp/pvc-migrate-e2e/" + suffix,
						Type: &directoryOrCreate,
					},
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
				StorageClassName:              storageClassName,
				VolumeMode:                    &filesystem,
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(
		ctx,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &storageClassName,
				VolumeName:       pvName,
				VolumeMode:       &filesystem,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: initialCapacity,
				}},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}
	pvc := waitForBoundPVC(t, ctx, client, namespace, "data")
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("96Mi")
	_, err = client.CoreV1().PersistentVolumeClaims(namespace).
		Update(ctx, pvc, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	destinationClass := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	targetNode := chooseStorageClassNode(t, ctx, client, destinationClass, "")
	binary := e2eBinary(t, ctx)
	args := []string{
		"--kubeconfig", kubeconfig,
		"--mode", mode,
		"--session-namespace", namespace,
		"--timeout", "2m",
		"--output", "json",
		"--yes",
		"copy",
		"--session", sessionID,
		"--source-namespace", namespace,
		"--temporary-namespace", namespace,
		"--source-pvc", "data",
		"--target-node", targetNode,
		"--destination-storage-class", destinationClass,
		"--strategy", "clusterip",
		"--dry-run=false",
	}
	output := runCLIExpectExitCode(t, ctx, binary, 3, args...)
	if !strings.Contains(string(output), "currently provides 64Mi") ||
		!strings.Contains(string(output), "volume expansion is incomplete") {
		t.Fatalf("incomplete expansion rejection omitted actionable details: %s", output)
	}

	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
	currentPVC, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Get(ctx, "data", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if currentPVC.Labels[sessionLabel] != "" || currentPVC.Annotations[sessionLabel] != "" {
		t.Fatalf("capacity rejection changed source PVC ownership: %#v", currentPVC.ObjectMeta)
	}
	managedPVCs, err := client.CoreV1().PersistentVolumeClaims(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: sessionLabel + "=" + sessionID})
	if err != nil {
		t.Fatal(err)
	}
	managedPods, err := client.CoreV1().Pods(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: sessionLabel + "=" + sessionID})
	if err != nil {
		t.Fatal(err)
	}
	if len(managedPVCs.Items) != 0 || len(managedPods.Items) != 0 {
		t.Fatalf(
			"capacity rejection created managed resources: PVCs=%d Pods=%d",
			len(managedPVCs.Items),
			len(managedPods.Items),
		)
	}
}

func TestCompletedSourceExpansionUsesPVCapacity(t *testing.T) {
	config, client, kubeconfig := capacityE2EClients(t)
	mode := e2eMode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	sourceClass := envOrDefault(
		"PVC_MIGRATE_E2E_EXPANSION_CLASS",
		envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath"),
	)
	storageClass, err := client.StorageV1().StorageClasses().Get(
		ctx,
		sourceClass,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if storageClass.AllowVolumeExpansion == nil || !*storageClass.AllowVolumeExpansion {
		t.Skipf("StorageClass %s does not allow volume expansion", sourceClass)
	}

	suffix := e2eSuffix()
	namespace := "pvc-migrate-expanded-" + suffix
	sessionID := "expanded-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)
	createE2ENamespace(t, ctx, client, namespace, sessionID)
	source := createCapacitySource(
		t,
		ctx,
		config,
		client,
		namespace,
		sourceClass,
		resource.MustParse("64Mi"),
		"expanded-capacity-"+suffix,
	)

	expandedRequest := source.pv.Spec.Capacity[corev1.ResourceStorage].DeepCopy()
	expandedRequest.Add(resource.MustParse("32Mi"))
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Get(ctx, source.pvc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = expandedRequest
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	var expandedPV *corev1.PersistentVolume
	err = wait.PollUntilContextTimeout(
		ctx,
		2*time.Second,
		10*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			currentPVC, getErr := client.CoreV1().PersistentVolumeClaims(namespace).
				Get(waitCtx, source.pvc.Name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}
			currentPV, getErr := client.CoreV1().PersistentVolumes().Get(
				waitCtx,
				currentPVC.Spec.VolumeName,
				metav1.GetOptions{},
			)
			if getErr != nil {
				return false, getErr
			}
			pvCapacity := currentPV.Spec.Capacity[corev1.ResourceStorage]
			statusCapacity := currentPVC.Status.Capacity[corev1.ResourceStorage]
			if pvCapacity.Cmp(expandedRequest) < 0 || statusCapacity.Cmp(expandedRequest) < 0 {
				return false, nil
			}
			for _, condition := range currentPVC.Status.Conditions {
				if condition.Status == corev1.ConditionTrue &&
					(condition.Type == corev1.PersistentVolumeClaimResizing ||
						condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending) {
					return false, nil
				}
			}
			expandedPV = currentPV

			return true, nil
		},
	)
	if err != nil {
		t.Fatalf("wait for completed source expansion to %s: %v", expandedRequest.String(), err)
	}
	deletePod(t, ctx, client, namespace, source.pod.Name)

	destinationClass := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	targetNode := chooseStorageClassNode(t, ctx, client, destinationClass, source.node)
	destinationCapacity := expandedPV.Spec.Capacity[corev1.ResourceStorage].DeepCopy()
	destinationCapacity.Add(resource.MustParse("32Mi"))
	binary := e2eBinary(t, ctx)
	snapshot := runCapacityCopy(
		t,
		ctx,
		config,
		client,
		binary,
		kubeconfig,
		mode,
		namespace,
		sessionID,
		source.node,
		targetNode,
		destinationClass,
		destinationCapacity.String(),
	)
	expandedPVCapacity := expandedPV.Spec.Capacity[corev1.ResourceStorage]
	if len(snapshot.Spec.Volumes) != 1 ||
		snapshot.Spec.Volumes[0].SourceCapacity != expandedPVCapacity.String() {
		t.Fatalf("copy plan did not use expanded source PV capacity: %#v", snapshot.Spec.Volumes)
	}
	assertCapacityCopyDestination(
		t,
		ctx,
		config,
		client,
		namespace,
		targetNode,
		snapshot,
		destinationCapacity,
		source.digest,
	)
}

func TestStandaloneWFFCMigrationAndRollback(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}
	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}
	mode := e2eMode(t)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-e2e"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-e2e-" + suffix
	sessionID := "e2e-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)

	if _, err := client.CoreV1().
		Namespaces().
		Create(ctx, e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	destinationClass := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	claimName := "data"
	storage := resource.MustParse("64Mi")
	if _, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &sourceClass,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: storage},
				},
			},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	marker := "pvc-migrate-e2e-" + suffix
	initializerName := "initializer"
	if _, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: initializerName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "writer",
					Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
					Command: []string{
						"sh",
						"-c",
						"set -eu; printf '%s\\n' \"$1\" > /data/payload; dd if=/dev/zero bs=1048576 count=8 >> /data/payload 2>/dev/null; sync; touch /data/ready; exec sleep 86400",
						"initializer",
						marker,
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"test", "-f", "/data/ready"},
							},
						},
						PeriodSeconds: 1,
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	initializedPod := waitForReadyPod(t, ctx, client, namespace, initializerName, "")
	initialNode := initializedPod.Spec.NodeName
	uid := initializedPod.UID
	if err := client.CoreV1().
		Pods(namespace).
		Delete(ctx, initializerName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
		t.Fatal(err)
	}
	if err := waitForPodDeletion(ctx, client, namespace, initializerName); err != nil {
		t.Fatal(err)
	}

	podName := "writer"
	if _, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "writer",
					Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
					Command: []string{
						"sh",
						"-c",
						"set -eu; test -s /data/payload; test -f /data/ready; exec sleep 86400",
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"test", "-f", "/data/ready"},
							},
						},
						PeriodSeconds: 1,
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	sourcePod := waitForReadyPod(t, ctx, client, namespace, podName, "")
	sourceNode := sourcePod.Spec.NodeName
	if sourceNode != initialNode {
		t.Fatalf("writer node=%s initializer node=%s", sourceNode, initialNode)
	}
	targetNode := os.Getenv("PVC_MIGRATE_E2E_TARGET_NODE")
	if targetNode == "" {
		targetNode = chooseTargetNode(t, ctx, client, sourceNode)
	}
	if targetNode == sourceNode {
		t.Fatalf("target node %s equals source node", targetNode)
	}
	sourcePVC, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sourcePVC.Status.Phase != corev1.ClaimBound || sourcePVC.Spec.VolumeName == "" {
		t.Fatalf("source PVC phase=%s volume=%q", sourcePVC.Status.Phase, sourcePVC.Spec.VolumeName)
	}
	sourcePV := sourcePVC.Spec.VolumeName
	sourceDigest := podDigest(t, ctx, config, client, namespace, podName)
	binary := e2eBinary(t, ctx)
	common := []string{
		"--kubeconfig", kubeconfig,
		"--mode", mode,
		"--session-namespace", namespace,
		"--timeout", "12m",
		"--output", "json",
	}
	common = appendE2EToolImage(common)
	controllerProcess := startE2EController(
		t, ctx, client, binary, kubeconfig, namespace, mode,
	)
	defer controllerProcess.Stop(t)
	migration := []string{
		"migrate-pod",
		"--session", sessionID,
		"--source-namespace", namespace,
		"--temporary-namespace", namespace,
		"--pod", podName,
		"--target-node", targetNode,
		"--destination-storage-class", destinationClass,
		"--strategy", "clusterip",
		"--precopy-passes", "1",
	}
	dryRunMigration := append(append([]string{}, migration...), "--dry-run")
	runCLI(t, ctx, binary, append(append([]string{}, common...), dryRunMigration...)...)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID,
	)
	executeMigration := append(append([]string{"--yes"}, migration...), "--dry-run=false")
	runCLI(t, ctx, binary, append(append([]string{}, common...), executeMigration...)...)
	waitForSessionPhase(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID, "Completed",
	)
	controllerProcess.Stop(t)

	migratedPod := waitForReadyPod(t, ctx, client, namespace, podName, targetNode)
	if migratedPod.Spec.NodeName != targetNode {
		t.Fatalf("migrated Pod node=%s want=%s", migratedPod.Spec.NodeName, targetNode)
	}
	migratedPVC, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if migratedPVC.Spec.VolumeName == sourcePV || migratedPVC.Status.Phase != corev1.ClaimBound {
		t.Fatalf(
			"migrated PVC phase=%s volume=%s source=%s",
			migratedPVC.Status.Phase,
			migratedPVC.Spec.VolumeName,
			sourcePV,
		)
	}
	if digest := podDigest(t, ctx, config, client, namespace, podName); digest != sourceDigest {
		t.Fatalf("migrated digest=%s want=%s", digest, sourceDigest)
	}
	assertSessionPhase(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID, "Completed",
	)
	assertSessionPayloadShape(t, ctx, config, client, mode, namespace, sessionID)

	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"migrate-pod",
			"rollback",
			sessionID,
			"--dry-run=false",
		)...,
	)
	rolledBackPod := waitForReadyPod(t, ctx, client, namespace, podName, sourceNode)
	if rolledBackPod.Spec.NodeName != sourceNode {
		t.Fatalf("rolled-back Pod node=%s want=%s", rolledBackPod.Spec.NodeName, sourceNode)
	}
	rolledBackPVC, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rolledBackPVC.Spec.VolumeName != sourcePV {
		t.Fatalf("rolled-back PVC volume=%s want=%s", rolledBackPVC.Spec.VolumeName, sourcePV)
	}
	if digest := podDigest(t, ctx, config, client, namespace, podName); digest != sourceDigest {
		t.Fatalf("rolled-back digest=%s want=%s", digest, sourceDigest)
	}
	assertSessionPhase(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID, "RolledBack",
	)

	setRollbackPVsToDelete(t, ctx, client, sessionID)
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"migrate-pod",
			"cleanup",
			sessionID,
			"--delete-rollback-pv",
			"--finalize",
			"--delete-session",
			"--dry-run=false",
		)...,
	)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID,
	)
	finalPVC, err := client.CoreV1().
		PersistentVolumeClaims(namespace).
		Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if finalPVC.Annotations[sessionLabel] != "" {
		t.Fatalf("finalized PVC remains session-owned: %v", finalPVC.Annotations)
	}
}

func TestOfflineMigrationAndRollback(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}
	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}
	mode := e2eMode(t)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-e2e-offline"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-offline-" + suffix
	sessionID := "offline-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)

	if _, err := client.CoreV1().Namespaces().Create(
		ctx,
		e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		),
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	destinationClass := envOrDefault(
		"PVC_MIGRATE_E2E_DESTINATION_CLASS",
		"openebs-backup",
	)
	claimName := "data"
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(
		ctx,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &sourceClass,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("64Mi"),
				}},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	marker := "offline-e2e-" + suffix
	initializerName := "initializer"
	if _, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: initializerName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{{
				Name:  "writer",
				Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
				Command: []string{
					"sh",
					"-c",
					"set -eu; printf '%s\\n' \"$1\" > /data/payload; dd if=/dev/zero bs=1048576 count=8 >> /data/payload 2>/dev/null; sync; touch /data/ready; exec sleep 86400",
					"initializer",
					marker,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
						Command: []string{"test", "-f", "/data/ready"},
					}},
					PeriodSeconds: 1,
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: claimName,
					},
				},
			}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	initializer := waitForReadyPod(t, ctx, client, namespace, initializerName, "")
	sourceNode := initializer.Spec.NodeName
	sourceDigest := podDigest(t, ctx, config, client, namespace, initializerName)
	uid := initializer.UID
	if err := client.CoreV1().Pods(namespace).Delete(
		ctx,
		initializerName,
		metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	); err != nil {
		t.Fatal(err)
	}
	if err := waitForPodDeletion(ctx, client, namespace, initializerName); err != nil {
		t.Fatal(err)
	}

	targetNode := os.Getenv("PVC_MIGRATE_E2E_TARGET_NODE")
	if targetNode == "" || targetNode == sourceNode {
		targetNode = chooseTargetNode(t, ctx, client, sourceNode)
	}
	binary := e2eBinary(t, ctx)
	common := []string{
		"--kubeconfig", kubeconfig,
		"--mode", mode,
		"--session-namespace", namespace,
		"--timeout", "12m",
		"--output", "json",
	}
	common = appendE2EToolImage(common)
	controllerProcess := startE2EController(
		t, ctx, client, binary, kubeconfig, namespace, mode,
	)
	defer controllerProcess.Stop(t)
	migration := []string{
		"migrate",
		"--session", sessionID,
		"--source-namespace", namespace,
		"--temporary-namespace", namespace,
		"--source-pvc", claimName,
		"--source-node", sourceNode,
		"--target-node", targetNode,
		"--destination-storage-class", destinationClass,
		"--strategy", "clusterip",
	}
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			append(append([]string{}, migration...), "--dry-run")...,
		)...,
	)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "migrations", namespace, sessionID,
	)
	executeMigration := append(append([]string{"--yes"}, migration...), "--dry-run=false")
	runCLI(t, ctx, binary, append(append([]string{}, common...), executeMigration...)...)
	waitForSessionPhase(
		t, ctx, config, client, mode, "migrations", namespace, sessionID, "Completed",
	)
	controllerProcess.Stop(t)

	migratedPVC, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(
		ctx,
		claimName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if migratedPVC.Status.Phase != corev1.ClaimBound {
		t.Fatalf("migrated PVC phase=%s", migratedPVC.Status.Phase)
	}
	sessionData := readCopySession(
		t, ctx, config, client, mode, "migrations", namespace, sessionID,
	)
	if len(sessionData.Spec.Volumes) != 1 {
		t.Fatalf("offline session volumes=%d want=1", len(sessionData.Spec.Volumes))
	}
	sourcePV := sessionData.Spec.Volumes[0].SourcePV.Name
	if migratedPVC.Spec.VolumeName == sourcePV {
		t.Fatalf("offline migration retained source PV %s as active", sourcePV)
	}
	assertSessionPhase(
		t, ctx, config, client, mode, "migrations", namespace, sessionID, "Completed",
	)
	assertOfflineSessionPayloadShape(t, ctx, config, client, mode, namespace, sessionID)

	readerName := "migrated-reader"
	createPVCReader(t, ctx, client, namespace, readerName, targetNode, []string{claimName})
	waitForReadyPod(t, ctx, client, namespace, readerName, targetNode)
	if digest := readerDigest(
		t,
		ctx,
		config,
		client,
		namespace,
		readerName,
	); digest != sourceDigest {
		t.Fatalf("offline migrated digest=%s want=%s", digest, sourceDigest)
	}
	deletePod(t, ctx, client, namespace, readerName)

	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			[]string{"--yes", "migrate", "rollback", sessionID, "--dry-run=false"}...,
		)...,
	)
	assertSessionPhase(
		t, ctx, config, client, mode, "migrations", namespace, sessionID, "RolledBack",
	)
	rolledBackPVC, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(
		ctx,
		claimName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rolledBackPVC.Spec.VolumeName != sourcePV {
		t.Fatalf("rolled-back PVC volume=%s want=%s", rolledBackPVC.Spec.VolumeName, sourcePV)
	}
	readerName = "rollback-reader"
	createPVCReader(t, ctx, client, namespace, readerName, sourceNode, []string{claimName})
	waitForReadyPod(t, ctx, client, namespace, readerName, sourceNode)
	if digest := readerDigest(
		t,
		ctx,
		config,
		client,
		namespace,
		readerName,
	); digest != sourceDigest {
		t.Fatalf("offline rollback digest=%s want=%s", digest, sourceDigest)
	}
	deletePod(t, ctx, client, namespace, readerName)

	setRollbackPVsToDelete(t, ctx, client, sessionID)
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			[]string{
				"--yes", "migrate", "cleanup", sessionID,
				"--delete-rollback-pv", "--finalize", "--delete-session", "--dry-run=false",
			}...,
		)...,
	)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "migrations", namespace, sessionID,
	)
}

func TestHelmManagedStatefulSetMigrationAndRollback(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}
	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}
	mode := e2eMode(t)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-e2e-helm-statefulset"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-sts-" + suffix
	sessionID := "sts-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)

	if _, err := client.CoreV1().
		Namespaces().
		Create(ctx, e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	destinationClass := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	labels := map[string]string{
		"app":                          "redis",
		"component":                    "e2e",
		"app.kubernetes.io/managed-by": "Helm",
	}
	marker := "helm-statefulset-" + suffix
	if _, err := client.CoreV1().Services(namespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			Ports:     []corev1.ServicePort{{Name: "redis", Port: 6379}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	replicas := int32(1)
	if _, err := client.AppsV1().StatefulSets(namespace).Create(ctx, &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis",
			Namespace: namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"meta.helm.sh/release-name":      "redis",
				"meta.helm.sh/release-namespace": namespace,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "redis",
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{
						Name:  "writer",
						Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
						Command: []string{
							"sh",
							"-c",
							"set -eu; if [ ! -s /data/payload ]; then printf '%s\\n' \"$1\" > /data/payload; sync; fi; exec sleep 86400",
							"writer",
							marker,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{"test", "-s", "/data/payload"},
								},
							},
							PeriodSeconds: 1,
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					},
				}, RestartPolicy: corev1.RestartPolicyAlways},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: &sourceClass,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("64Mi"),
						},
					},
				},
			}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := waitForReadyPod(t, ctx, client, namespace, "redis-0", "")
	sourceNode := pod.Spec.NodeName
	sourceDigest := podDigest(t, ctx, config, client, namespace, pod.Name)
	targetNode := os.Getenv("PVC_MIGRATE_E2E_TARGET_NODE")
	if targetNode == "" || targetNode == sourceNode {
		targetNode = chooseTargetNode(t, ctx, client, sourceNode)
	}
	binary := e2eBinary(t, ctx)
	common := []string{
		"--kubeconfig",
		kubeconfig,
		"--mode",
		mode,
		"--session-namespace",
		namespace,
		"--timeout",
		"12m",
		"--output",
		"json",
	}
	common = appendE2EToolImage(common)
	controllerProcess := startE2EController(
		t, ctx, client, binary, kubeconfig, namespace, mode,
	)
	defer controllerProcess.Stop(t)
	migration := []string{
		"migrate-pod",
		"--session",
		sessionID,
		"--source-namespace",
		namespace,
		"--temporary-namespace",
		namespace,
		"--pod",
		"redis-0",
		"--target-node",
		targetNode,
		"--destination-storage-class",
		destinationClass,
		"--strategy",
		"clusterip",
		"--precopy-passes",
		"1",
	}
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			append(append([]string{}, migration...), "--dry-run")...,
		)...,
	)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID,
	)
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			append(append([]string{"--yes"}, migration...), "--dry-run=false")...,
		)...,
	)
	waitForSessionPhase(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID, "Completed",
	)
	controllerProcess.Stop(t)
	migrated := waitForReadyPod(t, ctx, client, namespace, "redis-0", targetNode)
	if digest := podDigest(
		t,
		ctx,
		config,
		client,
		namespace,
		migrated.Name,
	); digest != sourceDigest {
		t.Fatalf("migrated digest=%s want=%s", digest, sourceDigest)
	}
	assertSessionPhase(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID, "Completed",
	)

	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"migrate-pod",
			"rollback",
			sessionID,
			"--dry-run=false",
		)...,
	)
	rolledBack := waitForReadyPod(t, ctx, client, namespace, "redis-0", sourceNode)
	if digest := podDigest(
		t,
		ctx,
		config,
		client,
		namespace,
		rolledBack.Name,
	); digest != sourceDigest {
		t.Fatalf("rolled-back digest=%s want=%s", digest, sourceDigest)
	}
	assertSessionPhase(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID, "RolledBack",
	)
	statefulSet, err := client.AppsV1().
		StatefulSets(namespace).
		Get(ctx, "redis", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != replicas {
		t.Fatalf("StatefulSet replicas=%v want=%d", statefulSet.Spec.Replicas, replicas)
	}
	setRollbackPVsToDelete(t, ctx, client, sessionID)
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"migrate-pod",
			"cleanup",
			sessionID,
			"--delete-rollback-pv",
			"--finalize",
			"--delete-session",
			"--dry-run=false",
		)...,
	)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "podmigrations", namespace, sessionID,
	)
}

func TestOnlineCopyMultiVolumeIdempotencyAndCleanup(t *testing.T) {
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}
	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}
	mode := e2eMode(t)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-e2e-online"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}
	namespace := "pvc-migrate-online-e2e-" + suffix
	sessionID := "online-" + suffix
	defer cleanupTestResources(t, config, client, namespace, sessionID)

	if _, err := client.CoreV1().
		Namespaces().
		Create(ctx, e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	sourceClass := envOrDefault("PVC_MIGRATE_E2E_SOURCE_CLASS", "openebs-hostpath")
	destinationClass := envOrDefault("PVC_MIGRATE_E2E_DESTINATION_CLASS", "openebs-backup")
	claims := []string{"data-a", "data-b"}
	for _, claim := range claims {
		if _, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Create(ctx, &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: claim, Namespace: namespace},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: &sourceClass,
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("64Mi"),
					}},
				},
			}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	writerName := "live-writer"
	if _, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: writerName, Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  "writer",
					Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
					Command: []string{
						"sh",
						"-c",
						"set -eu; printf 'seed-a\\n' > /data-a/live.log; printf 'seed-b\\n' > /data-b/live.log; touch /data-a/ready /data-b/ready; sequence=0; while true; do sequence=$((sequence + 1)); printf '%s\\n' \"$sequence\" >> /data-a/live.log; printf '%s\\n' \"$sequence\" >> /data-b/live.log; sync; sleep 1; done",
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"test", "-f", "/data-a/ready"},
							},
						},
						PeriodSeconds: 1,
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "data-a", MountPath: "/data-a"},
						{Name: "data-b", MountPath: "/data-b"},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data-a",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data-a",
						},
					},
				},
				{
					Name: "data-b",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "data-b",
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	writer := waitForReadyPod(t, ctx, client, namespace, writerName, "")
	sourceNode := writer.Spec.NodeName
	targetNode := os.Getenv("PVC_MIGRATE_E2E_TARGET_NODE")
	if targetNode == "" {
		targetNode = chooseTargetNode(t, ctx, client, sourceNode)
	}
	if targetNode == sourceNode {
		t.Fatalf("target node %s equals source node", targetNode)
	}
	binary := e2eBinary(t, ctx)
	common := []string{
		"--kubeconfig",
		kubeconfig,
		"--mode",
		mode,
		"--session-namespace",
		namespace,
		"--timeout",
		"12m",
		"--output",
		"json",
	}
	common = appendE2EToolImage(common)
	controllerProcess := startE2EController(
		t, ctx, client, binary, kubeconfig, namespace, mode,
	)
	defer controllerProcess.Stop(t)
	copyArgs := []string{
		"copy",
		"--session",
		sessionID,
		"--source-namespace",
		namespace,
		"--temporary-namespace",
		namespace,
		"--target-node",
		targetNode,
		"--destination-storage-class",
		destinationClass,
		"--strategy",
		"clusterip",
		"--online",
		"--source-pvc",
		claims[0],
		"--source-pvc",
		claims[1],
	}
	repeatCopyArgs := []string{"copy", "--session", sessionID, "--online"}
	offlineCopyArgs := []string{
		"copy",
		"--session",
		sessionID,
		"--source-namespace",
		namespace,
		"--temporary-namespace",
		namespace,
		"--target-node",
		targetNode,
		"--destination-storage-class",
		destinationClass,
		"--strategy",
		"clusterip",
		"--source-pvc",
		claims[0],
		"--source-pvc",
		claims[1],
	}
	offlineCopyExecution := append(append([]string{"--yes"}, offlineCopyArgs...), "--dry-run=false")
	if output := runCLIExpectFailure(
		t,
		ctx,
		binary,
		append(append([]string{}, common...), offlineCopyExecution...)...,
	); !strings.Contains(
		string(output),
		"offline copy requires PVC",
	) {
		t.Fatalf("unexpected offline consumer error: %s", output)
	}
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)

	dryRunCopy := append(append([]string{}, copyArgs...), "--dry-run")
	runCLI(t, ctx, binary, append(append([]string{}, common...), dryRunCopy...)...)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
	executeCopy := append(append([]string{"--yes"}, copyArgs...), "--dry-run=false")
	runCLI(t, ctx, binary, append(append([]string{}, common...), executeCopy...)...)
	waitForSessionPhase(
		t, ctx, config, client, mode, "copies", namespace, sessionID, "WarmCopied",
	)
	controllerProcess.Stop(t)
	assertCopySession(t, ctx, config, client, mode, namespace, sessionID, claims, true)
	var statusOutput copySessionSnapshot
	if err := json.Unmarshal(
		runCLI(
			t,
			ctx,
			binary,
			append(append([]string{}, common...), "copy", "status", sessionID)...,
		),
		&statusOutput,
	); err != nil {
		t.Fatal(err)
	}
	if statusOutput.ID != sessionID || statusOutput.Spec.Type != "Copy" ||
		statusOutput.Status.Phase != "WarmCopied" {
		t.Fatalf("session status output is invalid: %#v", statusOutput)
	}
	var listedSessions []copySessionSnapshot
	if err := json.Unmarshal(
		runCLI(t, ctx, binary, append(append([]string{}, common...), "copy", "status")...),
		&listedSessions,
	); err != nil {
		t.Fatal(err)
	}
	if len(listedSessions) != 1 || listedSessions[0].ID != sessionID {
		t.Fatalf("session status list is invalid: %#v", listedSessions)
	}
	writer = waitForReadyPod(t, ctx, client, namespace, writerName, sourceNode)
	if writer.Spec.NodeName != sourceNode {
		t.Fatalf("online copy moved source writer to %s, want %s", writer.Spec.NodeName, sourceNode)
	}

	sessionData := readCopySession(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
	destinationClaims := make([]string, 0, len(sessionData.Spec.Volumes))
	for _, volume := range sessionData.Spec.Volumes {
		destinationClaims = append(destinationClaims, volume.DestinationPVC.Name)
		if volume.DestinationPVC.UID == "" || volume.DestinationPV.UID == "" {
			t.Fatalf("destination identity checkpoint missing: %#v", volume)
		}
	}
	readerName := "destination-reader"
	createPVCReader(t, ctx, client, namespace, readerName, targetNode, destinationClaims)
	defer deletePod(t, ctx, client, namespace, readerName)
	reader := waitForReadyPod(t, ctx, client, namespace, readerName, targetNode)
	initialDestinationLines := make([]int, len(destinationClaims))
	for index := range destinationClaims {
		destinationData := execOutput(
			t,
			ctx,
			config,
			client,
			namespace,
			reader.Name,
			"reader",
			[]string{"cat", "/data-" + strconv.Itoa(index) + "/live.log"},
		)
		sourceData := execOutput(
			t,
			ctx,
			config,
			client,
			namespace,
			writerName,
			"writer",
			[]string{"cat", "/" + claims[index] + "/live.log"},
		)
		initialDestinationLines[index] = assertAppendOnlyCopy(
			t,
			sourceData,
			destinationData,
			"seed-"+strings.TrimPrefix(claims[index], "data-"),
			destinationClaims[index],
		)
	}

	// Re-running copy against the completed Copy session exercises the durable
	// retry path while the source and destination claims remain mounted.
	repeatCopyExecution := append(append([]string{"--yes"}, repeatCopyArgs...), "--dry-run=false")
	runCLI(t, ctx, binary, append(append([]string{}, common...), repeatCopyExecution...)...)
	assertCopySession(t, ctx, config, client, mode, namespace, sessionID, claims, true)
	sessionData = readCopySession(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
	for index := range destinationClaims {
		destinationData := execOutput(
			t,
			ctx,
			config,
			client,
			namespace,
			reader.Name,
			"reader",
			[]string{"cat", "/data-" + strconv.Itoa(index) + "/live.log"},
		)
		sourceData := execOutput(
			t,
			ctx,
			config,
			client,
			namespace,
			writerName,
			"writer",
			[]string{"cat", "/" + claims[index] + "/live.log"},
		)
		if lines := assertAppendOnlyCopy(
			t,
			sourceData,
			destinationData,
			"seed-"+strings.TrimPrefix(claims[index], "data-"),
			destinationClaims[index],
		); lines <= initialDestinationLines[index] {
			t.Fatalf(
				"repeated copy did not advance %s: lines=%d initial=%d",
				destinationClaims[index],
				lines,
				initialDestinationLines[index],
			)
		}
		if sessionData.Status.Volumes[index].Sync.Attempts < 2 {
			t.Fatalf(
				"copy volume %d attempts=%d want at least 2",
				index,
				sessionData.Status.Volumes[index].Sync.Attempts,
			)
		}
	}
	for _, phase := range []string{"WarmCopying", "WarmCopied"} {
		count := 0
		for _, entry := range sessionData.Status.History {
			if entry.Phase == phase {
				count++
			}
		}
		if count < 2 {
			t.Fatalf(
				"session history contains %d %s entries, want at least 2: %#v",
				count,
				phase,
				sessionData.Status.History,
			)
		}
	}
	if output := runCLIExpectFailure(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"copy",
			"cleanup",
			sessionID,
			"--delete-temporary",
			"--dry-run=false",
		)...,
	); !strings.Contains(
		string(output),
		"is still referenced by Pod",
	) {
		t.Fatalf("unexpected mounted cleanup error: %s", output)
	}
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"copy",
			"abort",
			sessionID,
			"--dry-run=false",
		)...,
	)
	assertSessionPhase(t, ctx, config, client, mode, "copies", namespace, sessionID, "Aborted")
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"copy",
			"resume",
			sessionID,
			"--dry-run=false",
		)...,
	)
	assertSessionPhase(t, ctx, config, client, mode, "copies", namespace, sessionID, "Aborted")
	if output := runCLIExpectFailure(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"copy",
			"cleanup",
			sessionID,
			"--delete-temporary",
			"--dry-run=false",
		)...,
	); !strings.Contains(
		string(output),
		"is still referenced by Pod",
	) {
		t.Fatalf("unexpected mounted cleanup error: %s", output)
	}
	deletePod(t, ctx, client, namespace, readerName)
	runCLI(
		t,
		ctx,
		binary,
		append(
			append([]string{}, common...),
			"--yes",
			"copy",
			"cleanup",
			sessionID,
			"--delete-temporary",
			"--delete-rollback-pv",
			"--finalize",
			"--delete-session",
			"--dry-run=false",
		)...,
	)
	assertSessionRecordNotFound(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
	leases, err := client.CoordinationV1().
		Leases(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: labels.Set{sessionLabel: sessionID}.String()})
	if err != nil {
		t.Fatal(err)
	}
	if len(leases.Items) != 0 {
		t.Fatalf("session leases remain after cleanup: %#v", leases.Items)
	}
	for index, claim := range claims {
		volume := sessionData.Spec.Volumes[index]
		pvc, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, claim, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != volume.SourcePV.Name ||
			pvc.Annotations[sessionLabel] != "" {
			t.Fatalf("source PVC %s was not finalized: %#v", claim, pvc)
		}
		pv, err := client.CoreV1().
			PersistentVolumes().
			Get(ctx, volume.SourcePV.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if string(pv.UID) != volume.SourcePV.UID || pv.Labels[sessionLabel] != "" ||
			pv.Labels[roleLabel] != "" ||
			string(pv.Spec.PersistentVolumeReclaimPolicy) != volume.SourceReclaimPolicy {
			t.Fatalf("source PV %s was not finalized: %#v", pv.Name, pv)
		}
		if _, err := client.CoreV1().
			PersistentVolumeClaims(namespace).
			Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
			err,
		) {
			t.Fatalf("destination PVC %s still exists: %v", volume.DestinationPVC.Name, err)
		}
		if _, err := client.CoreV1().
			PersistentVolumes().
			Get(ctx, volume.DestinationPV.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
			err,
		) {
			t.Fatalf("destination PV %s still exists: %v", volume.DestinationPV.Name, err)
		}
	}
	waitForReadyPod(t, ctx, client, namespace, writerName, sourceNode)
}

type e2eControllerProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
	stdout  bytes.Buffer
	stderr  bytes.Buffer
	stopped bool
}

func startE2EController(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	binary, kubeconfig, namespace, mode string,
) *e2eControllerProcess {
	t.Helper()
	if mode != "controller" {
		return nil
	}

	healthProbeBindAddress := allocateLoopbackAddress(t)
	process := &e2eControllerProcess{done: make(chan struct{})}
	args := []string{
		"--kubeconfig", kubeconfig,
		"--mode", "controller",
		"--session-namespace", namespace,
		"--controller-namespace", namespace,
		"--timeout", "0",
		"--log-level", "info",
	}
	args = appendE2EToolImage(args)
	args = append(
		args,
		"controller",
		"--health-probe-bind-address", healthProbeBindAddress,
	)
	process.command = exec.CommandContext(ctx, binary, args...)
	process.command.Stdout = &process.stdout
	process.command.Stderr = &process.stderr
	if err := process.command.Start(); err != nil {
		t.Fatalf("start E2E controller: %v", err)
	}
	go func() {
		process.waitErr = process.command.Wait()
		close(process.done)
	}()

	err := wait.PollUntilContextTimeout(
		ctx,
		250*time.Millisecond,
		controllerStartup,
		true,
		func(waitCtx context.Context) (bool, error) {
			select {
			case <-process.done:
				return false, fmt.Errorf(
					"controller exited before leader election: %w",
					process.waitErr,
				)
			default:
			}

			lease, err := client.CoordinationV1().
				Leases(namespace).
				Get(waitCtx, controllerLease, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}

			if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
				return false, nil
			}

			request, requestErr := http.NewRequestWithContext(
				waitCtx,
				http.MethodGet,
				"http://"+healthProbeBindAddress+"/readyz",
				nil,
			)
			if requestErr != nil {
				return false, requestErr
			}
			response, requestErr := http.DefaultClient.Do(request)
			if requestErr != nil {
				return false, nil
			}
			defer response.Body.Close()

			return response.StatusCode == http.StatusOK, nil
		},
	)
	if err != nil {
		_ = process.command.Process.Kill()
		<-process.done
		t.Fatalf(
			"wait for E2E controller readiness: %v\nstdout:\n%s\nstderr:\n%s",
			err,
			process.stdout.String(),
			process.stderr.String(),
		)
	}

	return process
}

func allocateLoopbackAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate E2E controller health probe address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release E2E controller health probe address: %v", err)
	}

	return address
}

func (p *e2eControllerProcess) Stop(t *testing.T) {
	t.Helper()
	if p == nil || p.stopped {
		return
	}
	p.stopped = true

	select {
	case <-p.done:
		t.Fatalf(
			"E2E controller exited unexpectedly: %v\nstdout:\n%s\nstderr:\n%s",
			p.waitErr,
			p.stdout.String(),
			p.stderr.String(),
		)
	default:
	}

	if err := p.command.Process.Signal(os.Interrupt); err != nil {
		t.Errorf("stop E2E controller: %v", err)
		return
	}

	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		_ = p.command.Process.Kill()
		<-p.done
		t.Errorf("E2E controller did not stop within 30s")
	}
}

func e2eBinary(t *testing.T, ctx context.Context) string {
	t.Helper()
	if binary := os.Getenv("PVC_MIGRATE_E2E_BINARY"); binary != "" {
		return binary
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve E2E source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	binary := filepath.Join(t.TempDir(), "pvc-migrate")
	command := exec.CommandContext(
		ctx,
		"go",
		"build",
		"-trimpath",
		"-o",
		binary,
		"./cmd/pvc-migrate",
	)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build pvc-migrate: %v\n%s", err, output)
	}
	return binary
}

func runCLI(t *testing.T, ctx context.Context, binary string, args ...string) []byte {
	t.Helper()
	command := exec.CommandContext(ctx, binary, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err != nil {
		t.Fatalf(
			"pvc-migrate %s: %v\nstdout:\n%s\nstderr:\n%s",
			strings.Join(args, " "),
			err,
			stdout.String(),
			stderr.String(),
		)
	}
	return stdout.Bytes()
}

func runCLIExpectFailure(t *testing.T, ctx context.Context, binary string, args ...string) []byte {
	t.Helper()
	command := exec.CommandContext(ctx, binary, args...)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("pvc-migrate %s unexpectedly succeeded\n%s", strings.Join(args, " "), output)
	}
	return output
}

func runCLIExpectExitCode(
	t *testing.T,
	ctx context.Context,
	binary string,
	want int,
	args ...string,
) []byte {
	t.Helper()
	command := exec.CommandContext(ctx, binary, args...)
	output, err := command.CombinedOutput()
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != want {
		t.Fatalf(
			"pvc-migrate %s exit=%v want=%d\n%s",
			strings.Join(args, " "),
			err,
			want,
			output,
		)
	}
	return output
}

type copySessionVolume struct {
	SourcePVC struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		UID       string `json:"uid"`
	} `json:"sourcePVC"`
	SourcePV struct {
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"sourcePV"`
	SourceReclaimPolicy string `json:"sourceReclaimPolicy"`
	DestinationPVC      struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
		UID       string `json:"uid"`
	} `json:"destinationPVC"`
	DestinationPV struct {
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"destinationPV"`
	DestinationReclaimPolicy string   `json:"destinationReclaimPolicy"`
	SourceCapacity           string   `json:"sourceCapacity"`
	Capacity                 string   `json:"capacity"`
	StorageClass             string   `json:"storageClass"`
	AccessModes              []string `json:"accessModes"`
	VolumeMode               string   `json:"volumeMode"`
}

type copySessionSnapshot struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	ID         string `json:"id"`
	Generation int64  `json:"generation"`
	Spec       struct {
		Type                 string                     `json:"type"`
		SourceNamespace      string                     `json:"sourceNamespace"`
		TemporaryNamespace   string                     `json:"temporaryNamespace"`
		DestinationNamespace string                     `json:"destinationNamespace"`
		SessionNamespace     string                     `json:"sessionNamespace"`
		Copy                 map[string]json.RawMessage `json:"copy"`
		Volumes              []copySessionVolume        `json:"volumes"`
	} `json:"spec"`
	SpecFields map[string]json.RawMessage `json:"-"`
	Status     struct {
		Phase              string `json:"phase"`
		ObservedGeneration int64  `json:"observedGeneration"`
		StartedAt          string `json:"startedAt"`
		UpdatedAt          string `json:"updatedAt"`
		Volumes            []struct {
			DestinationPVC struct {
				Namespace string `json:"namespace"`
				Name      string `json:"name"`
				UID       string `json:"uid"`
			} `json:"destinationPVC"`
			DestinationPV struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"destinationPV"`
			DestinationReclaimPolicy string `json:"destinationReclaimPolicy"`
			Reserved                 bool   `json:"reserved"`
			Sync                     struct {
				WarmCompletedAt string `json:"warmCompletedAt"`
				Attempts        int    `json:"attempts"`
			} `json:"sync"`
		} `json:"volumes"`
		History []struct {
			Phase string `json:"phase"`
			Time  string `json:"time"`
		} `json:"history"`
	} `json:"status"`
}

func readCopySession(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, resourceName, namespace, id string,
) copySessionSnapshot {
	t.Helper()
	if mode == "controller" {
		object := readWorkflowObject(t, ctx, config, resourceName, namespace, id)
		var snapshot copySessionSnapshot
		snapshot.APIVersion = object.GetAPIVersion()
		snapshot.Kind = object.GetKind()
		snapshot.ID = object.GetName()
		snapshot.Generation = object.GetGeneration()

		spec, found, err := unstructured.NestedMap(object.Object, "spec")
		if err != nil || !found {
			t.Fatalf("read Copy spec: found=%t error=%v", found, err)
		}
		specJSON, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(specJSON, &snapshot.Spec); err != nil {
			t.Fatal(err)
		}
		if resourceName == "copies" {
			snapshot.Spec.Type = "Copy"
		} else if resourceName == "migrations" {
			snapshot.Spec.Type = "Migrate"
		}
		snapshot.SpecFields = make(map[string]json.RawMessage, len(spec))
		snapshot.Spec.Copy = make(map[string]json.RawMessage, len(spec))
		for field, value := range spec {
			raw, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			snapshot.SpecFields[field] = raw
			snapshot.Spec.Copy[field] = raw
		}

		status, found, err := unstructured.NestedMap(object.Object, "status")
		if err != nil || !found {
			t.Fatalf("read Copy status: found=%t error=%v", found, err)
		}
		statusJSON, err := json.Marshal(status)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(statusJSON, &snapshot.Status); err != nil {
			t.Fatal(err)
		}
		// Namespaced workflow specs intentionally use local references. Rebuild
		// the execution-model namespace in this test snapshot so the
		// assertions can compare identities with the live PVCs.
		if resourceName == "copies" {
			for index := range snapshot.Spec.Volumes {
				snapshot.Spec.Volumes[index].SourcePVC.Namespace = namespace
				snapshot.Spec.Volumes[index].DestinationPVC.Namespace = namespace
			}
		}
		for index := range min(len(snapshot.Spec.Volumes), len(snapshot.Status.Volumes)) {
			checkpoint := snapshot.Status.Volumes[index]
			snapshot.Spec.Volumes[index].DestinationPVC = checkpoint.DestinationPVC
			snapshot.Spec.Volumes[index].DestinationPV = checkpoint.DestinationPV
			snapshot.Spec.Volumes[index].DestinationReclaimPolicy = checkpoint.DestinationReclaimPolicy
		}

		return snapshot
	}

	configMap, err := client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, "pvc-migrate-session-"+id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if configMap.Labels[sessionLabel] != id || len(configMap.Data) != 1 ||
		configMap.Data[sessionKey] == "" {
		t.Fatalf(
			"session ConfigMap metadata or data is invalid: labels=%v dataKeys=%v",
			configMap.Labels,
			configMap.Data,
		)
	}
	var snapshot copySessionSnapshot
	if err := json.Unmarshal([]byte(configMap.Data[sessionKey]), &snapshot); err != nil {
		t.Fatal(err)
	}
	var document struct {
		Spec map[string]json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal([]byte(configMap.Data[sessionKey]), &document); err != nil {
		t.Fatal(err)
	}
	snapshot.SpecFields = document.Spec
	return snapshot
}

func assertCopySession(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, namespace, id string,
	sourceClaims []string,
	online bool,
) {
	t.Helper()
	snapshot := readCopySession(t, ctx, config, client, mode, "copies", namespace, id)
	if mode == "controller" {
		// Copy specs use local references; the assertion compares them with live
		// namespaced objects after the status-owned identities are overlaid.
		for index := range snapshot.Spec.Volumes {
			snapshot.Spec.Volumes[index].SourcePVC.Namespace = namespace
			snapshot.Spec.Volumes[index].DestinationPVC.Namespace = namespace
		}
	}
	wantKind := "MigrationSession"
	if mode == "controller" {
		wantKind = "Copy"
	}
	if snapshot.APIVersion != "migrate.sealos.io/v1alpha1" || snapshot.Kind != wantKind ||
		snapshot.ID != id ||
		snapshot.Generation != 1 {
		t.Fatalf(
			"copy session identity is invalid: apiVersion=%q kind=%q id=%q generation=%d",
			snapshot.APIVersion,
			snapshot.Kind,
			snapshot.ID,
			snapshot.Generation,
		)
	}
	if mode == "controller" && snapshot.Status.ObservedGeneration != snapshot.Generation {
		t.Fatalf(
			"Copy observedGeneration=%d want metadata generation=%d",
			snapshot.Status.ObservedGeneration,
			snapshot.Generation,
		)
	}
	if snapshot.Spec.Type != "Copy" || len(snapshot.Spec.Volumes) != len(sourceClaims) {
		t.Fatalf(
			"copy session type=%s volumes=%d want type Copy volumes=%d",
			snapshot.Spec.Type,
			len(snapshot.Spec.Volumes),
			len(sourceClaims),
		)
	}
	if mode != "controller" && (snapshot.Spec.SourceNamespace != namespace ||
		snapshot.Spec.TemporaryNamespace != namespace ||
		snapshot.Spec.DestinationNamespace != namespace ||
		snapshot.Spec.SessionNamespace != namespace) {
		t.Fatalf("copy session namespaces are invalid: %#v", snapshot.Spec)
	}
	if mode == "controller" {
		for _, field := range []string{"sourceNamespace", "temporaryNamespace", "destinationNamespace", "sessionNamespace"} {
			if _, exists := snapshot.SpecFields[field]; exists {
				t.Fatalf("field %q leaked into namespaced Copy CRD spec", field)
			}
		}
		for _, field := range []string{"type", "copy", "workload", "precopyPasses", "verifyChecksum", "reserve", "migrate", "migratePod", "rename", "move"} {
			if _, exists := snapshot.SpecFields[field]; exists {
				t.Fatalf("field %q leaked into Copy CRD spec", field)
			}
		}
		var volumes []map[string]json.RawMessage
		if err := json.Unmarshal(snapshot.SpecFields["volumes"], &volumes); err != nil {
			t.Fatal(err)
		}
		for index, volume := range volumes {
			for _, field := range []string{"destinationPV", "destinationReclaimPolicy"} {
				if _, exists := volume[field]; exists {
					t.Fatalf(
						"controller-owned field %q leaked into Copy spec volume %d",
						field,
						index,
					)
				}
			}
			var destinationPVC map[string]json.RawMessage
			if err := json.Unmarshal(volume["destinationPVC"], &destinationPVC); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"uid", "resourceVersion"} {
				if _, exists := destinationPVC[field]; exists {
					t.Fatalf(
						"controller-owned destinationPVC.%s leaked into Copy spec volume %d",
						field,
						index,
					)
				}
			}
		}
	} else {
		for _, field := range []string{"sourceNode", "targetNode", "strategies", "verifyChecksum", "deleteExtraneous", "online", "workload", "reserve", "migrate", "migratePod", "rename", "move"} {
			if _, exists := snapshot.SpecFields[field]; exists {
				t.Fatalf(
					"field %q leaked into Copy SessionCommon or selected the wrong payload",
					field,
				)
			}
		}
	}
	var storedOnline bool
	if err := json.Unmarshal(snapshot.Spec.Copy["online"], &storedOnline); err != nil {
		t.Fatal(err)
	}
	if storedOnline != online {
		t.Fatalf("copy online=%t want=%t", storedOnline, online)
	}
	for _, field := range []string{"sourceNode", "targetNode", "strategies", "deleteExtraneous"} {
		if _, exists := snapshot.Spec.Copy[field]; !exists {
			t.Fatalf("copy payload field %q missing", field)
		}
	}
	if _, exists := snapshot.Spec.Copy["verifyChecksum"]; exists {
		t.Fatal("copy payload serialized the default verifyChecksum=false")
	}
	var sourceNode, targetNode string
	if err := json.Unmarshal(snapshot.Spec.Copy["sourceNode"], &sourceNode); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(snapshot.Spec.Copy["targetNode"], &targetNode); err != nil {
		t.Fatal(err)
	}
	if sourceNode == "" || targetNode == "" {
		t.Fatalf("copy node options source=%q target=%q", sourceNode, targetNode)
	}
	if snapshot.Status.Phase != "WarmCopied" || snapshot.Status.StartedAt == "" ||
		snapshot.Status.UpdatedAt == "" {
		t.Fatalf("copy phase=%s want WarmCopied", snapshot.Status.Phase)
	}
	if len(snapshot.Status.Volumes) != len(sourceClaims) || len(snapshot.Status.History) < 5 {
		t.Fatalf(
			"copy status volumes=%d history=%d: %#v",
			len(snapshot.Status.Volumes),
			len(snapshot.Status.History),
			snapshot.Status,
		)
	}
	for index, source := range sourceClaims {
		volume := snapshot.Spec.Volumes[index]
		if volume.SourcePVC.Namespace != namespace || volume.SourcePVC.Name != source ||
			volume.SourcePVC.UID == "" ||
			volume.SourcePV.Name == "" ||
			volume.SourcePV.UID == "" ||
			volume.SourceReclaimPolicy == "" ||
			volume.DestinationPVC.Namespace != namespace ||
			volume.DestinationPVC.Name == "" ||
			volume.DestinationPVC.UID == "" ||
			volume.DestinationPV.Name == "" ||
			volume.DestinationPV.UID == "" ||
			volume.DestinationReclaimPolicy == "" ||
			volume.Capacity == "" ||
			volume.StorageClass == "" ||
			len(volume.AccessModes) != 1 ||
			volume.AccessModes[0] != "ReadWriteOnce" ||
			volume.VolumeMode != "Filesystem" {
			t.Fatalf("copy volume %d identity is incomplete: %#v", index, volume)
		}
		status := snapshot.Status.Volumes[index]
		if !status.Reserved || status.Sync.WarmCompletedAt == "" || status.Sync.Attempts < 1 {
			t.Fatalf("copy volume %d checkpoints are incomplete: %#v", index, status)
		}
	}
	for index, entry := range snapshot.Status.History {
		if entry.Phase == "" || entry.Time == "" {
			t.Fatalf("copy history entry %d is incomplete: %#v", index, entry)
		}
	}
}

func assertAppendOnlyCopy(t *testing.T, source, destination, seed, claim string) int {
	t.Helper()
	if destination == "" || !strings.HasPrefix(destination, seed+"\n") {
		t.Fatalf("destination claim %s has invalid seed or empty data: %q", claim, destination)
	}
	if !strings.HasPrefix(source, destination) {
		t.Fatalf(
			"destination claim %s is not an exact prefix of its append-only source: source=%q destination=%q",
			claim,
			source,
			destination,
		)
	}
	lines := strings.Split(strings.TrimSuffix(destination, "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf(
			"destination claim %s copied %d lines, want at least seed plus one live record",
			claim,
			len(lines),
		)
	}
	for index, line := range lines[1:] {
		if _, err := strconv.ParseInt(line, 10, 64); err != nil {
			t.Fatalf("destination claim %s live record %d is invalid: %q", claim, index, line)
		}
	}
	return len(lines)
}

type capacitySourceFixture struct {
	pvc    *corev1.PersistentVolumeClaim
	pv     *corev1.PersistentVolume
	pod    *corev1.Pod
	node   string
	digest string
}

func capacityE2EClients(
	t *testing.T,
) (*rest.Config, kubernetes.Interface, string) {
	t.Helper()
	if os.Getenv("PVC_MIGRATE_E2E") != "1" {
		t.Skip("set PVC_MIGRATE_E2E=1 to run cluster E2E tests")
	}

	kubeconfig := os.Getenv("PVC_MIGRATE_E2E_KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		t.Fatal("PVC_MIGRATE_E2E_KUBECONFIG or KUBECONFIG is required")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	config.UserAgent = "pvc-migrate-capacity-e2e"
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	return config, client, kubeconfig
}

func e2eSuffix() string {
	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
	if len(suffix) > 10 {
		suffix = suffix[len(suffix)-10:]
	}

	return suffix
}

func createE2ENamespace(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	namespace, sessionID string,
) {
	t.Helper()
	_, err := client.CoreV1().Namespaces().Create(
		ctx,
		e2eNamespace(
			namespace,
			map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		),
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func e2eNamespace(name string, additionalLabels map[string]string) *corev1.Namespace {
	namespaceLabels := maps.Clone(additionalLabels)
	if namespaceLabels == nil {
		namespaceLabels = make(map[string]string)
	}

	for key, value := range map[string]string{
		"pod-security.kubernetes.io/audit":           "restricted",
		"pod-security.kubernetes.io/audit-version":   podSecurityVersion,
		"pod-security.kubernetes.io/enforce":         "baseline",
		"pod-security.kubernetes.io/enforce-version": podSecurityVersion,
		"pod-security.kubernetes.io/warn":            "restricted",
		"pod-security.kubernetes.io/warn-version":    podSecurityVersion,
	} {
		namespaceLabels[key] = value
	}

	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: namespaceLabels,
	}}
}

func createE2EStorageClass(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	baseName, name, sessionID string,
	allowExpansion bool,
) *storagev1.StorageClass {
	t.Helper()
	base, err := client.StorageV1().StorageClasses().Get(
		ctx,
		baseName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	storageClass := base.DeepCopy()
	storageClass.TypeMeta = metav1.TypeMeta{}
	storageClass.ObjectMeta = metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
			sessionLabel:                   sessionID,
		},
	}
	storageClass.AllowVolumeExpansion = &allowExpansion
	created, err := client.StorageV1().StorageClasses().Create(
		ctx,
		storageClass,
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	return created
}

func deleteE2EStorageClass(
	t *testing.T,
	client kubernetes.Interface,
	storageClass *storagev1.StorageClass,
) {
	t.Helper()
	if storageClass == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	uid := storageClass.UID
	if err := client.StorageV1().StorageClasses().Delete(
		ctx,
		storageClass.Name,
		metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
	); err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("delete E2E StorageClass %s: %v", storageClass.Name, err)
	}
}

func createCapacitySource(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	namespace, storageClass string,
	capacity resource.Quantity,
	marker string,
) capacitySourceFixture {
	t.Helper()
	const claimName = "data"

	storageNode := chooseStorageClassNode(t, ctx, client, storageClass, "")
	node, err := client.CoreV1().Nodes().Get(ctx, storageNode, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(
		ctx,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: namespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &storageClass,
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: capacity,
				}},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	podName := "writer"
	if _, err := client.CoreV1().Pods(namespace).Create(
		ctx,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyAlways,
				NodeSelector:  map[string]string{corev1.LabelHostname: storageNode},
				Tolerations:   e2eNodeTolerations(node),
				Containers: []corev1.Container{{
					Name:  "writer",
					Image: envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
					Command: []string{
						"sh", "-c",
						"set -eu; printf '%s\\n' \"$1\" > /data/payload; " +
							"dd if=/dev/zero bs=1048576 count=4 >> /data/payload 2>/dev/null; " +
							"sync; touch /data/ready; exec sleep 86400",
						"writer", marker,
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
							Command: []string{"test", "-f", "/data/ready"},
						}},
						PeriodSeconds: 1,
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				}},
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: claimName,
						},
					},
				}},
			},
		},
		metav1.CreateOptions{},
	); err != nil {
		t.Fatal(err)
	}

	pod := waitForReadyPod(t, ctx, client, namespace, podName, "")
	pvc := waitForBoundPVC(t, ctx, client, namespace, claimName)
	pv, err := client.CoreV1().PersistentVolumes().Get(
		ctx,
		pvc.Spec.VolumeName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}

	return capacitySourceFixture{
		pvc:    pvc,
		pv:     pv,
		pod:    pod,
		node:   pod.Spec.NodeName,
		digest: podDigest(t, ctx, config, client, namespace, podName),
	}
}

func runCapacityCopy(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	binary, kubeconfig, mode, namespace, sessionID, sourceNode, targetNode,
	destinationClass, destinationCapacity string,
) copySessionSnapshot {
	t.Helper()
	common := []string{
		"--kubeconfig", kubeconfig,
		"--mode", mode,
		"--session-namespace", namespace,
		"--timeout", "12m",
		"--output", "json",
	}
	common = appendE2EToolImage(common)

	controllerProcess := startE2EController(
		t, ctx, client, binary, kubeconfig, namespace, mode,
	)
	defer controllerProcess.Stop(t)
	args := []string{
		"--yes",
		"copy",
		"--session", sessionID,
		"--source-namespace", namespace,
		"--temporary-namespace", namespace,
		"--source-pvc", "data",
		"--source-node", sourceNode,
		"--target-node", targetNode,
		"--destination-storage-class", destinationClass,
		"--destination-capacity", destinationCapacity,
		"--strategy", "clusterip",
		"--dry-run=false",
	}
	runCLI(t, ctx, binary, append(append([]string{}, common...), args...)...)
	waitForSessionPhase(
		t, ctx, config, client, mode, "copies", namespace, sessionID, "WarmCopied",
	)
	controllerProcess.Stop(t)

	return readCopySession(
		t, ctx, config, client, mode, "copies", namespace, sessionID,
	)
}

func assertCapacityCopyDestination(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	namespace, targetNode string,
	snapshot copySessionSnapshot,
	required resource.Quantity,
	sourceDigest string,
) {
	t.Helper()
	if len(snapshot.Spec.Volumes) != 1 {
		t.Fatalf("capacity copy volumes=%d want=1", len(snapshot.Spec.Volumes))
	}
	volume := snapshot.Spec.Volumes[0]
	destinationPVC, err := client.CoreV1().PersistentVolumeClaims(namespace).
		Get(ctx, volume.DestinationPVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	destinationPV, err := client.CoreV1().PersistentVolumes().Get(
		ctx,
		destinationPVC.Spec.VolumeName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	requested := destinationPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	provisioned := destinationPV.Spec.Capacity[corev1.ResourceStorage]
	if requested.Cmp(required) < 0 || provisioned.Cmp(required) < 0 {
		t.Fatalf(
			"destination capacity request=%s PV=%s required=%s",
			requested.String(),
			provisioned.String(),
			required.String(),
		)
	}

	createPVCReader(t, ctx, client, namespace, "capacity-reader", targetNode, []string{
		destinationPVC.Name,
	})
	waitForReadyPod(t, ctx, client, namespace, "capacity-reader", targetNode)
	if digest := readerDigest(
		t, ctx, config, client, namespace, "capacity-reader",
	); digest != sourceDigest {
		t.Fatalf("destination digest=%s want=%s", digest, sourceDigest)
	}
}

func waitForBoundPVC(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
) *corev1.PersistentVolumeClaim {
	t.Helper()
	var result *corev1.PersistentVolumeClaim
	err := wait.PollUntilContextTimeout(
		ctx,
		time.Second,
		6*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).
				Get(waitCtx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
				result = pvc

				return true, nil
			}

			return false, nil
		},
	)
	if err != nil {
		t.Fatalf("wait for PVC %s/%s binding: %v", namespace, name, err)
	}

	return result
}

func createPVCReader(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, node string,
	claims []string,
) {
	t.Helper()
	nodeObject, err := client.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	volumes := make([]corev1.Volume, 0, len(claims))
	mounts := make([]corev1.VolumeMount, 0, len(claims))
	for index, claim := range claims {
		volumeName := "data-" + strconv.Itoa(index)
		volumes = append(
			volumes,
			corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: claim,
					},
				},
			},
		)
		mounts = append(mounts, corev1.VolumeMount{Name: volumeName, MountPath: "/" + volumeName})
	}
	_, err = client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			NodeName:      node,
			RestartPolicy: corev1.RestartPolicyAlways,
			Tolerations:   e2eNodeTolerations(nodeObject),
			Containers: []corev1.Container{
				{
					Name:    "reader",
					Image:   envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
					Command: []string{"sh", "-c", "sleep 86400"},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{Command: []string{"true"}},
						},
						PeriodSeconds: 1,
					},
					VolumeMounts: mounts,
				},
			},
			Volumes: volumes,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func deletePod(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
) {
	t.Helper()
	pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().
		Pods(namespace).
		Delete(ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil &&
		!apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	if err := waitForPodDeletion(ctx, client, namespace, name); err != nil {
		t.Fatal(err)
	}
}

func waitForReadyPod(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name, expectedNode string,
) *corev1.Pod {
	t.Helper()
	var result *corev1.Pod
	err := wait.PollUntilContextTimeout(
		ctx,
		2*time.Second,
		6*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			pod, err := client.CoreV1().Pods(namespace).Get(waitCtx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if expectedNode != "" && pod.Spec.NodeName != expectedNode {
				return false, nil
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					result = pod
					return true, nil
				}
			}
			return false, nil
		},
	)
	if err != nil {
		pods, _ := client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
		t.Fatalf(
			"wait for Pod %s/%s on %s: %v; pods=%#v",
			namespace,
			name,
			expectedNode,
			err,
			pods.Items,
		)
	}
	return result
}

func waitForPodDeletion(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
) error {
	return wait.PollUntilContextTimeout(
		ctx,
		time.Second,
		2*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			_, err := client.CoreV1().Pods(namespace).Get(waitCtx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		},
	)
}

func chooseStorageClassNode(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	storageClassName, exclude string,
) string {
	t.Helper()
	storageClass, err := client.StorageV1().StorageClasses().Get(
		ctx,
		storageClassName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	for index := range nodes.Items {
		node := &nodes.Items[index]
		if node.Name == exclude || !kube.NodeReadyAndSchedulable(node) ||
			node.Labels[corev1.LabelHostname] == "" ||
			!kube.StorageClassAllowsNode(storageClass, node) {
			continue
		}
		if _, controlPlane := node.Labels["node-role.kubernetes.io/control-plane"]; controlPlane {
			continue
		}

		return node.Name
	}

	t.Fatalf(
		"no Ready node allowed by StorageClass %s differs from %s",
		storageClassName,
		exclude,
	)

	return ""
}

func e2eNodeTolerations(node *corev1.Node) []corev1.Toleration {
	if node == nil {
		return nil
	}

	result := make([]corev1.Toleration, 0, len(node.Spec.Taints))
	for _, taint := range node.Spec.Taints {
		if taint.Effect != corev1.TaintEffectNoSchedule &&
			taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}

		toleration := corev1.Toleration{Key: taint.Key, Effect: taint.Effect}
		if taint.Value == "" {
			toleration.Operator = corev1.TolerationOpExists
		} else {
			toleration.Operator = corev1.TolerationOpEqual
			toleration.Value = taint.Value
		}
		result = append(result, toleration)
	}

	return result
}

func chooseTargetNode(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	source string,
) string {
	t.Helper()
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Name == source || node.Spec.Unschedulable ||
			node.Labels[corev1.LabelHostname] == "" {
			continue
		}
		if _, controlPlane := node.Labels["node-role.kubernetes.io/control-plane"]; controlPlane {
			continue
		}
		blocked := false
		for _, taint := range node.Spec.Taints {
			if taint.Effect == corev1.TaintEffectNoSchedule ||
				taint.Effect == corev1.TaintEffectNoExecute {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				return node.Name
			}
		}
	}
	t.Fatalf("no Ready target node differs from %s", source)
	return ""
}

func podDigest(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	namespace, pod string,
) string {
	t.Helper()
	output := execOutput(
		t,
		ctx,
		config,
		client,
		namespace,
		pod,
		"writer",
		[]string{"sha256sum", "/data/payload"},
	)
	fields := strings.Fields(output)
	if len(fields) == 0 {
		t.Fatalf("empty sha256sum output: %q", output)
	}
	return fields[0]
}

func readerDigest(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	namespace, pod string,
) string {
	t.Helper()
	output := execOutput(
		t,
		ctx,
		config,
		client,
		namespace,
		pod,
		"reader",
		[]string{"sha256sum", "/data-0/payload"},
	)
	fields := strings.Fields(output)
	if len(fields) == 0 {
		t.Fatalf("empty reader sha256sum output: %q", output)
	}
	return fields[0]
}

func execOutput(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	namespace, pod, container string,
	command []string,
) string {
	t.Helper()
	request := client.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", request.URL())
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(
		ctx,
		remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr},
	); err != nil {
		t.Fatalf("exec in %s/%s: %v: %s", namespace, pod, err, stderr.String())
	}
	return stdout.String()
}

func workflowGVR(resourceName string) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group: workflowGroup, Version: workflowVersion, Resource: resourceName,
	}
}

func readWorkflowObject(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	resourceName, namespace, id string,
) *unstructured.Unstructured {
	t.Helper()
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	object, err := workflowResource(dynamicClient, resourceName, namespace).
		Get(ctx, id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	return object
}

func assertSessionRecordNotFound(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, resourceName, namespace, id string,
) {
	t.Helper()
	if _, err := client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, "pvc-migrate-session-"+id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("session ConfigMap unexpectedly exists in %s mode: %v", mode, err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflowResource(dynamicClient, resourceName, namespace).
		Get(ctx, id, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("%s workflow unexpectedly exists in %s mode: %v", resourceName, mode, err)
	}
}

func assertSessionPhase(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, resourceName, namespace, id, expected string,
) {
	t.Helper()
	phase, message, err := sessionPhase(
		ctx, config, client, mode, resourceName, namespace, id,
	)
	if err != nil {
		t.Fatal(err)
	}
	if phase != expected {
		t.Fatalf("session phase=%s want=%s: %s", phase, expected, message)
	}
}

func waitForSessionPhase(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, resourceName, namespace, id, expected string,
) {
	t.Helper()
	var lastPhase, lastMessage string
	err := wait.PollUntilContextTimeout(
		ctx,
		controllerPoll,
		15*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			phase, message, err := sessionPhase(
				waitCtx, config, client, mode, resourceName, namespace, id,
			)
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			lastPhase = phase
			lastMessage = message
			if phase == "Failed" {
				return false, fmt.Errorf("workflow failed: %s", message)
			}

			return phase == expected, nil
		},
	)
	if err != nil {
		t.Fatalf(
			"wait for %s %s/%s phase %s: %v; last phase=%s message=%s",
			resourceName,
			namespace,
			id,
			expected,
			err,
			lastPhase,
			lastMessage,
		)
	}
}

func sessionPhase(
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, resourceName, namespace, id string,
) (string, string, error) {
	if mode == "controller" {
		dynamicClient, err := dynamic.NewForConfig(config)
		if err != nil {
			return "", "", err
		}
		object, err := workflowResource(dynamicClient, resourceName, namespace).
			Get(ctx, id, metav1.GetOptions{})
		if err != nil {
			return "", "", err
		}
		phase, _, err := unstructured.NestedString(object.Object, "status", "phase")
		if err != nil {
			return "", "", err
		}
		message, _, err := unstructured.NestedString(object.Object, "status", "message")
		if err != nil {
			return "", "", err
		}
		failure, _, err := unstructured.NestedString(object.Object, "status", "failureReason")
		if err != nil {
			return "", "", err
		}
		if failure != "" {
			message = failure + ": " + message
		}

		return phase, message, nil
	}

	configMap, err := client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, "pvc-migrate-session-"+id, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	var stored struct {
		Status struct {
			Phase         string `json:"phase"`
			Message       string `json:"message"`
			FailureReason string `json:"failureReason"`
		} `json:"status"`
	}
	if err := json.Unmarshal([]byte(configMap.Data[sessionKey]), &stored); err != nil {
		return "", "", err
	}
	message := stored.Status.Message
	if stored.Status.FailureReason != "" {
		message = stored.Status.FailureReason + ": " + message
	}

	return stored.Status.Phase, message, nil
}

func workflowResource(
	dynamicClient dynamic.Interface,
	resourceName, namespace string,
) dynamic.ResourceInterface {
	resource := dynamicClient.Resource(workflowGVR(resourceName))
	switch resourceName {
	case domain.ClusterMigrationResource,
		domain.ClusterPodMigrationResource,
		domain.ClusterReservationResource,
		domain.ClusterCopyResource,
		domain.MoveResource:
		return resource
	default:
		return resource.Namespace(namespace)
	}
}

func assertSessionPayloadShape(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, namespace, id string,
) {
	t.Helper()
	if mode == "controller" {
		object := readWorkflowObject(t, ctx, config, "podmigrations", namespace, id)
		spec, found, err := unstructured.NestedMap(object.Object, "spec")
		if err != nil || !found {
			t.Fatalf("read PodMigration spec: found=%t error=%v", found, err)
		}
		for _, field := range []string{"type", "migrate", "migratePod", "backup", "restore", "online"} {
			if _, exists := spec[field]; exists {
				t.Fatalf("field %q leaked into PodMigration CRD spec", field)
			}
		}
		for _, field := range []string{"workload", "precopyPasses", "volumes"} {
			if _, exists := spec[field]; !exists {
				t.Fatalf("PodMigration CRD spec field %q missing", field)
			}
		}
		for _, field := range []string{"sourceNamespace", "temporaryNamespace", "destinationNamespace", "sessionNamespace"} {
			if _, exists := spec[field]; exists {
				t.Fatalf("field %q leaked into namespaced PodMigration CRD spec", field)
			}
		}

		return
	}

	configMap, err := client.CoreV1().
		ConfigMaps(namespace).
		Get(ctx, "pvc-migrate-session-"+id, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Spec map[string]json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal([]byte(configMap.Data[sessionKey]), &document); err != nil {
		t.Fatal(err)
	}
	var sessionType string
	if err := json.Unmarshal(document.Spec["type"], &sessionType); err != nil {
		t.Fatal(err)
	}
	if sessionType != "MigratePod" {
		t.Fatalf("session type=%q want MigratePod", sessionType)
	}
	for _, field := range []string{"sourceNode", "targetNode", "strategies", "verifyChecksum", "deleteExtraneous", "destinationStorageClass"} {
		if _, exists := document.Spec[field]; exists {
			t.Fatalf("workflow field %q leaked into SessionCommon", field)
		}
	}
	payload, exists := document.Spec["migratePod"]
	if !exists {
		t.Fatalf("migratePod payload missing: %s", configMap.Data[sessionKey])
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"sourceNode", "targetNode", "strategies", "deleteExtraneous", "workload"} {
		if _, exists := fields[field]; !exists {
			t.Fatalf("migratePod payload field %q missing: %s", field, payload)
		}
	}
	if _, exists := fields["verifyChecksum"]; exists {
		t.Fatal("migratePod payload serialized the default verifyChecksum=false")
	}
}

func assertOfflineSessionPayloadShape(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	client kubernetes.Interface,
	mode, namespace, id string,
) {
	t.Helper()
	if mode == "controller" {
		object := readWorkflowObject(t, ctx, config, "migrations", namespace, id)
		spec, found, err := unstructured.NestedMap(object.Object, "spec")
		if err != nil || !found {
			t.Fatalf("read Migration spec: found=%t error=%v", found, err)
		}
		for _, forbidden := range []string{
			"type", "migrate", "migratePod", "workload", "precopyPasses", "openebsLvmEnableShared",
		} {
			if _, exists := spec[forbidden]; exists {
				t.Fatalf("field %q leaked into Migration CRD spec", forbidden)
			}
		}
		for _, required := range []string{"volumes"} {
			if _, exists := spec[required]; !exists {
				t.Fatalf("Migration CRD spec field %q missing", required)
			}
		}
		for _, field := range []string{"sourceNamespace", "temporaryNamespace", "destinationNamespace", "sessionNamespace"} {
			if _, exists := spec[field]; exists {
				t.Fatalf("field %q leaked into namespaced Migration CRD spec", field)
			}
		}

		return
	}

	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(
		ctx,
		"pvc-migrate-session-"+id,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Spec map[string]json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal([]byte(configMap.Data[sessionKey]), &document); err != nil {
		t.Fatal(err)
	}
	var sessionType string
	if err := json.Unmarshal(document.Spec["type"], &sessionType); err != nil {
		t.Fatal(err)
	}
	if sessionType != "Migrate" {
		t.Fatalf("session type=%q want Migrate", sessionType)
	}
	for _, forbidden := range []string{"migratePod", "workload", "precopyPasses", "openebsLvmEnableShared"} {
		if _, exists := document.Spec[forbidden]; exists {
			t.Fatalf("real-time field %q leaked into offline Session spec", forbidden)
		}
	}
	payload, exists := document.Spec["migrate"]
	if !exists {
		t.Fatalf("offline migrate payload missing: %s", configMap.Data[sessionKey])
	}
	for _, forbidden := range []string{"workload", "precopyPasses", "openebsLvmEnableShared"} {
		if bytes.Contains(payload, []byte(forbidden)) {
			t.Fatalf("real-time field %q leaked into offline payload: %s", forbidden, payload)
		}
	}
}

func setRollbackPVsToDelete(
	t *testing.T,
	ctx context.Context,
	client kubernetes.Interface,
	sessionID string,
) {
	t.Helper()
	selector := labels.Set{sessionLabel: sessionID, roleLabel: "rollback"}.String()
	volumes, err := client.CoreV1().
		PersistentVolumes().
		List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Fatal(err)
	}
	if len(volumes.Items) == 0 {
		t.Fatal("session has no rollback PV")
	}
	for i := range volumes.Items {
		name := volumes.Items[i].Name
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			volume, getErr := client.CoreV1().
				PersistentVolumes().
				Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return getErr
			}
			volume.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
			_, updateErr := client.CoreV1().
				PersistentVolumes().
				Update(ctx, volume, metav1.UpdateOptions{})
			return updateErr
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func cleanupTestResources(
	t *testing.T,
	config *rest.Config,
	client kubernetes.Interface,
	namespace, sessionID string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	volumes, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err == nil {
		for i := range volumes.Items {
			volume := &volumes.Items[i]
			owned := volume.Labels[sessionLabel] == sessionID
			claimed := volume.Spec.ClaimRef != nil && volume.Spec.ClaimRef.Namespace == namespace
			if !owned && !claimed {
				continue
			}
			_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				current, getErr := client.CoreV1().
					PersistentVolumes().
					Get(ctx, volume.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(getErr) {
					return nil
				}
				if getErr != nil {
					return getErr
				}
				current.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimDelete
				_, updateErr := client.CoreV1().
					PersistentVolumes().
					Update(ctx, current, metav1.UpdateOptions{})
				return updateErr
			})
		}
	}
	sessionConfigMapName := "pvc-migrate-session-" + sessionID
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		configMap, getErr := client.CoreV1().
			ConfigMaps(namespace).
			Get(ctx, sessionConfigMapName, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		if configMap == nil || configMap.Labels[sessionLabel] != sessionID {
			return nil
		}
		configMap.Finalizers = nil
		_, updateErr := client.CoreV1().
			ConfigMaps(namespace).
			Update(ctx, configMap, metav1.UpdateOptions{})
		return updateErr
	}); err != nil {
		t.Errorf(
			"remove E2E session protection from %s/%s: %v",
			namespace,
			sessionConfigMapName,
			err,
		)
	}
	cleanupWorkflowResources(t, ctx, config, namespace, sessionID)
	propagation := metav1.DeletePropagationBackground
	if err := client.CoreV1().
		Namespaces().
		Delete(ctx, namespace, metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil &&
		!apierrors.IsNotFound(err) {
		t.Errorf("delete E2E namespace %s: %v", namespace, err)
	}
	_ = wait.PollUntilContextTimeout(
		ctx,
		2*time.Second,
		3*time.Minute,
		true,
		func(waitCtx context.Context) (bool, error) {
			_, getErr := client.CoreV1().Namespaces().Get(waitCtx, namespace, metav1.GetOptions{})
			return apierrors.IsNotFound(getErr), nil
		},
	)
	remaining, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Errorf("list PVs after E2E cleanup: %v", err)
		return
	}
	for i := range remaining.Items {
		volume := &remaining.Items[i]
		owned := volume.Labels[sessionLabel] == sessionID
		claimed := volume.Spec.ClaimRef != nil && volume.Spec.ClaimRef.Namespace == namespace
		if !owned && !claimed {
			continue
		}
		uid := volume.UID
		if err := client.CoreV1().
			PersistentVolumes().
			Delete(ctx, volume.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil &&
			!apierrors.IsNotFound(err) {
			t.Errorf("delete E2E PV %s: %v", volume.Name, err)
		}
	}
}

func cleanupWorkflowResources(
	t *testing.T,
	ctx context.Context,
	config *rest.Config,
	namespace, sessionID string,
) {
	t.Helper()
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Errorf("create dynamic client for E2E cleanup: %v", err)
		return
	}

	for _, resourceName := range []string{
		"migrations", "podmigrations", "reservations", "copies",
		"backups", "restores", "renames",
	} {
		cleanupWorkflowResource(
			t,
			ctx,
			dynamicClient.Resource(workflowGVR(resourceName)).Namespace(namespace),
			resourceName,
			sessionID,
		)
	}

	for _, resourceName := range []string{
		"clustermigrations", "clusterpodmigrations", "clusterreservations",
		"clustercopies", "moves",
	} {
		cleanupWorkflowResource(
			t,
			ctx,
			dynamicClient.Resource(workflowGVR(resourceName)),
			resourceName,
			sessionID,
		)
	}
}

func cleanupWorkflowResource(
	t *testing.T,
	ctx context.Context,
	resources dynamic.ResourceInterface,
	resourceName, sessionID string,
) {
	t.Helper()

	objects, err := resources.List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{sessionLabel: sessionID}.String(),
	})
	if apierrors.IsNotFound(err) {
		return
	}
	if err != nil {
		t.Errorf("list E2E %s resources: %v", resourceName, err)
		return
	}

	for i := range objects.Items {
		name := objects.Items[i].GetName()
		owned := false
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			object, getErr := resources.Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				return nil
			}
			if getErr != nil {
				return getErr
			}
			if object.GetLabels()[sessionLabel] != sessionID {
				return nil
			}
			owned = true
			object.SetFinalizers(nil)
			_, updateErr := resources.Update(ctx, object, metav1.UpdateOptions{})
			return updateErr
		})
		if err != nil {
			t.Errorf("remove E2E %s/%s finalizer: %v", resourceName, name, err)
			continue
		}
		if !owned {
			continue
		}
		if err := resources.Delete(ctx, name, metav1.DeleteOptions{}); err != nil &&
			!apierrors.IsNotFound(err) {
			t.Errorf("delete E2E %s/%s resource: %v", resourceName, name, err)
		}
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func appendE2EToolImage(args []string) []string {
	if toolImage := os.Getenv("PVC_MIGRATE_E2E_TOOL_IMAGE"); toolImage != "" {
		return append(args, "--tool-image", toolImage)
	}

	return args
}

func e2eMode(t *testing.T) string {
	t.Helper()
	mode := envOrDefault("PVC_MIGRATE_E2E_MODE", "session")
	if mode != "session" && mode != "controller" {
		t.Fatalf("PVC_MIGRATE_E2E_MODE must be session or controller, got %q", mode)
	}

	return mode
}

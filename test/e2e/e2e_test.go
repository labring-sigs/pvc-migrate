//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/retry"
)

const (
	sessionLabel      = "pvc-migrate.io/session"
	roleLabel         = "pvc-migrate.io/role"
	sessionKey        = "session.json"
	workflowGroup     = "migrate.sealos.io"
	workflowVersion   = "v1alpha1"
	controllerLease   = "pvc-migrate-controller"
	controllerPoll    = time.Second
	controllerStartup = 2 * time.Minute
)

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
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		}},
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
		domain.WorkloadSpec{Adapter: domain.WorkloadNone},
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
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		}},
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
				Containers: []corev1.Container{{
					Name:    "writer",
					Image:   envOrDefault("PVC_MIGRATE_E2E_HELPER_IMAGE", "busybox:1.36.1"),
					Command: []string{"sh", "-c", "echo topology > /data/payload; exec sleep 86400"},
					VolumeMounts: []corev1.VolumeMount{{
						Name: "data", MountPath: "/data",
					}},
				}},
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
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
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
		Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		}}, metav1.CreateOptions{}); err != nil {
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
	if toolImage := os.Getenv("PVC_MIGRATE_E2E_TOOL_IMAGE"); toolImage != "" {
		common = append(common, "--tool-image", toolImage)
	}
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

	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		},
	}, metav1.CreateOptions{}); err != nil {
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
	if toolImage := os.Getenv("PVC_MIGRATE_E2E_TOOL_IMAGE"); toolImage != "" {
		common = append(common, "--tool-image", toolImage)
	}
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
		Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		}}, metav1.CreateOptions{}); err != nil {
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
	if toolImage := os.Getenv("PVC_MIGRATE_E2E_TOOL_IMAGE"); toolImage != "" {
		common = append(common, "--tool-image", toolImage)
	}
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
		Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		}}, metav1.CreateOptions{}); err != nil {
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
	if toolImage := os.Getenv("PVC_MIGRATE_E2E_TOOL_IMAGE"); toolImage != "" {
		common = append(common, "--tool-image", toolImage)
	}
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

	process := &e2eControllerProcess{done: make(chan struct{})}
	process.command = exec.CommandContext(
		ctx,
		binary,
		"--kubeconfig", kubeconfig,
		"--mode", "controller",
		"--session-namespace", namespace,
		"--timeout", "0",
		"--log-level", "info",
		"controller",
		"--poll-interval", controllerPoll.String(),
	)
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
				return false, fmt.Errorf("controller exited before leader election: %w", process.waitErr)
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

			return lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "", nil
		},
	)
	if err != nil {
		_ = process.command.Process.Kill()
		<-process.done
		t.Fatalf(
			"wait for E2E controller leader election: %v\nstdout:\n%s\nstderr:\n%s",
			err,
			process.stdout.String(),
			process.stderr.String(),
		)
	}

	return process
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
		for index := range min(len(snapshot.Spec.Volumes), len(snapshot.Status.Volumes)) {
			checkpoint := snapshot.Status.Volumes[index]
			snapshot.Spec.Volumes[index].DestinationPVC = checkpoint.DestinationPVC
			snapshot.Spec.Volumes[index].DestinationPV = checkpoint.DestinationPV
			snapshot.Spec.Volumes[index].DestinationReclaimPolicy =
				checkpoint.DestinationReclaimPolicy
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
	if snapshot.Spec.SourceNamespace != namespace ||
		snapshot.Spec.TemporaryNamespace != namespace ||
		snapshot.Spec.DestinationNamespace != namespace ||
		snapshot.Spec.SessionNamespace != namespace {
		t.Fatalf("copy session namespaces are invalid: %#v", snapshot.Spec)
	}
	if mode == "controller" {
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
					t.Fatalf("controller-owned field %q leaked into Copy spec volume %d", field, index)
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
				t.Fatalf("field %q leaked into Copy SessionCommon or selected the wrong payload", field)
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
	requiredFields := []string{"sourceNode", "targetNode", "strategies", "deleteExtraneous"}
	if mode == "session" {
		requiredFields = append(requiredFields, "verifyChecksum")
	}
	for _, field := range requiredFields {
		if _, exists := snapshot.Spec.Copy[field]; !exists {
			t.Fatalf("copy payload field %q missing", field)
		}
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
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "pvc-migrate-e2e",
				sessionLabel:                   sessionID,
			},
		}},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
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
	if toolImage := os.Getenv("PVC_MIGRATE_E2E_TOOL_IMAGE"); toolImage != "" {
		common = append(common, "--tool-image", toolImage)
	}

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
	object, err := dynamicClient.Resource(workflowGVR(resourceName)).
		Namespace(namespace).
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
	if _, err := dynamicClient.Resource(workflowGVR(resourceName)).
		Namespace(namespace).
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
		object, err := dynamicClient.Resource(workflowGVR(resourceName)).
			Namespace(namespace).
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
		for _, field := range []string{"workload", "precopyPasses", "volumes", "sourceNamespace", "destinationNamespace"} {
			if _, exists := spec[field]; !exists {
				t.Fatalf("PodMigration CRD spec field %q missing", field)
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
	for _, field := range []string{"sourceNode", "targetNode", "strategies", "verifyChecksum", "deleteExtraneous", "workload"} {
		if _, exists := fields[field]; !exists {
			t.Fatalf("migratePod payload field %q missing: %s", field, payload)
		}
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
		for _, required := range []string{"volumes", "sourceNamespace", "destinationNamespace"} {
			if _, exists := spec[required]; !exists {
				t.Fatalf("Migration CRD spec field %q missing", required)
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
		"backups", "restores", "renames", "moves",
	} {
		resources := dynamicClient.Resource(workflowGVR(resourceName)).Namespace(namespace)
		owned := false
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			object, getErr := resources.Get(ctx, sessionID, metav1.GetOptions{})
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
			t.Errorf("remove E2E %s finalizer: %v", resourceName, err)
			continue
		}
		if !owned {
			continue
		}
		if err := resources.Delete(ctx, sessionID, metav1.DeleteOptions{}); err != nil &&
			!apierrors.IsNotFound(err) {
			t.Errorf("delete E2E %s resource: %v", resourceName, err)
		}
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func e2eMode(t *testing.T) string {
	t.Helper()
	mode := envOrDefault("PVC_MIGRATE_E2E_MODE", "session")
	if mode != "session" && mode != "controller" {
		t.Fatalf("PVC_MIGRATE_E2E_MODE must be session or controller, got %q", mode)
	}

	return mode
}

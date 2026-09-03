package kube

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestToolImageProbeRunsOncePerNodeAndChecksEveryComponent(t *testing.T) {
	node := readyProbeNode("node-a")
	client := fake.NewClientset(node)

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("probe-uid")
			created = pod.DeepCopy()
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", successfulProbeGetReactor(t, client))

	var output bytes.Buffer

	prober := NewToolImageProber(client)

	_, err := prober.Probe(context.Background(), ToolImageProbeOptions{
		OperationID: "session-123",
		Image:       "registry.example/pvc-migrate:test",
		Targets: []ToolProbeTarget{
			{Namespace: "system", NodeName: "node-a", Components: []string{ToolComponentRsync}},
			{Namespace: "system", NodeName: "node-a", Components: []string{ToolComponentSSHD}},
		},
		Timeout: time.Second,
		Poll:    time.Millisecond,
		Writer:  &output,
	})
	if err != nil {
		t.Fatal(err)
	}

	if created == nil {
		t.Fatal("probe Pod was not created")
	}

	if created.Namespace != "system" ||
		created.Spec.NodeSelector[corev1.LabelHostname] != "node-a" {
		t.Fatalf(
			"probe placement=%s/%s selector=%v",
			created.Namespace,
			created.Name,
			created.Spec.NodeSelector,
		)
	}

	if created.Spec.AutomountServiceAccountToken == nil ||
		*created.Spec.AutomountServiceAccountToken {
		t.Fatalf("probe automountServiceAccountToken=%v", created.Spec.AutomountServiceAccountToken)
	}

	if len(created.Spec.Tolerations) != 1 || created.Spec.Tolerations[0].Key != "storage" {
		t.Fatalf("probe tolerations=%v", created.Spec.Tolerations)
	}

	container := created.Spec.Containers[0]
	if container.Image != "registry.example/pvc-migrate:test" ||
		container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("probe image=%s pullPolicy=%s", container.Image, container.ImagePullPolicy)
	}

	if len(container.Command) != 3 || container.Command[0] != "sh" || container.Command[1] != "-c" {
		t.Fatalf("probe command=%q", container.Command)
	}

	if container.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Fatalf("terminationMessagePolicy=%s", container.TerminationMessagePolicy)
	}

	for _, command := range []string{
		"command -v sh", "command -v sleep", "command -v test",
		"command -v rsync", "command -v ssh", "rsync --version", "ssh -V",
		"command -v sshd", "command -v ssh-keygen", "command -v awk", "command -v id",
		"ssh-keygen -q -t ed25519", "ssh-keygen -q -t ecdsa", "/usr/sbin/sshd -t -f /etc/ssh/sshd_config",
	} {
		if !strings.Contains(container.Command[2], command) {
			t.Fatalf("probe command lacks %q: %s", command, container.Command[2])
		}
	}

	if container.SecurityContext == nil || container.SecurityContext.RunAsUser == nil ||
		*container.SecurityContext.RunAsUser != 0 ||
		container.SecurityContext.RunAsGroup == nil ||
		*container.SecurityContext.RunAsGroup != 0 {
		t.Fatalf("probe securityContext=%#v", container.SecurityContext)
	}

	if container.SecurityContext.Capabilities == nil ||
		!slices.Contains(
			container.SecurityContext.Capabilities.Add,
			corev1.Capability("SYS_CHROOT"),
		) {
		t.Fatalf("probe capabilities=%v", container.SecurityContext.Capabilities)
	}

	if !strings.Contains(output.String(), "tool image probe succeeded") ||
		!strings.Contains(output.String(), "node=node-a") ||
		!strings.Contains(output.String(), "components=rsync,sh,sshd") {
		t.Fatalf("probe output=%q", output.String())
	}

	if _, err := client.CoreV1().
		Pods("system").
		Get(context.Background(), created.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("probe Pod still exists: %v", err)
	}
}

func TestToolImageProbeCorrelatesPVCWithoutMountWhenNodeIsExplicit(t *testing.T) {
	target := ToolProbeTarget{
		Namespace:    "system",
		NodeName:     "node-a",
		PVCName:      "data",
		SkipPVCMount: true,
		Components:   []string{ToolComponentSSHD},
	}

	pod := toolProbePod(
		"registry.example/tool:test",
		"operation",
		target,
		readyProbeNode("node-a"),
		time.Minute,
	)
	if len(pod.Spec.Volumes) != 0 || len(pod.Spec.Containers[0].VolumeMounts) != 0 {
		t.Fatalf(
			"probe unexpectedly mounts PVC: volumes=%#v mounts=%#v",
			pod.Spec.Volumes,
			pod.Spec.Containers[0].VolumeMounts,
		)
	}

	if pod.Annotations[MetadataDomain+"/probe-pvc"] != "data" {
		t.Fatalf("probe PVC annotation=%q", pod.Annotations[MetadataDomain+"/probe-pvc"])
	}
}

func TestToolProbePodUsesWritablePVCMountWhenRequested(t *testing.T) {
	target := ToolProbeTarget{
		Namespace:        "system",
		NodeName:         "node-a",
		PVCName:          "data",
		WritablePVCMount: true,
	}

	pod := toolProbePod(
		"registry.example/tool:test",
		"operation",
		target,
		readyProbeNode("node-a"),
		time.Minute,
	)
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].PersistentVolumeClaim == nil ||
		pod.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly {
		t.Fatalf("probe PVC volume=%#v", pod.Spec.Volumes)
	}

	if len(pod.Spec.Containers[0].VolumeMounts) != 1 ||
		pod.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatalf("probe PVC mount=%#v", pod.Spec.Containers[0].VolumeMounts)
	}

	if strings.Contains(pod.Spec.Containers[0].Command[2], "/probe-volume") {
		t.Fatalf(
			"probe command writes or otherwise references the mounted PVC: %q",
			pod.Spec.Containers[0].Command[2],
		)
	}
}

func TestTransferPathProbeCreatesNestedDestinationAndRejectsSymlinks(t *testing.T) {
	root := t.TempDir()
	relative := "tenant data/current's files"

	command := strings.Replace(
		transferPathProbeCommand(relative, true),
		"current=/probe-volume",
		"current="+probeShellQuote(root),
		1,
	)
	if output, err := exec.CommandContext(t.Context(), "sh", "-c", command).
		CombinedOutput(); err != nil {
		t.Fatalf("create path: %v: %s", err, output)
	}

	if info, err := os.Stat(
		filepath.Join(root, "tenant data", "current's files"),
	); err != nil ||
		!info.IsDir() {
		t.Fatalf("created directory info=%v error=%v", info, err)
	}

	escape := t.TempDir()
	if err := os.Symlink(escape, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	command = strings.Replace(
		transferPathProbeCommand("link/child", true),
		"current=/probe-volume",
		"current="+probeShellQuote(root),
		1,
	)

	output, err := exec.CommandContext(t.Context(), "sh", "-c", command).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "symbolic link") {
		t.Fatalf("symlink path error=%v output=%q", err, output)
	}

	if _, err := os.Stat(filepath.Join(escape, "child")); !os.IsNotExist(err) {
		t.Fatalf("path probe wrote through symlink: %v", err)
	}
}

func TestTransferPathProbeRequiresExistingSourceDirectory(t *testing.T) {
	root := t.TempDir()
	command := strings.Replace(
		transferPathProbeCommand("missing/source", false),
		"current=/probe-volume",
		"current="+probeShellQuote(root),
		1,
	)

	output, err := exec.CommandContext(t.Context(), "sh", "-c", command).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "source directory does not exist") {
		t.Fatalf("missing source error=%v output=%q", err, output)
	}
}

func TestNormalizeToolProbeTargetMakesPathCreationWritable(t *testing.T) {
	targets, err := normalizeToolProbeTargets([]ToolProbeTarget{
		{
			Namespace:    "system",
			NodeName:     "node-a",
			PVCName:      "data",
			RequiredPath: "nested//data/",
			CreatePath:   true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(targets) != 1 || targets[0].RequiredPath != "nested/data" ||
		!targets[0].WritablePVCMount ||
		targets[0].SkipPVCMount {
		t.Fatalf("targets=%#v", targets)
	}

	pod := toolProbePod(
		"registry.example/tool:test",
		"operation",
		targets[0],
		readyProbeNode("node-a"),
		time.Minute,
	)
	if pod.Annotations[MetadataDomain+"/probe-path"] != "nested/data" ||
		!strings.Contains(pod.Spec.Containers[0].Command[2], "transfer_path='nested/data'") {
		t.Fatalf("path probe Pod=%#v command=%q", pod.Annotations, pod.Spec.Containers[0].Command)
	}

	for _, target := range []ToolProbeTarget{
		{Namespace: "system", RequiredPath: "data"},
		{Namespace: "system", PVCName: "data", SkipPVCMount: true, RequiredPath: "data"},
		{Namespace: "system", PVCName: "data", RequiredPath: ".", CreatePath: true},
	} {
		if _, err := normalizeToolProbeTargets(
			[]ToolProbeTarget{target},
		); domain.CategoryOf(
			err,
		) != domain.ErrorValidation {
			t.Fatalf("target=%#v error=%v category=%s", target, err, domain.CategoryOf(err))
		}
	}
}

func TestNormalizeToolProbeTargetsPreservesStrictMountRequirements(t *testing.T) {
	targets, err := normalizeToolProbeTargets([]ToolProbeTarget{
		{
			Namespace:    "system",
			NodeName:     "node-a",
			PVCName:      "data",
			SkipPVCMount: true,
			Components:   []string{ToolComponentRsync},
		},
		{
			Namespace:        "system",
			NodeName:         "node-a",
			PVCName:          "data",
			WritablePVCMount: true,
			Components:       []string{ToolComponentSSHD},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(targets) != 1 || targets[0].SkipPVCMount || !targets[0].WritablePVCMount ||
		!slices.Equal(
			targets[0].Components,
			[]string{ToolComponentRsync, ToolComponentShell, ToolComponentSSHD},
		) {
		t.Fatalf("normalized targets=%#v", targets)
	}
}

func TestSSHDToolProbeChecksRemoteRsyncAndBothHostKeyAlgorithms(t *testing.T) {
	command := toolProbeCommand([]string{ToolComponentSSHD})
	for _, required := range []string{
		"command -v rsync", "rsync --version",
		"ssh-keygen -q -t ed25519", "ssh-keygen -q -t ecdsa",
		"-o HostKey=/tmp/pvc-migrate-probe-host-key -o HostKey=/tmp/pvc-migrate-probe-host-key-ecdsa",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("SSHD probe command lacks %q: %s", required, command)
		}
	}
}

func TestToolProbeCommandCoversRcloneRuntime(t *testing.T) {
	command := toolProbeCommand([]string{ToolComponentShell, ToolComponentRclone})
	for _, fragment := range []string{
		"command -v sh", "command -v sleep", "command -v test",
		"command -v rclone", "command -v base64", "rclone version",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("probe command lacks %q: %s", fragment, command)
		}
	}
}

func TestToolImageProbeSerializesMultiNodeOutput(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"), readyProbeNode("node-b"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID(pod.Name)
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", successfulProbeGetReactor(t, client))

	var output bytes.Buffer

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image: "registry.example/pvc-migrate:test",
		Targets: []ToolProbeTarget{
			{Namespace: "system", NodeName: "node-a", Components: []string{ToolComponentRsync}},
			{Namespace: "system", NodeName: "node-b", Components: []string{ToolComponentSSHD}},
		},
		Timeout: time.Second,
		Poll:    time.Millisecond,
		Writer:  &output,
	})
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 8 || !strings.Contains(output.String(), "node=node-a") ||
		!strings.Contains(output.String(), "node=node-b") ||
		strings.Count(output.String(), "tool image probe succeeded") != 2 ||
		strings.Count(output.String(), "tool image probe cleanup started") != 2 {
		t.Fatalf("probe output=%q", output.String())
	}
}

func TestToolImageProbeIncludesPodCreationCause(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				corev1.Resource("pods"),
				"probe",
				errors.New("restricted policy requires non-root"),
			)
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "restricted policy requires non-root") {
		t.Fatalf("probe category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolImageProbeLogsStartBeforeCreateFailure(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				corev1.Resource("pods"),
				"probe",
				errors.New("quota exceeded"),
			)
		},
	)

	var output bytes.Buffer

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Writer:  &output,
	})
	if err == nil || !strings.Contains(output.String(), "tool image probe started") {
		t.Fatalf("error=%v output=%q", err, output.String())
	}
}

func TestToolProbePodUsesRequestedDeadlineAndSafeOperationMetadata(t *testing.T) {
	target := ToolProbeTarget{Namespace: "system", Components: []string{ToolComponentRclone}}

	pod := toolProbePod(
		"registry.example/tool:test",
		"UPPER/operation/with/slashes",
		target,
		nil,
		1500*time.Millisecond,
	)
	if pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds != 2 {
		t.Fatalf("activeDeadlineSeconds=%v", pod.Spec.ActiveDeadlineSeconds)
	}

	if _, exists := pod.Labels[SessionKey]; exists {
		t.Fatalf("invalid operation ID was copied into label: %v", pod.Labels)
	}

	if pod.Annotations[toolProbeOperation] != "UPPER/operation/with/slashes" {
		t.Fatalf("probe operation annotation=%q", pod.Annotations[toolProbeOperation])
	}
}

func TestToolImageProbeReportsSchedulerSelectedNode(t *testing.T) {
	client := fake.NewClientset()

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("probe-uid")
			created = pod.DeepCopy()
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Spec.NodeName = "scheduled-node"
			pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-pull"}}
			pod.Status.Phase = corev1.PodSucceeded

			return true, pod, nil
		},
	)

	var output bytes.Buffer

	results, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", PVCName: "source-data"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
		Writer:  &output,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 || results[0].NodeName != "scheduled-node" ||
		!slices.Equal(
			results[0].ImagePullSecrets,
			[]corev1.LocalObjectReference{{Name: "registry-pull"}},
		) {
		t.Fatalf("probe results=%#v", results)
	}

	if !strings.Contains(output.String(), "node=scheduled-node") {
		t.Fatalf("probe output=%q", output.String())
	}

	if created == nil || len(created.Spec.Volumes) != 1 ||
		created.Spec.Volumes[0].PersistentVolumeClaim == nil ||
		created.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != "source-data" ||
		!created.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly {
		t.Fatalf("probe volumes=%#v", created)
	}

	container := created.Spec.Containers[0]
	if len(container.VolumeMounts) != 1 || container.VolumeMounts[0].MountPath != "/probe-volume" ||
		!container.VolumeMounts[0].ReadOnly {
		t.Fatalf("probe volume mounts=%#v", container.VolumeMounts)
	}

	if created.Annotations[MetadataDomain+"/probe-pvc"] != "source-data" {
		t.Fatalf("probe annotations=%v", created.Annotations)
	}
}

func TestToolImagePullSecretHelmValuesMergeByComponent(t *testing.T) {
	results := []ToolImageProbeResult{
		{
			Target: ToolProbeTarget{
				Namespace:  "source",
				Components: []string{ToolComponentSSHD},
			},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "a-pull"}, {Name: "shared"}},
		},
		{
			Target: ToolProbeTarget{
				Namespace:  "source",
				Components: []string{ToolComponentSSHD, ToolComponentRsync},
			},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "shared"}, {Name: "a-pull"}},
		},
		{
			Target: ToolProbeTarget{
				Namespace:  "backup",
				Components: []string{ToolComponentRclone},
			},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "backup-pull"}},
		},
	}
	want := []string{
		"rsync.imagePullSecrets[0].name=a-pull",
		"rsync.imagePullSecrets[1].name=shared",
		"sshd.imagePullSecrets[0].name=a-pull",
		"sshd.imagePullSecrets[1].name=shared",
		"rclone.imagePullSecrets[0].name=backup-pull",
	}

	got, err := ToolImagePullSecretHelmValues(results)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("values=%v want=%v", got, want)
	}
}

func TestToolImagePullSecretHelmValuesRejectsNamespaceMismatch(t *testing.T) {
	results := []ToolImageProbeResult{
		{
			Target: ToolProbeTarget{
				Namespace:  "source",
				Components: []string{ToolComponentSSHD},
			},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "source-pull"}},
		},
		{
			Target: ToolProbeTarget{
				Namespace:  "destination",
				Components: []string{ToolComponentSSHD},
			},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "destination-pull"}},
		},
	}
	if _, err := ToolImagePullSecretHelmValues(
		results,
	); domain.CategoryOf(
		err,
	) != domain.ErrorPrecondition {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolImageProbeReportsImagePullFailureAndCleansPod(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("failed-probe-uid")
			created = pod.DeepCopy()
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodPending
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{
				{
					Name: "tool-probe",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "authentication required",
						},
					},
				},
			}

			return true, pod, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image: "registry.example/private/tool:test",
		Targets: []ToolProbeTarget{{
			Namespace: "system", NodeName: "node-a", Components: []string{ToolComponentRsync},
		}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "ImagePullBackOff") ||
		!strings.Contains(err.Error(), "authentication required") ||
		!strings.Contains(err.Error(), "node-a") {
		t.Fatalf("probe category=%s error=%v", domain.CategoryOf(err), err)
	}

	if created == nil {
		t.Fatal("probe Pod was not created")
	}

	if _, err := client.CoreV1().
		Pods("system").
		Get(context.Background(), created.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("failed probe Pod still exists: %v", err)
	}
}

func TestToolImageProbeWaitsForPodDeletion(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	deletionRequested := false
	cleanupGets := 0

	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("delayed-delete-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"delete",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			deletionRequested = true
			return true, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			if deletionRequested {
				cleanupGets++
				if cleanupGets == 3 {
					if err := client.Tracker().
						Delete(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName()); err != nil {
						return true, nil, err
					}

					return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), get.GetName())
				}
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodSucceeded

			return true, pod, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	if cleanupGets != 3 {
		t.Fatalf("cleanup GET calls=%d", cleanupGets)
	}
}

func TestCleanupSessionToolProbePodsDeletesOnlyMatchingSessionProbes(t *testing.T) {
	owned := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "source",
		Name:      "owned-probe",
		UID:       "owned-uid",
		Labels: map[string]string{
			ManagedByLabel:    ManagedByValue,
			SessionKey:        "session-1",
			ResourceRoleLabel: ResourceRoleToolProbe,
		},
	}}
	foreign := owned.DeepCopy()
	foreign.Name = "foreign-probe"
	foreign.UID = "foreign-uid"
	foreign.Labels = map[string]string{
		ManagedByLabel:    ManagedByValue,
		SessionKey:        "session-2",
		ResourceRoleLabel: ResourceRoleToolProbe,
	}
	client := fake.NewClientset(owned, foreign)

	if err := CleanupSessionToolProbePods(
		context.Background(),
		client,
		"session-1",
		[]string{"source", "source", ""},
	); err != nil {
		t.Fatal(err)
	}

	if _, err := client.CoreV1().
		Pods("source").
		Get(context.Background(), owned.Name, metav1.GetOptions{}); !apierrors.IsNotFound(
		err,
	) {
		t.Fatalf("owned probe still exists: %v", err)
	}

	if _, err := client.CoreV1().
		Pods("source").
		Get(context.Background(), foreign.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign probe was deleted: %v", err)
	}
}

func TestToolImageProbeCleanupRejectsReplacementPod(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))

	var createdName string
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("original-probe")
			createdName = pod.Name
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", successfulProbeGetReactor(t, client))
	client.PrependReactor(
		"delete",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			deleteAction := testutil.MustType[clienttesting.DeleteAction](t, action)

			resource := corev1.SchemeGroupVersion.WithResource("pods")
			if err := client.Tracker().
				Delete(resource, deleteAction.GetNamespace(), deleteAction.GetName()); err != nil {
				return true, nil, err
			}

			replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: deleteAction.GetNamespace(),
				Name:      deleteAction.GetName(),
				UID:       types.UID("replacement-probe"),
			}}
			if err := client.Tracker().Add(replacement); err != nil {
				return true, nil, err
			}

			return true, nil, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced while waiting for probe cleanup") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	pod, getErr := client.CoreV1().
		Pods("system").
		Get(context.Background(), createdName, metav1.GetOptions{})
	if getErr != nil || pod.UID != types.UID("replacement-probe") {
		t.Fatalf("replacement Pod=%#v error=%v", pod, getErr)
	}
}

func TestToolImageProbeCancelsPendingSiblingAfterFailure(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"), readyProbeNode("node-b"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID(pod.Name)
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()

			pod.Status.Phase = corev1.PodPending
			if pod.Spec.NodeSelector[corev1.LabelHostname] == "node-a" {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: "tool-probe",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
						Reason: "ImagePullBackOff", Message: "authentication required",
					}},
				}}
			}

			return true, pod, nil
		},
	)

	started := time.Now()

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image: "registry.example/private/tool:test",
		Targets: []ToolProbeTarget{
			{Namespace: "system", NodeName: "node-a"},
			{Namespace: "system", NodeName: "node-b"},
		},
		Timeout: 2 * time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "ImagePullBackOff") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
		t.Fatalf("sibling cancellation took %s", elapsed)
	}
}

func TestToolProbeFailureDeduplicatesPodAndContainerDetails(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Reason:  "ErrImagePull",
		Message: "authentication required",
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "ErrImagePull", Message: "authentication required",
			}},
		}},
	}}

	err := toolProbePodFailureWithMessage(
		pod,
		"registry.example/private/tool:test",
		ToolProbeTarget{
			Namespace:  "system",
			NodeName:   "node-a",
			Components: []string{ToolComponentRsync},
		},
		"ErrImagePull",
		"authentication required",
	)
	if strings.Count(err.Error(), "ErrImagePull") != 1 ||
		strings.Count(err.Error(), "authentication required") != 1 {
		t.Fatalf("probe error contains duplicate details: %v", err)
	}
}

func TestToolProbeFailureClassifiesOnlyReportedTransferPathErrors(t *testing.T) {
	target := ToolProbeTarget{
		Namespace: "system", NodeName: "node-a", PVCName: "data",
		RequiredPath: "mysql/current", Components: []string{ToolComponentRsync},
	}
	imageFailure := &corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
				},
			},
		}},
	}
	err := toolProbePodFailureWithMessage(
		imageFailure,
		"registry.example/private/tool:test",
		target,
		"ImagePullBackOff",
		"authentication required",
	)

	var typed *domain.Error
	if !errors.As(err, &typed) || typed.Op != "tool image probe" {
		t.Fatalf("image failure error=%v", err)
	}

	pathFailure := &corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 66,
				Message:  transferPathFailureMarker + " transfer source directory does not exist: mysql/current",
			}},
		}}},
	}
	err = toolProbePodFailureWithMessage(
		pathFailure,
		"registry.example/tool:test",
		target,
		"Error",
		"",
	)

	typed = nil
	if !errors.As(err, &typed) || typed.Op != "transfer path preflight" {
		t.Fatalf("path failure error=%v", err)
	}
}

func TestToolImageProbeReportsCommandFailure(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("failed-command-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodFailed
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "tool-probe",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 127, Reason: "Error", Message: "rclone: not found",
				}},
			}}

			return true, pod, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image: "registry.example/tool:test",
		Targets: []ToolProbeTarget{{
			Namespace: "system", NodeName: "node-a", Components: []string{ToolComponentRclone},
		}},
		Timeout: time.Second, Poll: time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "rclone: not found") ||
		!strings.Contains(err.Error(), "exit code 127") {
		t.Fatalf("probe category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolImageProbeReportsUnschedulablePod(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("unschedulable-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodPending
			pod.Status.Conditions = []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
				Reason: "Unschedulable", Message: "no nodes satisfy policy",
			}}

			return true, pod, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image: "registry.example/tool:test", Targets: []ToolProbeTarget{{Namespace: "system"}},
		Timeout: time.Second, Poll: time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "Unschedulable") ||
		!strings.Contains(err.Error(), "no nodes satisfy policy") {
		t.Fatalf("probe category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolImageProbeCleansPodAfterCancellation(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	ctx, cancel := context.WithCancel(context.Background())

	var createdName string
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("canceled-probe")
			createdName = pod.Name

			cancel()

			return false, nil, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(ctx, ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) ||
		!strings.Contains(err.Error(), "canceled while waiting for tool image probe Pod") {
		t.Fatalf("probe category=%s error=%v", domain.CategoryOf(err), err)
	}

	if _, getErr := client.CoreV1().
		Pods("system").
		Get(context.Background(), createdName, metav1.GetOptions{}); !apierrors.IsNotFound(
		getErr,
	) {
		t.Fatalf("canceled probe Pod still exists: %v", getErr)
	}
}

func TestToolImageProbeCancellationDoesNotAddTimeoutDiagnostics(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	ctx, cancel := context.WithCancel(context.Background())

	var createdName string
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("canceled-diagnostics-probe")
			createdName = pod.Name
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			cancel()
			return false, nil, context.Canceled
		},
	)

	_, err := NewToolImageProber(client).Probe(ctx, ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) ||
		!strings.Contains(err.Error(), "canceled while waiting for tool image probe Pod") ||
		strings.Contains(err.Error(), "did not complete") {
		t.Fatalf("probe cancellation error=%v", err)
	}

	if _, getErr := client.CoreV1().
		Pods("system").
		Get(context.Background(), createdName, metav1.GetOptions{}); !apierrors.IsNotFound(
		getErr,
	) {
		t.Fatalf("canceled probe Pod still exists: %v", getErr)
	}
}

func TestToolImageProbeCleanupTimeoutExplainsUnconfirmedDeletion(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("stuck-cleanup-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", successfulProbeGetReactor(t, client))
	client.PrependReactor(
		"delete",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    10 * time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorTimeout ||
		!strings.Contains(err.Error(), "deletion was not confirmed") ||
		!strings.Contains(err.Error(), "inspect with kubectl") {
		t.Fatalf("cleanup timeout error=%v", err)
	}
}

func TestToolImageProbeCancellationKeepsCleanupWarningSeparate(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	ctx, cancel := context.WithCancel(context.Background())

	var (
		createdName string
		output      strings.Builder
	)

	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("canceled-stuck-cleanup-probe")
			createdName = pod.Name
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)
			if get.GetName() == createdName {
				cancel()
			}

			return false, nil, nil
		},
	)
	client.PrependReactor(
		"delete",
		"pods",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(ctx, ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    10 * time.Millisecond,
		Writer:  &output,
	})
	if domain.CategoryOf(err) != domain.ErrorTimeout || !errors.Is(err, context.Canceled) ||
		!strings.Contains(err.Error(), "canceled while waiting for tool image probe Pod") {
		t.Fatalf("probe cancellation error=%v", err)
	}

	if strings.Contains(err.Error(), "cleanup was not confirmed") ||
		!strings.Contains(output.String(), "warning: tool image probe cleanup was not confirmed") ||
		!strings.Contains(output.String(), createdName) {
		t.Fatalf("cleanup warning output=%q error=%v", output.String(), err)
	}
}

func TestToolImageProbeFailsImmediatelyWhenPodDisappears(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("deleted-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)
			return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), get.GetName())
		},
	)

	started := time.Now()

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "disappeared before completion") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("disappeared Pod detection took %s", elapsed)
	}
}

func TestToolImageProbeRejectsReplacementBeforeCompletion(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))

	var created *corev1.Pod
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			created = testutil.MustActionObject[*corev1.Pod](t, action)
			created.UID = "created-probe"
			return false, nil, nil
		},
	)
	client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if created == nil {
			return false, nil, nil
		}

		replacement := created.DeepCopy()
		replacement.UID = "replacement-probe"
		replacement.Status.Phase = corev1.PodSucceeded

		return true, replacement, nil
	})

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
		Timeout: time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorConflict ||
		!strings.Contains(err.Error(), "replaced before completion") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolImageProbeTimeoutIncludesPodEvents(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	listCalls := 0

	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("pending-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodPending

			return true, pod, nil
		},
	)
	// Match the generated Pod name while retaining the UID ownership check.
	client.PrependReactor(
		"list",
		"events",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			listCalls++
			selector := testutil.MustType[clienttesting.ListAction](t, action).
				GetListRestrictions().Fields.String()
			name := strings.TrimPrefix(selector, "involvedObject.name=")

			return true, &corev1.EventList{Items: []corev1.Event{{
				InvolvedObject: corev1.ObjectReference{Name: name, UID: types.UID("pending-probe")},
				Reason:         "FailedMount",
				Message:        "unable to attach or mount volumes",
			}}}, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a", PVCName: "data"}},
		Timeout: 20 * time.Millisecond,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorTimeout ||
		!strings.Contains(err.Error(), "FailedMount: unable to attach or mount volumes") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if listCalls < 1 || listCalls > 2 {
		t.Fatalf(
			"event list calls=%d, want at most one poll check plus timeout diagnostics",
			listCalls,
		)
	}
}

func TestToolImageProbeFailsEarlyOnConcurrentMountEvent(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("concurrent-mount-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodPending

			return true, pod, nil
		},
	)

	listCalls := 0
	client.PrependReactor(
		"list",
		"events",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			listCalls++
			selector := testutil.MustType[clienttesting.ListAction](t, action).
				GetListRestrictions().Fields.String()
			name := strings.TrimPrefix(selector, "involvedObject.name=")

			return true, &corev1.EventList{Items: []corev1.Event{
				{
					InvolvedObject: corev1.ObjectReference{
						Name: name,
						UID:  types.UID("concurrent-mount-probe"),
					},
					Reason:  "FailedMount",
					Message: "mount failed: device already mounted on another Pod path",
				},
			}}, nil
		},
	)

	started := time.Now()

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a", PVCName: "data"}},
		Timeout: 2 * time.Second,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "FailedMount") ||
		!strings.Contains(err.Error(), "already mounted") {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}

	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("concurrent mount detection took %s", elapsed)
	}

	if listCalls != 1 {
		t.Fatalf("event list calls=%d, want 1", listCalls)
	}
}

func TestToolImageProbeIgnoresMountEventsWhenPVCMountIsSkipped(t *testing.T) {
	client := fake.NewClientset(readyProbeNode("node-a"))
	client.PrependReactor(
		"create",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			pod := testutil.MustActionObject[*corev1.Pod](t, action)
			pod.UID = types.UID("skipped-mount-probe")
			return false, nil, nil
		},
	)
	client.PrependReactor(
		"get",
		"pods",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			get := testutil.MustType[clienttesting.GetAction](t, action)

			object, err := client.Tracker().
				Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
			if err != nil {
				return false, nil, nil
			}

			pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
			pod.Status.Phase = corev1.PodPending

			return true, pod, nil
		},
	)
	client.PrependReactor(
		"list",
		"events",
		func(action clienttesting.Action) (bool, runtime.Object, error) {
			selector := testutil.MustType[clienttesting.ListAction](t, action).
				GetListRestrictions().Fields.String()
			name := strings.TrimPrefix(selector, "involvedObject.name=")

			return true, &corev1.EventList{Items: []corev1.Event{
				{
					InvolvedObject: corev1.ObjectReference{
						Name: name,
						UID:  types.UID("skipped-mount-probe"),
					},
					Reason:  "FailedMount",
					Message: "device already mounted on another Pod path",
				},
			}}, nil
		},
	)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image: "registry.example/tool:test",
		Targets: []ToolProbeTarget{
			{Namespace: "system", NodeName: "node-a", PVCName: "data", SkipPVCMount: true},
		},
		Timeout: 20 * time.Millisecond,
		Poll:    time.Millisecond,
	})
	if domain.CategoryOf(err) != domain.ErrorTimeout {
		t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
	}
}

func TestToolImageProbeRejectsUnschedulableNodeBeforePodCreation(t *testing.T) {
	node := readyProbeNode("node-a")
	node.Spec.Unschedulable = true
	client := fake.NewClientset(node)

	_, err := NewToolImageProber(client).Probe(context.Background(), ToolImageProbeOptions{
		Image:   "registry.example/tool:test",
		Targets: []ToolProbeTarget{{Namespace: "system", NodeName: "node-a"}},
	})
	if domain.CategoryOf(err) != domain.ErrorPrecondition ||
		!strings.Contains(err.Error(), "Ready and schedulable") {
		t.Fatalf("probe category=%s error=%v", domain.CategoryOf(err), err)
	}

	pods, listErr := client.CoreV1().Pods("system").List(context.Background(), metav1.ListOptions{})
	if listErr != nil || len(pods.Items) != 0 {
		t.Fatalf("probe Pods=%v error=%v", pods.Items, listErr)
	}
}

func TestToolImageProbeValidatesTargets(t *testing.T) {
	for _, test := range []struct {
		name   string
		target ToolProbeTarget
		want   string
	}{
		{name: "namespace", target: ToolProbeTarget{Components: []string{ToolComponentRsync}}, want: "namespace is required"},
		{name: "component", target: ToolProbeTarget{Namespace: "system", Components: []string{"curl"}}, want: "unsupported tool component"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewToolImageProber(
				fake.NewClientset(),
			).Probe(context.Background(), ToolImageProbeOptions{
				Image: "registry.example/tool:test", Targets: []ToolProbeTarget{test.target},
			})
			if domain.CategoryOf(err) != domain.ErrorValidation ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("category=%s error=%v", domain.CategoryOf(err), err)
			}
		})
	}
}

func successfulProbeGetReactor(t *testing.T, client *fake.Clientset) clienttesting.ReactionFunc {
	t.Helper()

	return func(action clienttesting.Action) (bool, runtime.Object, error) {
		get := testutil.MustType[clienttesting.GetAction](t, action)

		object, err := client.Tracker().
			Get(corev1.SchemeGroupVersion.WithResource("pods"), get.GetNamespace(), get.GetName())
		if err != nil {
			return false, nil, nil
		}

		pod := testutil.MustType[*corev1.Pod](t, object).DeepCopy()
		pod.Status.Phase = corev1.PodSucceeded

		return true, pod, nil
	}
}

func readyProbeNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{corev1.LabelHostname: name},
		},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
			Key: "storage", Value: "local", Effect: corev1.TaintEffectNoSchedule,
		}}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		}}},
	}
}

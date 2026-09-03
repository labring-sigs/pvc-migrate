package kube

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	"github.com/utkuozdemir/pv-migrate/pvmigrate"
	"helm.sh/helm/v4/pkg/chart/common"
	commonutil "helm.sh/helm/v4/pkg/chart/common/util"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	chartloader "helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/cli/values"
	"helm.sh/helm/v4/pkg/engine"
	"helm.sh/helm/v4/pkg/getter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const upstreamModule = "github.com/utkuozdemir/pv-migrate"

// Keep the adapter review-driven when upstream adds or renames an option. A
// newly added field can otherwise compile cleanly and silently retain its
// zero value in our request builders.
func TestUpstreamRequestShapeRequiresAdapterReview(t *testing.T) {
	assertUpstreamFieldNames(t, "PVC", reflect.TypeFor[pvmigrate.PVC](), []string{
		"KubeconfigPath", "Context", "Namespace", "Name", "Path",
	})
	assertUpstreamFieldNames(t, "Migration", reflect.TypeFor[pvmigrate.Migration](), []string{
		"ID",
		"ImageTag",
		"ChartVersion",
		"Source",
		"Dest",
		"DeleteExtraneousFiles",
		"IgnoreMounted",
		"IgnoreSizes",
		"NoChown",
		"Detach",
		"Push",
		"NoCleanup",
		"NoCleanupOnFailure",
		"ShowProgressBar",
		"SourceMountReadWrite",
		"NoCompress",
		"NonRoot",
		"RsyncExtraArgs",
		"KeyAlgorithm",
		"SSHReverseTunnelPort",
		"Strategies",
		"DestHostOverride",
		"HelmTimeout",
		"LoadBalancerTimeout",
		"HelmValuesFiles",
		"HelmValues",
		"HelmFileValues",
		"HelmStringValues",
		"Writer",
		"Logger",
		"StructuredLogs",
		"ColorOutput",
	})
	assertUpstreamFieldNames(t, "Backup", reflect.TypeFor[pvmigrate.Backup](), []string{
		"ID",
		"ImageTag",
		"ChartVersion",
		"PVC",
		"Backend",
		"Bucket",
		"S3Provider",
		"Endpoint",
		"Region",
		"AccessKey",
		"SecretKey",
		"StorageAccount",
		"StorageKey",
		"GCSServiceAccountJSON",
		"GCSBucketPolicyOnly",
		"Name",
		"Prefix",
		"Path",
		"RcloneConfigFile",
		"Remote",
		"RcloneExtraArgs",
		"IgnoreMounted",
		"NonRoot",
		"Detach",
		"NoCleanup",
		"NoCleanupOnFailure",
		"HelmTimeout",
		"HelmValuesFiles",
		"HelmValues",
		"HelmFileValues",
		"HelmStringValues",
		"Writer",
		"Logger",
		"StructuredLogs",
		"ColorOutput",
	})
	assertUpstreamFieldNames(t, "Restore", reflect.TypeFor[pvmigrate.Restore](), []string{
		"ID",
		"ImageTag",
		"ChartVersion",
		"PVC",
		"Backend",
		"Bucket",
		"S3Provider",
		"Endpoint",
		"Region",
		"AccessKey",
		"SecretKey",
		"StorageAccount",
		"StorageKey",
		"GCSServiceAccountJSON",
		"GCSBucketPolicyOnly",
		"Name",
		"Prefix",
		"Path",
		"RcloneConfigFile",
		"Remote",
		"RcloneExtraArgs",
		"DeleteExtraneousFiles",
		"IgnoreMounted",
		"NonRoot",
		"Detach",
		"NoCleanup",
		"NoCleanupOnFailure",
		"HelmTimeout",
		"HelmValuesFiles",
		"HelmValues",
		"HelmFileValues",
		"HelmStringValues",
		"Writer",
		"Logger",
		"StructuredLogs",
		"ColorOutput",
	})
}

func assertUpstreamFieldNames(t *testing.T, typeName string, typ reflect.Type, want []string) {
	t.Helper()

	got := make([]string, typ.NumField())
	for index := range got {
		got[index] = typ.Field(index).Name
	}

	slices.Sort(got)
	slices.Sort(want)

	if !slices.Equal(got, want) {
		t.Fatalf(
			"upstream %s fields changed: got=%v want=%v; inspect adapter mappings before upgrading",
			typeName,
			got,
			want,
		)
	}
}

// These tests render the chart and inspect Dockerfiles from the resolved module
// itself. An upstream upgrade therefore changes the contract under test and
// fails here before a tool reaches a cluster with a different shape.
func TestUpstreamChartToolContract(t *testing.T) {
	chart := loadUpstreamChart(t)
	assertUpstreamChartComponents(t, chart.Values)

	image := "registry.example/team/pvc-migrate:contract"

	imageValues, err := ToolImageHelmValues(image)
	if err != nil {
		t.Fatal(err)
	}

	pullSecretResults := make([]ToolImageProbeResult, 0, 3)
	for _, component := range []string{ToolComponentRsync, ToolComponentSSHD, ToolComponentRclone} {
		pullSecretResults = append(pullSecretResults, ToolImageProbeResult{
			Target:           ToolProbeTarget{Components: []string{component}},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: component + "-registry"}},
		})
	}

	toolNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "tool-node"},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{
			Key: "storage", Value: "true", Effect: corev1.TaintEffectNoSchedule,
		}}},
	}

	schedulingValues, err := ToolComponentNodeHelmValues(ToolComponentRclone, toolNode)
	if err != nil {
		t.Fatal(err)
	}

	schedulingValues = append(schedulingValues,
		"rsync.nodeSelector.kubernetes\\.io/hostname=target-host",
		"sshd.nodeSelector.kubernetes\\.io/hostname=source-host",
	)
	schedulingValues = append(
		schedulingValues,
		ToolComponentTolerationHelmValues(ToolComponentRsync, toolNode)...,
	)
	schedulingValues = append(
		schedulingValues,
		ToolComponentTolerationHelmValues(ToolComponentSSHD, toolNode)...,
	)
	identityValues := upstreamContractIdentityValues(t)
	schedulingValues = append(schedulingValues, identityValues.StringValues...)

	pullSecretValues, err := ToolImagePullSecretHelmValues(pullSecretResults)
	if err != nil {
		t.Fatal(err)
	}

	options := values.Options{
		Values: append(
			ToolSecurityContextHelmValues(),
			identityValues.Values...,
		),
		StringValues: append(
			append(append(imageValues, ZeroResourceHelmValues()...), pullSecretValues...),
			schedulingValues...,
		),
	}

	overrides, err := options.MergeValues(getter.All(cli.New()))
	if err != nil {
		t.Fatal(err)
	}

	configureUpstreamContractWorkloads(t, overrides)

	renderValues, err := commonutil.ToRenderValues(chart, overrides, common.ReleaseOptions{
		Name:      "pv-migrate-contract",
		Namespace: "contract",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	files, err := engine.Render(chart, renderValues)
	if err != nil {
		t.Fatal(err)
	}

	assertNoServiceAccountsRendered(t, files)

	containers := upstreamToolContainers(t, files)

	podSpecs := upstreamToolPodSpecs(t, files)
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		container, ok := containers[component]
		if !ok {
			rendered := make([]string, 0, len(containers))
			for name := range containers {
				rendered = append(rendered, name)
			}

			slices.Sort(rendered)
			t.Fatalf("upstream chart did not render %s tool; rendered=%v", component, rendered)
		}

		if container.Image != image || container.ImagePullPolicy != corev1.PullIfNotPresent {
			t.Fatalf(
				"%s image=%q pullPolicy=%q",
				component,
				container.Image,
				container.ImagePullPolicy,
			)
		}

		if len(container.Command) != 3 || container.Command[0] != "sh" ||
			container.Command[1] != "-c" ||
			strings.TrimSpace(container.Command[2]) == "" {
			t.Fatalf("%s command=%q", component, container.Command)
		}

		assertRootToolSecurityContext(t, component, container.SecurityContext)
		assertZeroToolResources(t, component, container.Resources)

		if secrets := podSpecs[component].ImagePullSecrets; !slices.Equal(
			secrets,
			[]corev1.LocalObjectReference{{Name: component + "-registry"}},
		) {
			t.Fatalf("%s imagePullSecrets=%v", component, secrets)
		}

		assertNoTokenServiceAccount(t, component, podSpecs[component])

		if tolerations := podSpecs[component].Tolerations; !slices.Equal(
			tolerations,
			[]corev1.Toleration{
				{
					Key:      "storage",
					Operator: corev1.TolerationOpEqual,
					Value:    "true",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		) {
			t.Fatalf("%s tolerations=%v", component, tolerations)
		}
	}

	if podSpecs[ToolComponentRclone].NodeName != "tool-node" {
		t.Fatalf("rclone nodeName=%q", podSpecs[ToolComponentRclone].NodeName)
	}

	if nodeSelector := podSpecs[ToolComponentRsync].NodeSelector; nodeSelector[corev1.LabelHostname] != "target-host" {
		t.Fatalf("rsync nodeSelector=%v", nodeSelector)
	}

	if nodeSelector := podSpecs[ToolComponentSSHD].NodeSelector; nodeSelector[corev1.LabelHostname] != "source-host" {
		t.Fatalf("sshd nodeSelector=%v", nodeSelector)
	}

	sshd := containers["sshd"]

	service := upstreamSSHDService(t, files)
	if len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 22 ||
		service.Spec.Ports[0].TargetPort.IntVal != 22 {
		t.Fatalf("sshd service ports=%v", service.Spec.Ports)
	}

	for _, fragment := range []string{"ssh-keygen -t ed25519", "ssh-keygen -t ecdsa", "/usr/sbin/sshd -D -e -f /etc/ssh/sshd_config", "HostKey=/tmp/ssh_host_ed25519_key"} {
		if !strings.Contains(sshd.Command[2], fragment) {
			t.Fatalf("sshd command lacks %q: %s", fragment, sshd.Command[2])
		}
	}

	if !slices.Contains(sshd.SecurityContext.Capabilities.Add, corev1.Capability("SYS_CHROOT")) {
		t.Fatalf("sshd capabilities=%v", sshd.SecurityContext.Capabilities)
	}

	if !strings.Contains(
		containers["rsync"].Command[2],
		"rsync -aHAXS --numeric-ids /source/ /dest/",
	) {
		t.Fatalf("rsync command=%s", containers["rsync"].Command[2])
	}

	if !strings.Contains(containers["rclone"].Command[2], "rclone sync /data remote:bucket/") {
		t.Fatalf("rclone command=%s", containers["rclone"].Command[2])
	}
}

func upstreamContractIdentityValues(t *testing.T) HelmOverrides {
	t.Helper()

	transfer := TransferServiceAccountHelmValues()

	rclone, err := ToolServiceAccountHelmValues(TransferServiceAccountName)
	if err != nil {
		t.Fatal(err)
	}

	return HelmOverrides{
		Values:       append(transfer.Values, rclone.Values...),
		StringValues: append(transfer.StringValues, rclone.StringValues...),
	}
}

func TestToolDockerfileContainsUpstreamToolRuntime(t *testing.T) {
	upstreamDir := upstreamModuleDir(t)

	required := make(map[string]struct{})
	for _, relative := range []string{
		"docker/rsync/Dockerfile",
		"docker/sshd/Dockerfile",
		"docker/rclone/Dockerfile",
	} {
		for _, packageName := range apkPackages(t, filepath.Join(upstreamDir, relative)) {
			required[packageName] = struct{}{}
		}
	}

	root := repositoryRoot(t)

	toolDockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}

	available := make(map[string]struct{})
	for _, packageName := range apkPackages(t, filepath.Join(root, "Dockerfile")) {
		available[packageName] = struct{}{}
	}

	for packageName := range required {
		if _, ok := available[packageName]; !ok {
			t.Errorf(
				"tool Dockerfile dropped upstream tool package %q; Dockerfile=%s",
				packageName,
				toolDockerfile,
			)
		}
	}

	for _, fragment := range []string{
		"sed -i -e 's/^root:!:/root:*:/",
		"addgroup -g 10000 pvmigrate",
		"adduser -D -u 10000",
		"COPY docker/sshd_config /etc/ssh/sshd_config",
		"USER 10000:10000",
	} {
		if !strings.Contains(string(toolDockerfile), fragment) {
			t.Errorf("tool Dockerfile lacks required runtime fragment %q", fragment)
		}
	}
}

func TestToolImageShellWrapperUsesTiniProcessGroup(t *testing.T) {
	root := repositoryRoot(t)

	content, err := os.ReadFile(filepath.Join(root, "docker", "sh"))
	if err != nil {
		t.Fatal(err)
	}

	shell := string(content)
	for _, fragment := range []string{
		"#!/bin/sh",
		"exec /sbin/tini -g -- /bin/busybox sh \"$@\"",
	} {
		if !strings.Contains(shell, fragment) {
			t.Errorf("docker/sh lacks signal-forwarding fragment %q: %s", fragment, shell)
		}
	}

	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(
		string(dockerfile),
		"COPY --link --chmod=0755 docker/sh /usr/local/bin/sh",
	) {
		t.Fatalf("Dockerfile does not install the signal-forwarding sh wrapper")
	}

	if !strings.Contains(
		string(dockerfile),
		"ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	) {
		t.Fatalf("Dockerfile does not keep /usr/local/bin ahead of /bin in PATH")
	}
}

func TestToolDockerfileTracksUpstreamSSHDConfig(t *testing.T) {
	upstreamConfig, err := os.ReadFile(
		filepath.Join(upstreamModuleDir(t), "docker", "sshd", "sshd_config"),
	)
	if err != nil {
		t.Fatal(err)
	}

	localConfig, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docker", "sshd_config"))
	if err != nil {
		t.Fatal(err)
	}

	if string(localConfig) != string(upstreamConfig) {
		t.Fatalf(
			"tool image sshd_config diverges from upstream:\nlocal:\n%s\nupstream:\n%s",
			localConfig,
			upstreamConfig,
		)
	}
}

func TestToolDockerfileTracksUpstreamAlpineBase(t *testing.T) {
	upstreamDir := upstreamModuleDir(t)

	versions := make(map[string]struct{})
	for _, relative := range []string{
		"docker/rsync/Dockerfile",
		"docker/sshd/Dockerfile",
		"docker/rclone/Dockerfile",
	} {
		content, err := os.ReadFile(filepath.Join(upstreamDir, relative))
		if err != nil {
			t.Fatal(err)
		}

		version := alpineBaseVersion(string(content))
		if version == "" {
			t.Fatalf("%s does not declare an alpine base image", relative)
		}

		versions[version] = struct{}{}
	}

	if len(versions) != 1 {
		t.Fatalf("upstream tool Dockerfiles use different Alpine versions: %v", versions)
	}

	var upstreamVersion string
	for version := range versions {
		upstreamVersion = version
	}

	local, err := os.ReadFile(filepath.Join(repositoryRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(local), "ARG ALPINE_VERSION="+upstreamVersion) {
		t.Fatalf("tool Dockerfile Alpine version differs from upstream: want %s", upstreamVersion)
	}
}

func TestToolDockerfileDefaultsMissingVersionMetadata(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(repositoryRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}

	dockerfile := string(content)
	if strings.Count(dockerfile, "${BUILD_VERSION:-dev}") != 2 {
		t.Fatalf(
			"Dockerfile must normalize an empty VERSION for validation and build, content=%s",
			dockerfile,
		)
	}
}

func alpineBaseVersion(content string) string {
	for line := range strings.SplitSeq(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "FROM" && strings.HasPrefix(fields[1], "alpine:") {
			return strings.TrimPrefix(fields[1], "alpine:")
		}
	}

	return ""
}

func loadUpstreamChart(t *testing.T) *chart.Chart {
	t.Helper()
	directory := upstreamModuleDir(t)
	chartDirectory := filepath.Join(directory, "internal", "helm", "pv-migrate")

	loaded, err := chartloader.LoadDir(chartDirectory)
	if err != nil {
		t.Fatalf(
			"load upstream chart from %s: %v; inspect the dependency upgrade and update this contract",
			chartDirectory,
			err,
		)
	}

	return loaded
}

func upstreamModuleDir(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	command := exec.CommandContext(ctx, "go", "list", "-m", "-f", "{{.Dir}}", upstreamModule)

	output, err := command.Output()
	if err != nil {
		t.Fatalf("locate %s module: %v", upstreamModule, err)
	}

	return strings.TrimSpace(string(output))
}

func repositoryRoot(t *testing.T) string {
	t.Helper()

	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}

		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not locate repository go.mod")
		}

		directory = parent
	}
}

func apkPackages(t *testing.T, filename string) []string {
	t.Helper()

	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	command := string(content)
	marker := "apk add --no-cache"

	start := strings.Index(command, marker)
	if start < 0 {
		t.Fatalf("%s lacks %q", filename, marker)
	}

	command = command[start+len(marker):]
	if end := strings.Index(command, "&&"); end >= 0 {
		command = command[:end]
	}

	packages := make([]string, 0)
	for token := range strings.FieldsSeq(command) {
		token = strings.Trim(token, "\\")
		if token != "" {
			packages = append(packages, token)
		}
	}

	return packages
}

func assertUpstreamChartComponents(t *testing.T, chartValues map[string]any) {
	t.Helper()

	var imageComponents []string
	for name, raw := range chartValues {
		component, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		if _, hasImage := component["image"]; hasImage {
			imageComponents = append(imageComponents, name)
		}
	}

	slices.Sort(imageComponents)

	if !slices.Equal(imageComponents, []string{"rclone", "rsync", "sshd"}) {
		t.Fatalf(
			"upstream image components=%v; inspect whether the tool image must support new or changed roles",
			imageComponents,
		)
	}

	sshd := testutil.MustType[map[string]any](t, chartValues["sshd"])
	if sshd["containerPort"] != float64(22) && sshd["containerPort"] != 22 {
		t.Fatalf("upstream sshd.containerPort=%#v", sshd["containerPort"])
	}

	if sshd["publicKeyMountPath"] != "/root/.ssh/authorized_keys" {
		t.Fatalf("upstream sshd.publicKeyMountPath=%#v", sshd["publicKeyMountPath"])
	}
}

func configureUpstreamContractWorkloads(t *testing.T, overrides map[string]any) {
	t.Helper()

	for _, component := range []string{"rsync", "sshd", "rclone"} {
		values, ok := overrides[component].(map[string]any)
		if !ok {
			t.Fatalf("%s overrides=%#v", component, overrides[component])
		}

		values["enabled"] = true
		values["namespace"] = "contract"
		values["pvcMounts"] = []any{map[string]any{"name": "data", "mountPath": "/data"}}
	}

	sshd := testutil.MustType[map[string]any](t, overrides["sshd"])
	sshd["nodeName"] = "source-node"
	sshd["publicKeyMount"] = false
	rsync := testutil.MustType[map[string]any](t, overrides["rsync"])
	rsync["command"] = "rsync -aHAXS --numeric-ids /source/ /dest/"
	rclone := testutil.MustType[map[string]any](t, overrides["rclone"])
	rclone["command"] = "rclone sync /data remote:bucket/"
	rclone["configMount"] = false
}

func upstreamToolContainers(t *testing.T, files map[string]string) map[string]corev1.Container {
	t.Helper()

	containers := make(map[string]corev1.Container)
	for filename, content := range files {
		if strings.TrimSpace(content) == "" {
			continue
		}

		var workload struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
			Spec     struct {
				Template corev1.PodTemplateSpec `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(content), &workload); err != nil {
			t.Fatalf("parse upstream manifest %s: %v", filename, err)
		}

		component := workload.Metadata.Labels["app.kubernetes.io/component"]
		if component == "" || len(workload.Spec.Template.Spec.Containers) == 0 {
			continue
		}

		if instance := workload.Spec.Template.Labels[AppInstanceLabel]; instance == "" ||
			instance != workload.Metadata.Labels[AppInstanceLabel] {
			t.Fatalf(
				"upstream %s tool Pod instance label=%q workload label=%q; tool log discovery requires the release identity on Pods",
				component,
				instance,
				workload.Metadata.Labels[AppInstanceLabel],
			)
		}

		if workload.Spec.Template.Labels[AppComponentLabel] != component {
			t.Fatalf(
				"upstream %s tool Pod component label=%q",
				component,
				workload.Spec.Template.Labels[AppComponentLabel],
			)
		}

		if len(workload.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf(
				"upstream %s tool containers=%d",
				component,
				len(workload.Spec.Template.Spec.Containers),
			)
		}

		containers[component] = workload.Spec.Template.Spec.Containers[0]
	}

	return containers
}

func assertNoServiceAccountsRendered(t *testing.T, files map[string]string) {
	t.Helper()

	for filename, content := range files {
		if strings.TrimSpace(content) == "" {
			continue
		}

		var header struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(content), &header); err != nil {
			t.Fatalf("parse upstream manifest header %s: %v", filename, err)
		}

		if header.Kind == "ServiceAccount" {
			t.Fatalf("upstream chart unexpectedly rendered ServiceAccount %s", filename)
		}
	}
}

func upstreamToolPodSpecs(t *testing.T, files map[string]string) map[string]corev1.PodSpec {
	t.Helper()

	podSpecs := make(map[string]corev1.PodSpec)
	for filename, content := range files {
		if strings.TrimSpace(content) == "" {
			continue
		}

		var workload struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
			Spec     struct {
				Template corev1.PodTemplateSpec `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(content), &workload); err != nil {
			t.Fatalf("parse upstream manifest %s: %v", filename, err)
		}

		component := workload.Metadata.Labels[AppComponentLabel]
		if component != "" && len(workload.Spec.Template.Spec.Containers) > 0 {
			podSpecs[component] = workload.Spec.Template.Spec
		}
	}

	return podSpecs
}

func upstreamSSHDService(t *testing.T, files map[string]string) *corev1.Service {
	t.Helper()

	for filename, content := range files {
		if strings.TrimSpace(content) == "" {
			continue
		}

		var header struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(content), &header); err != nil {
			t.Fatalf("parse upstream service header %s: %v", filename, err)
		}

		if header.Kind != "Service" {
			continue
		}

		var service corev1.Service
		if err := yaml.Unmarshal([]byte(content), &service); err != nil {
			t.Fatalf("parse upstream service %s: %v", filename, err)
		}

		if service.Labels["app.kubernetes.io/component"] == "sshd" && len(service.Spec.Ports) > 0 {
			return &service
		}
	}

	t.Fatal("upstream chart did not render the SSHD service")

	return nil
}

func assertRootToolSecurityContext(
	t *testing.T,
	component string,
	context *corev1.SecurityContext,
) {
	t.Helper()

	if context == nil || context.RunAsUser == nil || *context.RunAsUser != 0 ||
		context.RunAsGroup == nil ||
		*context.RunAsGroup != 0 {
		t.Fatalf("%s securityContext=%#v", component, context)
	}
}

func assertNoTokenServiceAccount(t *testing.T, component string, podSpec corev1.PodSpec) {
	t.Helper()

	if podSpec.ServiceAccountName != TransferServiceAccountName {
		t.Fatalf(
			"%s serviceAccountName=%q, want %q",
			component,
			podSpec.ServiceAccountName,
			TransferServiceAccountName,
		)
	}

	if podSpec.AutomountServiceAccountToken != nil && *podSpec.AutomountServiceAccountToken {
		t.Fatalf("%s explicitly enables ServiceAccount token automount", component)
	}
}

func assertZeroToolResources(
	t *testing.T,
	component string,
	resources corev1.ResourceRequirements,
) {
	t.Helper()

	zero := resource.MustParse("0")
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		request, ok := resources.Requests[name]
		if !ok || request.Cmp(zero) != 0 {
			t.Fatalf("%s request %s=%s", component, name, request.String())
		}
	}

	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		limit, ok := resources.Limits[name]
		if !ok || limit.Cmp(zero) != 0 {
			t.Fatalf("%s limit %s=%s", component, name, limit.String())
		}
	}

	if limit, exists := resources.Limits[corev1.ResourceEphemeralStorage]; exists {
		t.Fatalf("%s has ephemeral-storage limit=%s", component, limit.String())
	}
}

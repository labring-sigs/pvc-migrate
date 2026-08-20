package kube_test

import (
	"slices"
	"testing"

	. "github.com/labring-sigs/pvc-migrate/internal/kube"
	"github.com/labring-sigs/pvc-migrate/internal/testutil"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/cli/values"
	"helm.sh/helm/v4/pkg/getter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestPodBlocksPVCDeletion(t *testing.T) {
	for _, test := range []struct {
		name            string
		phase           corev1.PodPhase
		nodeName        string
		wantProtection  bool
		wantSafeRemoval bool
	}{
		{name: "scheduled running", phase: corev1.PodRunning, nodeName: "node-a", wantProtection: true, wantSafeRemoval: true},
		{name: "unscheduled pending", phase: corev1.PodPending, wantSafeRemoval: true},
		{name: "scheduled succeeded", phase: corev1.PodSucceeded, nodeName: "node-a", wantProtection: true, wantSafeRemoval: true},
		{name: "scheduled failed", phase: corev1.PodFailed, nodeName: "node-a", wantProtection: true, wantSafeRemoval: true},
		{name: "unscheduled succeeded", phase: corev1.PodSucceeded},
		{name: "unscheduled failed", phase: corev1.PodFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{
				Spec: corev1.PodSpec{NodeName: test.nodeName, Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				}},
				Status: corev1.PodStatus{Phase: test.phase},
			}
			if got := PodBlocksPVCDeletion(pod, "data"); got != test.wantProtection {
				t.Fatalf("PodBlocksPVCDeletion()=%t, want %t", got, test.wantProtection)
			}

			if got := PodPreventsSafePVCDeletion(pod, "data"); got != test.wantSafeRemoval {
				t.Fatalf("PodPreventsSafePVCDeletion()=%t, want %t", got, test.wantSafeRemoval)
			}

			if PodBlocksPVCDeletion(pod, "other") {
				t.Fatal("unreferenced PVC blocks deletion")
			}

			if PodPreventsSafePVCDeletion(pod, "other") {
				t.Fatal("unreferenced PVC prevents safe deletion")
			}
		})
	}
}

func TestActivePodUsesPVC(t *testing.T) {
	for _, test := range []struct {
		name  string
		phase corev1.PodPhase
		want  bool
	}{
		{name: "pending", phase: corev1.PodPending, want: true},
		{name: "running", phase: corev1.PodRunning, want: true},
		{name: "succeeded", phase: corev1.PodSucceeded},
		{name: "failed", phase: corev1.PodFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{
					{
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "data",
							},
						},
					},
				}},
				Status: corev1.PodStatus{Phase: test.phase},
			}
			if got := ActivePodUsesPVC(pod, "data"); got != test.want {
				t.Fatalf("ActivePodUsesPVC()=%t, want %t", got, test.want)
			}

			if ActivePodUsesPVC(pod, "other") {
				t.Fatal("unreferenced PVC has an active consumer")
			}
		})
	}
}

func TestDefaultToolImageUsesBuildRepositoryAndTag(t *testing.T) {
	tests := []struct {
		name       string
		repository string
		version    string
		want       string
	}{
		{
			name:       "release",
			repository: "registry.example/pvc-migrate",
			version:    "v1.2.3",
			want:       "registry.example/pvc-migrate:1.2.3",
		},
		{
			name:       "repository trailing slash",
			repository: "registry.example/team/pvc-migrate/",
			version:    "1.2.3",
			want:       "registry.example/team/pvc-migrate:1.2.3",
		},
		{
			name:       "trim inputs",
			repository: " registry.example/pvc-migrate/ ",
			version:    " v1.2.3 ",
			want:       "registry.example/pvc-migrate:1.2.3",
		},
		{
			name:       "development",
			repository: "",
			version:    "dev",
			want:       DefaultToolImageRepository + ":main",
		},
		{
			name:       "empty version",
			repository: "registry.example/pvc-migrate",
			version:    "",
			want:       "registry.example/pvc-migrate:main",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := DefaultToolImage(test.repository, test.version); got != test.want {
				t.Fatalf(
					"DefaultToolImage(%q, %q)=%q, want %q",
					test.repository,
					test.version,
					got,
					test.want,
				)
			}
		})
	}
}

func TestZeroResourceRequirementsIncludesComputeAndEphemeralStorage(t *testing.T) {
	requirements := ZeroResourceRequirements()
	zero := resource.MustParse("0")
	wantRequests := []corev1.ResourceName{
		corev1.ResourceCPU,
		corev1.ResourceMemory,
		corev1.ResourceEphemeralStorage,
	}
	wantLimits := []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory}

	for _, name := range wantRequests {
		request, ok := requirements.Requests[name]
		if !ok || request.Cmp(zero) != 0 {
			t.Fatalf("resource %s request=%s, want zero", name, request.String())
		}
	}

	for _, name := range wantLimits {
		limit, ok := requirements.Limits[name]
		if !ok || limit.Cmp(zero) != 0 {
			t.Fatalf("resource %s limit=%s, want zero", name, limit.String())
		}
	}

	if _, ok := requirements.Limits[corev1.ResourceEphemeralStorage]; ok {
		t.Fatal("zero ephemeral-storage limit would evict every tool")
	}

	if len(requirements.Requests) != len(wantRequests) ||
		len(requirements.Limits) != len(wantLimits) {
		t.Fatalf("resource keys requests=%v limits=%v", requirements.Requests, requirements.Limits)
	}
}

func TestZeroResourceHelmValuesCoverAllToolComponents(t *testing.T) {
	values := ZeroResourceHelmValues()
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		for _, resourceName := range []string{"requests.cpu", "requests.memory", "requests.ephemeral-storage", "limits.cpu", "limits.memory"} {
			value := component + ".resources." + resourceName + "=0"
			if !slices.Contains(values, value) {
				t.Fatalf("missing Helm value %q", value)
			}
		}
	}

	if len(values) != 15 {
		t.Fatalf("values=%d, want 15", len(values))
	}
}

func TestZeroResourceHelmValuesParseAsChartOverrides(t *testing.T) {
	options := values.Options{StringValues: ZeroResourceHelmValues()}

	merged, err := options.MergeValues(getter.All(cli.New()))
	if err != nil {
		t.Fatalf("parse Helm resource overrides: %v", err)
	}

	for _, component := range []string{"rsync", "sshd", "rclone"} {
		componentValues, ok := merged[component].(map[string]any)
		if !ok {
			t.Fatalf("component %s values=%#v", component, merged[component])
		}

		resources, ok := componentValues["resources"].(map[string]any)
		if !ok {
			t.Fatalf("component %s resources=%#v", component, componentValues["resources"])
		}

		for _, resourceName := range []string{"cpu", "memory", "ephemeral-storage"} {
			bounds := []string{"requests"}
			if resourceName != "ephemeral-storage" {
				bounds = append(bounds, "limits")
			}

			for _, bound := range bounds {
				valuesMap, ok := resources[bound].(map[string]any)
				if !ok || valuesMap[resourceName] != "0" {
					t.Fatalf(
						"%s.resources.%s type=%T value=%#v parsed=%#v",
						component,
						bound,
						resources[bound],
						valuesMap[resourceName],
						merged,
					)
				}
			}

			limits := testutil.MustType[map[string]any](t, resources["limits"])
			if _, ok := limits["ephemeral-storage"]; ok {
				t.Fatalf("component %s sets an evicting ephemeral-storage limit", component)
			}
		}
	}
}

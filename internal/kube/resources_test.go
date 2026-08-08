package kube

import (
	"slices"
	"testing"

	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/cli/values"
	"helm.sh/helm/v4/pkg/getter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestPVMigrateImageTagIsPinned(t *testing.T) {
	if PVMigrateImageTag != "v3.6.1" {
		t.Fatalf("PVMigrateImageTag=%q, want v3.6.1", PVMigrateImageTag)
	}
}

func TestZeroResourceRequirementsIncludesComputeAndEphemeralStorage(t *testing.T) {
	requirements := ZeroResourceRequirements()
	zero := resource.MustParse("0")
	wantRequests := []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage}
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
		t.Fatal("zero ephemeral-storage limit would evict every helper")
	}
	if len(requirements.Requests) != len(wantRequests) || len(requirements.Limits) != len(wantLimits) {
		t.Fatalf("resource keys requests=%v limits=%v", requirements.Requests, requirements.Limits)
	}
}

func TestZeroResourceHelmValuesCoverAllHelperComponents(t *testing.T) {
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
					t.Fatalf("%s.resources.%s type=%T value=%#v parsed=%#v", component, bound, resources[bound], valuesMap[resourceName], merged)
				}
			}
			if _, ok := resources["limits"].(map[string]any)["ephemeral-storage"]; ok {
				t.Fatalf("component %s sets an evicting ephemeral-storage limit", component)
			}
		}
	}
}

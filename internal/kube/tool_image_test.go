package kube

import (
	"slices"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	chartutil "helm.sh/helm/v4/pkg/chart/common/util"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"helm.sh/helm/v4/pkg/cli"
	"helm.sh/helm/v4/pkg/cli/values"
	"helm.sh/helm/v4/pkg/getter"
)

func TestNormalizeToolImageCanonicalizesAndConfiguresAllComponents(t *testing.T) {
	got, err := NormalizeToolImage("registry.example/team/pvc-migrate:aio")
	if err != nil {
		t.Fatal(err)
	}
	if got != "registry.example/team/pvc-migrate:aio" {
		t.Fatalf("normalized image=%q", got)
	}
	values, err := ToolImageHelmValues(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 6 {
		t.Fatalf("Helm image values=%v", values)
	}
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		if !slices.Contains(values, component+".image.repository=registry.example/team/pvc-migrate") || !slices.Contains(values, component+".image.tag=aio") {
			t.Fatalf("component %s values=%v", component, values)
		}
	}
}

func TestNormalizeToolImageAcceptsSupportedReferenceForms(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty uses build default", input: "", want: DefaultToolImageRepository + ":main"},
		{name: "trims whitespace", input: "  registry.example/team/tool:v1  ", want: "registry.example/team/tool:v1"},
		{name: "registry port", input: "registry.example:5000/team/tool:v1", want: "registry.example:5000/team/tool:v1"},
		{name: "localhost registry", input: "localhost:5000/tool:test", want: "localhost:5000/tool:test"},
		{name: "docker hub canonical name", input: "busybox:1.36", want: "docker.io/library/busybox:1.36"},
		{name: "explicit latest tag", input: "registry.example/tool:latest", want: "registry.example/tool:latest"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeToolImage(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("NormalizeToolImage(%q)=%q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestToolImageValuesParseAsHelmStringOverrides(t *testing.T) {
	imageValues, err := ToolImageHelmValues("registry.example:5000/team/pvc-migrate:aio")
	if err != nil {
		t.Fatal(err)
	}
	options := values.Options{StringValues: imageValues}
	merged, err := options.MergeValues(getter.All(cli.New()))
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		componentValues, ok := merged[component].(map[string]any)
		if !ok {
			t.Fatalf("component %s=%#v", component, merged[component])
		}
		image, ok := componentValues["image"].(map[string]any)
		if !ok || image["repository"] != "registry.example:5000/team/pvc-migrate" || image["tag"] != "aio" {
			t.Fatalf("component %s image=%#v", component, componentValues["image"])
		}
	}
}

func TestToolSecurityContextValuesParseAsNumericHelmOverrides(t *testing.T) {
	securityValues := ToolSecurityContextHelmValues()
	if len(securityValues) != 6 {
		t.Fatalf("security values=%v", securityValues)
	}
	options := values.Options{Values: securityValues}
	merged, err := options.MergeValues(getter.All(cli.New()))
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		componentValues, ok := merged[component].(map[string]any)
		if !ok {
			t.Fatalf("component %s=%#v", component, merged[component])
		}
		securityContext, ok := componentValues["securityContext"].(map[string]any)
		if !ok {
			t.Fatalf("component %s securityContext=%#v", component, componentValues["securityContext"])
		}
		for _, field := range []string{"runAsUser", "runAsGroup"} {
			if value, ok := securityContext[field].(int64); !ok || value != 0 {
				t.Fatalf("component %s %s=%#v (%T), want numeric zero", component, field, securityContext[field], securityContext[field])
			}
		}
	}
}

func TestToolSecurityContextValuesPreserveSSHDChartCapabilities(t *testing.T) {
	options := values.Options{Values: ToolSecurityContextHelmValues()}
	overrides, err := options.MergeValues(getter.All(cli.New()))
	if err != nil {
		t.Fatal(err)
	}
	merged, err := chartutil.CoalesceValues(&chart.Chart{
		Metadata: &chart.Metadata{Name: "pv-migrate", APIVersion: chart.APIVersionV2},
		Values: map[string]any{
			"sshd": map[string]any{
				"securityContext": map[string]any{
					"capabilities": map[string]any{"add": []any{"SYS_CHROOT"}},
				},
			},
		},
	}, overrides)
	if err != nil {
		t.Fatal(err)
	}
	sshd := merged["sshd"].(map[string]any)
	securityContext := sshd["securityContext"].(map[string]any)
	capabilities := securityContext["capabilities"].(map[string]any)
	add, ok := capabilities["add"].([]any)
	if !ok || len(add) != 1 || add[0] != "SYS_CHROOT" {
		t.Fatalf("sshd capabilities.add=%#v", capabilities["add"])
	}
	for _, field := range []string{"runAsUser", "runAsGroup"} {
		if value, ok := securityContext[field].(int64); !ok || value != 0 {
			t.Fatalf("sshd %s=%#v (%T), want numeric zero", field, securityContext[field], securityContext[field])
		}
	}
}

func TestNormalizeToolImageRejectsUnpinnedAndDigestReferences(t *testing.T) {
	for _, image := range []string{
		"registry.example/team/pvc-migrate",
		"registry.example/team/pvc-migrate@sha256:abc",
		"registry.example/team/pvc-migrate:aio@sha256:abc",
		"registry.example/team/pvc-migrate:",
		"bad image:aio",
		"registry.example/tool:tag extra",
		"http://registry.example/tool:tag",
	} {
		_, err := NormalizeToolImage(image)
		if domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("image %q error=%v category=%q", image, err, domain.CategoryOf(err))
		}
		if !strings.Contains(err.Error(), image) && !strings.Contains(image, "@") {
			t.Fatalf("image %q error=%v does not identify input", image, err)
		}
	}
}

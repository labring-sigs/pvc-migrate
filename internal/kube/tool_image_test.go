package kube

import (
	"slices"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
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
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		if !slices.Contains(values, component+".image.repository=registry.example/team/pvc-migrate") || !slices.Contains(values, component+".image.tag=aio") {
			t.Fatalf("component %s values=%v", component, values)
		}
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

func TestNormalizeToolImageRejectsUnpinnedAndDigestReferences(t *testing.T) {
	for _, image := range []string{"registry.example/team/pvc-migrate", "registry.example/team/pvc-migrate@sha256:abc", "bad image:aio"} {
		_, err := NormalizeToolImage(image)
		if domain.CategoryOf(err) != domain.ErrorValidation {
			t.Fatalf("image %q error=%v category=%q", image, err, domain.CategoryOf(err))
		}
		if !strings.Contains(err.Error(), image) && !strings.Contains(image, "@") {
			t.Fatalf("image %q error=%v does not identify input", image, err)
		}
	}
}

package controller

import (
	"os"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func TestControllerDeploymentsDoNotDependOnWritableTemporaryVolume(t *testing.T) {
	for _, manifest := range []string{"../../config/manager/manager.yaml", "../../deploy/controller.yaml"} {
		data, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatal(err)
		}

		var deployment appsv1.Deployment
		if err := yaml.Unmarshal(data, &deployment); err != nil {
			t.Fatalf("parse %s: %v", manifest, err)
		}

		if len(deployment.Spec.Template.Spec.Volumes) != 0 {
			t.Fatalf(
				"%s declares temporary volumes: %#v",
				manifest,
				deployment.Spec.Template.Spec.Volumes,
			)
		}

		if len(deployment.Spec.Template.Spec.Containers) != 1 {
			t.Fatalf(
				"%s has %d containers, want one",
				manifest,
				len(deployment.Spec.Template.Spec.Containers),
			)
		}

		container := deployment.Spec.Template.Spec.Containers[0]
		if len(container.VolumeMounts) != 0 {
			t.Fatalf("%s declares temporary volume mounts: %#v", manifest, container.VolumeMounts)
		}

		if container.SecurityContext == nil ||
			container.SecurityContext.ReadOnlyRootFilesystem == nil ||
			!*container.SecurityContext.ReadOnlyRootFilesystem {
			t.Fatalf("%s must keep readOnlyRootFilesystem enabled", manifest)
		}
	}
}

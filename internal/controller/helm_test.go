package controller

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"helm.sh/helm/v4/pkg/chart/common"
	commonutil "helm.sh/helm/v4/pkg/chart/common/util"
	chartloader "helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/engine"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

//nolint:gocyclo // Validates the rendered production contract end to end.
func TestHelmDeploymentContract(t *testing.T) {
	files, err := renderControllerChart(nil)
	if err != nil {
		t.Fatal(err)
	}

	var deployment appsv1.Deployment
	decodeChartObject(t, files, "deployment.yaml", &deployment)

	if deployment.Name != "pvc-migrate-controller" || deployment.Namespace != "operators" {
		t.Fatalf("unexpected deployment identity: %s/%s", deployment.Namespace, deployment.Name)
	}

	legacy, err := os.ReadFile("../../deploy/controller.yaml")
	if err != nil {
		t.Fatal(err)
	}

	var existing appsv1.Deployment
	if err := yaml.Unmarshal(legacy, &existing); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(deployment.Spec.Selector, existing.Spec.Selector) {
		t.Fatal("Helm adoption would change the immutable Deployment selector")
	}

	spec := deployment.Spec.Template.Spec
	if *deployment.Spec.Replicas != 2 || *spec.TerminationGracePeriodSeconds < 30 {
		t.Fatal("controller lacks standby replica or graceful termination")
	}

	if !*spec.AutomountServiceAccountToken || spec.ServiceAccountName != "pvc-migrate" {
		t.Fatal("controller must use its explicit operator identity")
	}

	if !*spec.SecurityContext.RunAsNonRoot || len(spec.Volumes) != 0 {
		t.Fatal("controller must run without root or writable volumes")
	}

	container := spec.Containers[0]
	if !*container.SecurityContext.ReadOnlyRootFilesystem ||
		*container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("controller security context is not restricted")
	}

	if container.StartupProbe == nil || container.ReadinessProbe.HTTPGet.Path != "/readyz" ||
		container.LivenessProbe.HTTPGet.Path != "/healthz" || container.Resources.Requests.Memory().IsZero() {
		t.Fatal("controller health or resource configuration is missing")
	}

	for _, arg := range []string{
		"--controller-namespace=operators", "--session-namespace=operators",
		"--tool-image=ghcr.io/labring-sigs/pvc-migrate:0.4.0",
	} {
		if !slices.Contains(container.Args, arg) {
			t.Fatalf("missing controller argument %q", arg)
		}
	}

	var testPod corev1.Pod
	decodeChartObject(t, files, "tests/test-connection.yaml", &testPod)

	if *testPod.Spec.AutomountServiceAccountToken ||
		reflect.DeepEqual(testPod.Labels, deployment.Spec.Template.Labels) ||
		testPod.Labels["app.kubernetes.io/component"] == "controller" {
		t.Fatal("Helm test must not receive credentials or match the controller selector")
	}

	for name, data := range files {
		decoder := yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(data), 4096)
		for {
			var object struct {
				metav1.TypeMeta `json:",inline"`
			}

			err := decoder.Decode(&object)
			if errors.Is(err, io.EOF) || strings.HasSuffix(name, "NOTES.txt") {
				break
			}

			if err != nil {
				t.Fatalf("decode %s: %v", name, err)
			}

			if object.Kind == "Namespace" || object.Kind == "CustomResourceDefinition" {
				t.Fatalf("release must not manage lifecycle of %s", object.Kind)
			}
		}
	}
}

func TestHelmRBACMatchesControllerContract(t *testing.T) {
	files, err := renderControllerChart(map[string]any{
		"rbac": map[string]any{"kubeBlocksMongoDBNamespaces": []any{"database"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	decoder := yamlutil.NewYAMLOrJSONDecoder(
		strings.NewReader(files["pvc-migrate/templates/rbac.yaml"]),
		4096,
	)

	var role rbacv1.ClusterRole
	if err := decoder.Decode(&role); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(rolePermissions(role), controllerRolePermissions()) {
		t.Fatal("Helm ClusterRole differs from the controller permission contract")
	}

	var binding rbacv1.ClusterRoleBinding
	if err := decoder.Decode(&binding); err != nil {
		t.Fatal(err)
	}

	if binding.Subjects[0].Namespace != "operators" || binding.Subjects[0].Name != "pvc-migrate" {
		t.Fatal("Helm role binding references the wrong operator identity")
	}

	var mongoRole rbacv1.Role
	if err := decoder.Decode(&mongoRole); err != nil {
		t.Fatal(err)
	}

	if mongoRole.Namespace != "database" || len(mongoRole.Rules) != 1 ||
		!reflect.DeepEqual(mongoRole.Rules[0].Resources, []string{"pods/exec"}) {
		t.Fatal("MongoDB exec permission must be confined to the approved namespace")
	}
}

func TestHelmValuesValidationAndOverrides(t *testing.T) {
	for _, values := range []map[string]any{
		{"replicaCount": 0},
		{"image": map[string]any{"tag": "latest"}},
		{"image": map[string]any{"digest": "sha256:invalid"}},
		{"serviceAccount": map[string]any{"create": false}},
		{"serviceAccount": map[string]any{"name": "default"}},
		{"podLabels": map[string]any{"app.kubernetes.io/name": "wrong"}},
		{"podDisruptionBudget": map[string]any{"minAvailable": 2}},
		{"controller": map[string]any{"operationTimeout": "0s"}},
		{"createNamespace": true},
	} {
		if _, err := renderControllerChart(values); err == nil {
			t.Fatalf("invalid values accepted: %v", values)
		}
	}

	digest := "sha256:" + strings.Repeat("a", 64)

	files, err := renderControllerChart(map[string]any{
		"replicaCount":   1,
		"image":          map[string]any{"digest": digest},
		"serviceAccount": map[string]any{"create": false, "name": "existing-operator"},
		"rbac":           map[string]any{"create": false},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"serviceaccount.yaml", "rbac.yaml", "poddisruptionbudget.yaml"} {
		if strings.TrimSpace(files["pvc-migrate/templates/"+name]) != "" {
			t.Fatalf("disabled resource %s was rendered", name)
		}
	}

	var deployment appsv1.Deployment
	decodeChartObject(t, files, "deployment.yaml", &deployment)

	if deployment.Spec.Template.Spec.ServiceAccountName != "existing-operator" ||
		!strings.HasSuffix(deployment.Spec.Template.Spec.Containers[0].Image, "@"+digest) {
		t.Fatal("external service account or pinned controller image was ignored")
	}
}

func TestHelmCRDsMatchGeneratedSchemas(t *testing.T) {
	sources, err := filepath.Glob("../../config/crd/bases/*.yaml")
	if err != nil || len(sources) == 0 {
		t.Fatalf("find generated CRDs: %v", err)
	}

	packaged, err := filepath.Glob("../../charts/pvc-migrate/crds/*.yaml")
	if err != nil || len(packaged) != len(sources) {
		t.Fatalf("packaged CRD count differs: %v", err)
	}

	for _, source := range sources {
		want, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}

		got, err := os.ReadFile(
			filepath.Join("../../charts/pvc-migrate/crds", filepath.Base(source)),
		)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("chart CRD %s is stale; run make chart-sync: %v", filepath.Base(source), err)
		}
	}
}

func renderControllerChart(values map[string]any) (map[string]string, error) {
	chart, err := chartloader.LoadDir("../../charts/pvc-migrate")
	if err != nil {
		return nil, err
	}

	renderValues, err := commonutil.ToRenderValues(chart, values, common.ReleaseOptions{
		Name: "pvc-migrate", Namespace: "operators",
	}, nil)
	if err != nil {
		return nil, err
	}

	return engine.Render(chart, renderValues)
}

func decodeChartObject(t *testing.T, files map[string]string, name string, object any) {
	t.Helper()

	if err := yaml.Unmarshal([]byte(files["pvc-migrate/templates/"+name]), object); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
}

package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestCheckPodDependenciesCollectsAllReferenceForms(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "worker"},
		Spec: corev1.PodSpec{
			ServiceAccountName: "runner",
			ImagePullSecrets:   []corev1.LocalObjectReference{{Name: "pull"}},
			Volumes: []corev1.Volume{
				{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "volume-secret"}}},
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "volume-config"}}}},
				{Name: "projected", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
					{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "projected-secret"}}},
					{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "projected-config"}}},
				}}}},
			},
			InitContainers: []corev1.Container{{Name: "init", EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "env-secret"}}}}}},
			Containers: []corev1.Container{{
				Name:    "app",
				EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "env-config"}}}},
				Env: []corev1.EnvVar{
					{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "key-secret"}}}},
					{Name: "MODE", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "key-config"}}}},
				},
			}},
		},
	}
	objects := []runtime.Object{&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "runner"}}}
	for _, name := range []string{"pull", "volume-secret", "projected-secret", "env-secret", "key-secret"} {
		objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name}})
	}
	for _, name := range []string{"volume-config", "projected-config", "env-config", "key-config"} {
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name}})
	}
	plan := &domain.MigrationPlan{Ready: true}
	New(kubernetesfake.NewClientset(objects...), nil).checkPodDependencies(context.Background(), plan, pod)
	if !plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "5 Secret(s), 4 ConfigMap(s), ServiceAccount/runner") {
		t.Fatalf("dependency result: %#v", plan.Checks)
	}
}

func TestCheckPodDependenciesReportsSortedMissingDefaults(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "worker"},
		Spec: corev1.PodSpec{
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "z-secret"}},
			Volumes:          []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "a-config"}}}}},
		},
	}
	plan := &domain.MigrationPlan{Ready: true}
	New(kubernetesfake.NewClientset(), nil).checkPodDependencies(context.Background(), plan, pod)
	want := "missing dependencies: ConfigMap/a-config, Secret/z-secret, ServiceAccount/default"
	if plan.Ready || len(plan.Checks) != 1 || plan.Checks[0].Message != want {
		t.Fatalf("dependency result: %#v", plan.Checks)
	}
}

func TestCheckPodDependenciesStopsOnKubernetesError(t *testing.T) {
	client := kubernetesfake.NewClientset()
	client.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API timeout")
	})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "worker"},
		Spec:       corev1.PodSpec{ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull"}}},
	}
	plan := &domain.MigrationPlan{Ready: true}
	New(client, nil).checkPodDependencies(context.Background(), plan, pod)
	if plan.Ready || len(plan.Checks) != 1 || !strings.Contains(plan.Checks[0].Message, "API timeout") {
		t.Fatalf("dependency result: %#v", plan.Checks)
	}
}

func TestRiskRolesAreCaseInsensitive(t *testing.T) {
	for _, role := range []string{"leader", "PRIMARY", "Master", "unknown"} {
		if !isRiskRole(role) {
			t.Fatalf("role %q should be risky", role)
		}
	}
	for _, role := range []string{"", "secondary", "reader", "writer"} {
		if isRiskRole(role) {
			t.Fatalf("role %q should be low risk", role)
		}
	}
}

func TestKubeBlocksRoleWarningDescribesTheSelectedSafetyPath(t *testing.T) {
	tests := []struct {
		name      string
		spec      domain.KubeBlocksSpec
		want      string
		forbidden string
	}{
		{name: "unknown role with accepted downtime", spec: domain.KubeBlocksSpec{Role: "unknown"}, want: "possible leader downtime was explicitly acknowledged", forbidden: "target="},
		{name: "known leader with accepted downtime", spec: domain.KubeBlocksSpec{Role: "primary"}, want: "leader downtime was explicitly acknowledged", forbidden: "target="},
		{name: "automatic switchover", spec: domain.KubeBlocksSpec{Role: "primary", SwitchoverCandidate: "db-1"}, want: "switchover target=db-1"},
		{name: "MongoDB native switchover", spec: domain.KubeBlocksSpec{Role: "primary", SwitchoverCandidate: "db-1", SwitchoverStrategy: domain.KubeBlocksSwitchoverMongoDBNative}, want: "native candidate switchover targets=db-1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := kubeBlocksRoleWarning(&test.spec)
			if !strings.Contains(message, test.want) || (test.forbidden != "" && strings.Contains(message, test.forbidden)) {
				t.Fatalf("message=%q want=%q forbidden=%q", message, test.want, test.forbidden)
			}
		})
	}
}

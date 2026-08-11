package planner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dependencyKindSecret         = "Secret"
	dependencyKindConfigMap      = "ConfigMap"
	dependencyKindServiceAccount = "ServiceAccount"
)

func (p *Planner) checkPodDependencies(ctx context.Context, plan *domain.MigrationPlan, pod *corev1.Pod) {
	secrets := map[string]struct{}{}
	configMaps := map[string]struct{}{}
	for _, reference := range pod.Spec.ImagePullSecrets {
		secrets[reference.Name] = struct{}{}
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret != nil {
			secrets[volume.Secret.SecretName] = struct{}{}
		}
		if volume.ConfigMap != nil {
			configMaps[volume.ConfigMap.Name] = struct{}{}
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil {
					secrets[source.Secret.Name] = struct{}{}
				}
				if source.ConfigMap != nil {
					configMaps[source.ConfigMap.Name] = struct{}{}
				}
			}
		}
	}
	containers := append([]corev1.Container(nil), pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)
	for _, container := range containers {
		for _, source := range container.EnvFrom {
			if source.SecretRef != nil {
				secrets[source.SecretRef.Name] = struct{}{}
			}
			if source.ConfigMapRef != nil {
				configMaps[source.ConfigMapRef.Name] = struct{}{}
			}
		}
		for _, variable := range container.Env {
			if variable.ValueFrom == nil {
				continue
			}
			if variable.ValueFrom.SecretKeyRef != nil {
				secrets[variable.ValueFrom.SecretKeyRef.Name] = struct{}{}
			}
			if variable.ValueFrom.ConfigMapKeyRef != nil {
				configMaps[variable.ValueFrom.ConfigMapKeyRef.Name] = struct{}{}
			}
		}
	}
	serviceAccount := pod.Spec.ServiceAccountName
	if serviceAccount == "" {
		serviceAccount = "default"
	}

	type dependency struct {
		kind string
		name string
	}
	dependencies := make([]dependency, 0, len(secrets)+len(configMaps)+1)
	for name := range secrets {
		if name != "" {
			dependencies = append(dependencies, dependency{kind: dependencyKindSecret, name: name})
		}
	}
	for name := range configMaps {
		if name != "" {
			dependencies = append(dependencies, dependency{kind: dependencyKindConfigMap, name: name})
		}
	}
	if serviceAccount != "" {
		dependencies = append(dependencies, dependency{kind: dependencyKindServiceAccount, name: serviceAccount})
	}
	sort.SliceStable(dependencies, func(i, j int) bool {
		if dependencies[i].kind != dependencies[j].kind {
			return dependencyKindOrder(dependencies[i].kind) < dependencyKindOrder(dependencies[j].kind)
		}
		return dependencies[i].name < dependencies[j].name
	})
	results := make([]error, len(dependencies))
	parallel.For(len(dependencies), func(index int) {
		item := dependencies[index]
		switch item.kind {
		case dependencyKindSecret:
			_, results[index] = p.client.CoreV1().Secrets(pod.Namespace).Get(ctx, item.name, metav1.GetOptions{})
		case dependencyKindConfigMap:
			_, results[index] = p.client.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, item.name, metav1.GetOptions{})
		case dependencyKindServiceAccount:
			_, results[index] = p.client.CoreV1().ServiceAccounts(pod.Namespace).Get(ctx, item.name, metav1.GetOptions{})
		}
	})

	missing := make([]string, 0)
	for index, item := range dependencies {
		err := results[index]
		if err == nil {
			continue
		}
		if apierrors.IsNotFound(err) {
			missing = append(missing, item.kind+"/"+item.name)
			continue
		}
		plan.AddCheck(failed("pod-dependencies", fmt.Sprintf("read %s %s/%s: %v", item.kind, pod.Namespace, item.name, err)))
		return
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		plan.AddCheck(failed("pod-dependencies", "missing dependencies: "+strings.Join(missing, ", ")))
		return
	}
	plan.AddCheck(passed("pod-dependencies", fmt.Sprintf("Pod dependencies resolved: %d Secret(s), %d ConfigMap(s), ServiceAccount/%s", len(secrets), len(configMaps), serviceAccount)))
}

func dependencyKindOrder(kind string) int {
	switch kind {
	case dependencyKindSecret:
		return 0
	case dependencyKindConfigMap:
		return 1
	default:
		return 2
	}
}

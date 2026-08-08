package planner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	missing := make([]string, 0)
	for name := range secrets {
		if name == "" {
			continue
		}
		if _, err := p.client.CoreV1().Secrets(pod.Namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				missing = append(missing, "Secret/"+name)
			} else {
				plan.AddCheck(failed("pod-dependencies", fmt.Sprintf("read Secret %s/%s: %v", pod.Namespace, name, err)))
				return
			}
		}
	}
	for name := range configMaps {
		if name == "" {
			continue
		}
		if _, err := p.client.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				missing = append(missing, "ConfigMap/"+name)
			} else {
				plan.AddCheck(failed("pod-dependencies", fmt.Sprintf("read ConfigMap %s/%s: %v", pod.Namespace, name, err)))
				return
			}
		}
	}
	serviceAccount := pod.Spec.ServiceAccountName
	if serviceAccount == "" {
		serviceAccount = "default"
	}
	if _, err := p.client.CoreV1().ServiceAccounts(pod.Namespace).Get(ctx, serviceAccount, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			missing = append(missing, "ServiceAccount/"+serviceAccount)
		} else {
			plan.AddCheck(failed("pod-dependencies", fmt.Sprintf("read ServiceAccount %s/%s: %v", pod.Namespace, serviceAccount, err)))
			return
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		plan.AddCheck(failed("pod-dependencies", "missing dependencies: "+strings.Join(missing, ", ")))
		return
	}
	plan.AddCheck(passed("pod-dependencies", fmt.Sprintf("Pod dependencies resolved: %d Secret(s), %d ConfigMap(s), ServiceAccount/%s", len(secrets), len(configMaps), serviceAccount)))
}

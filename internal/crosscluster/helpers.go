package crosscluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func objectRef(v ClusterResourceRef) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      v.APIVersion,
		Kind:            v.Kind,
		Namespace:       v.Namespace,
		Name:            v.Name,
		UID:             v.UID,
		ResourceVersion: v.ResourceVersion,
	}
}

func capacitySmaller(destination, source string) bool {
	d, err1 := resource.ParseQuantity(destination)
	s, err2 := resource.ParseQuantity(source)
	return err1 == nil && err2 == nil && d.Cmp(s) < 0
}

func normalizeStrategies(in []string) []string {
	if len(in) == 0 {
		return []string{"local"}
	}
	return append([]string(nil), in...)
}

func validateStrategies(in []string) error {
	for _, v := range in {
		switch v {
		case "local", "loadbalancer", "nodeport":
		default:
			return fmt.Errorf(
				"strategy %q is not supported for cross-cluster copy; use local, loadbalancer, or nodeport",
				v,
			)
		}
	}

	return nil
}

func activeConsumers(
	ctx context.Context,
	client kubernetes.Interface,
	namespace, name string,
) ([]string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var out []string
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodSucceeded ||
			pods.Items[i].Status.Phase == corev1.PodFailed {
			continue
		}

		if kube.ActivePodUsesPVC(&pods.Items[i], name) {
			out = append(out, pods.Items[i].Name)
		}
	}

	sort.Strings(out)

	return out, nil
}

func resolveNames(values, source []string) ([]string, error) {
	if len(values) == 0 {
		out := make([]string, len(source))
		for i := range source {
			out[i] = source[i] + "-copy"
		}

		return out, nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(
					"multiple source PVCs require explicit destination mappings such as source=destination",
				)
			}
		}
	}

	out := make([]string, len(source))

	seen := map[string]bool{}
	for _, v := range values {
		key, val, ok := strings.Cut(v, "=")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("destination PVC mapping %q must use source=name", v)
		}

		matched := false
		for i, s := range source {
			if s == key {
				matched = true

				if seen[key] {
					return nil, fmt.Errorf("duplicate destination PVC mapping for %s", key)
				}

				out[i] = val
				seen[key] = true
			}
		}

		if !matched {
			return nil, fmt.Errorf("destination PVC mapping references unknown source PVC %s", key)
		}
	}

	for i := range out {
		if out[i] == "" {
			return nil, fmt.Errorf("destination PVC mapping is missing source %s", source[i])
		}
	}

	seenDestinations := make(map[string]struct{}, len(out))
	for _, destination := range out {
		if _, exists := seenDestinations[destination]; exists {
			return nil, fmt.Errorf(
				"destination PVC %s is mapped from more than one source PVC",
				destination,
			)
		}

		seenDestinations[destination] = struct{}{}
	}

	return out, nil
}

func resolveValues(values, source []string) ([]string, error) {
	if len(values) == 0 {
		return make([]string, len(source)), nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(
					"multiple source PVCs require explicit capacity mappings such as source=capacity",
				)
			}
		}
	}

	out := make([]string, len(source))

	seen := map[string]bool{}
	for _, v := range values {
		key, val, ok := strings.Cut(v, "=")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("capacity mapping %q must use source=capacity", v)
		}

		found := false
		for i, s := range source {
			if s == key {
				if seen[key] {
					return nil, fmt.Errorf("duplicate capacity mapping for %s", key)
				}

				out[i] = val
				seen[key] = true
				found = true
			}
		}

		if !found {
			return nil, fmt.Errorf("capacity mapping references unknown source PVC %s", key)
		}
	}

	return out, nil
}

func resolvePaths(values, source []string) ([]string, error) {
	if len(values) == 0 {
		out := make([]string, len(source))
		for i := range out {
			out[i] = "."
		}

		return out, nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(
					"multiple source PVCs require explicit path mappings such as source=path",
				)
			}
		}
	}

	out := make([]string, len(source))

	seen := map[string]bool{}
	for _, v := range values {
		key, val, ok := strings.Cut(v, "=")
		if !ok || key == "" || val == "" {
			return nil, fmt.Errorf("path mapping %q must use source=path", v)
		}

		matched := false
		for i, s := range source {
			if s == key {
				matched = true

				if seen[key] {
					return nil, fmt.Errorf("duplicate path mapping for %s", key)
				}

				out[i] = val
				seen[key] = true
			}
		}

		if !matched {
			return nil, fmt.Errorf("path mapping references unknown source PVC %s", key)
		}
	}

	for i := range out {
		if out[i] == "" {
			return nil, fmt.Errorf("path mapping is missing source %s", source[i])
		}
	}

	return out, nil
}

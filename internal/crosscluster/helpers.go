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
		return []string{domain.StrategyLocal}
	}
	return append([]string(nil), in...)
}

func validateStrategies(in []string) error {
	for _, v := range in {
		switch v {
		case domain.StrategyLocal, domain.StrategyLoadBalancer, domain.StrategyNodePort:
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
	return resolveMappedValues(values, source, mappedValueOptions{
		defaultValue:     func(source string) string { return source + "-copy" },
		multipleMessage:  "multiple source PVCs require explicit destination mappings such as source=destination",
		invalidMessage:   "destination PVC mapping %q must use source=name",
		duplicateMessage: "duplicate destination PVC mapping for %s",
		unknownMessage:   "destination PVC mapping references unknown source PVC %s",
		missingMessage:   "destination PVC mapping is missing source %s",
		requireAll:       true,
		uniqueValues:     true,
	})
}

func resolveValues(values, source []string) ([]string, error) {
	return resolveMappedValues(values, source, mappedValueOptions{
		multipleMessage:  "multiple source PVCs require explicit capacity mappings such as source=capacity",
		invalidMessage:   "capacity mapping %q must use source=capacity",
		duplicateMessage: "duplicate capacity mapping for %s",
		unknownMessage:   "capacity mapping references unknown source PVC %s",
	})
}

func resolvePaths(values, source []string) ([]string, error) {
	return resolveMappedValues(values, source, mappedValueOptions{
		defaultValue:     func(string) string { return "." },
		multipleMessage:  "multiple source PVCs require explicit path mappings such as source=path",
		invalidMessage:   "path mapping %q must use source=path",
		duplicateMessage: "duplicate path mapping for %s",
		unknownMessage:   "path mapping references unknown source PVC %s",
		missingMessage:   "path mapping is missing source %s",
		requireAll:       true,
	})
}

type mappedValueOptions struct {
	defaultValue     func(string) string
	multipleMessage  string
	invalidMessage   string
	duplicateMessage string
	unknownMessage   string
	missingMessage   string
	requireAll       bool
	uniqueValues     bool
}

func resolveMappedValues(values, source []string, options mappedValueOptions) ([]string, error) {
	out := make([]string, len(source))
	if len(values) == 0 {
		if options.defaultValue != nil {
			for index, name := range source {
				out[index] = options.defaultValue(name)
			}
		}

		return out, nil
	}

	if len(values) == 1 && !strings.Contains(values[0], "=") && len(source) == 1 {
		return []string{values[0]}, nil
	}

	if len(source) > 1 {
		for _, value := range values {
			if !strings.Contains(value, "=") {
				return nil, errors.New(options.multipleMessage)
			}
		}
	}

	seen := make(map[string]bool, len(values))
	for _, value := range values {
		key, mapped, ok := strings.Cut(value, "=")
		if !ok || key == "" || mapped == "" {
			return nil, fmt.Errorf(options.invalidMessage, value)
		}

		matched := false
		for index, name := range source {
			if name != key {
				continue
			}

			matched = true

			if seen[key] {
				return nil, fmt.Errorf(options.duplicateMessage, key)
			}

			out[index] = mapped
			seen[key] = true
		}

		if !matched {
			return nil, fmt.Errorf(options.unknownMessage, key)
		}
	}

	if options.requireAll {
		for index, value := range out {
			if value == "" {
				return nil, fmt.Errorf(options.missingMessage, source[index])
			}
		}
	}

	if options.uniqueValues {
		seenValues := make(map[string]struct{}, len(out))
		for _, value := range out {
			if _, exists := seenValues[value]; exists {
				return nil, fmt.Errorf(
					"destination PVC %s is mapped from more than one source PVC",
					value,
				)
			}

			seenValues[value] = struct{}{}
		}
	}

	return out, nil
}

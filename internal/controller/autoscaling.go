package controller

import (
	"context"
	"fmt"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func (m *Manager) rejectHorizontalPodAutoscaler(
	ctx context.Context,
	namespace string,
	targetKind string,
	targetName string,
	operation string,
) error {
	hpas, err := m.typed.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(
		ctx,
		metav1.ListOptions{},
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			operation,
			"list HorizontalPodAutoscalers",
			err,
		)
	}

	for index := range hpas.Items {
		hpa := &hpas.Items[index]

		if horizontalPodAutoscalerTargetsAppsWorkload(
			hpa.Spec.ScaleTargetRef,
			targetKind,
			targetName,
		) {
			return domain.NewError(
				domain.ErrorPrecondition,
				operation,
				fmt.Sprintf(
					"%s %s/%s is targeted by HorizontalPodAutoscaler %s; remove that HPA and suspend any controller that recreates it before migrate-pod",
					targetKind,
					namespace,
					targetName,
					hpa.Name,
				),
			)
		}
	}

	return nil
}

func horizontalPodAutoscalerTargetsAppsWorkload(
	ref autoscalingv2.CrossVersionObjectReference,
	targetKind string,
	targetName string,
) bool {
	groupVersion, err := schema.ParseGroupVersion(ref.APIVersion)

	return err == nil && groupVersion.Group == appsv1.GroupName &&
		ref.Kind == targetKind && ref.Name == targetName
}

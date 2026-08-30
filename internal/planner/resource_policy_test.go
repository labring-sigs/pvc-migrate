package planner

import (
	"context"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMigrationNamespaceResourceEstimatesUseSerializedChartAndConcurrentProbePeaks(
	t *testing.T,
) {
	t.Setenv("HELM_DRIVER", "secret")

	scope := &domain.TransferScope{SourcePath: "source", DestinationPath: "destination"}
	state := &planState{
		options: planOptions{
			Operation:        domain.OperationCopy,
			SourceNamespace:  "application",
			StagingNamespace: "application",
			SessionNamespace: "application",
			TargetNode:       "worker",
			Strategies:       []string{domain.StrategyClusterIP},
		},
		plannedVolumes: []domain.PlannedVolume{
			{
				SourcePVC:      domain.ObjectReference{Namespace: "application", Name: "data-a"},
				DestinationPVC: domain.ObjectReference{Namespace: "application", Name: "copy-a"},
				TransferScope:  scope,
			},
			{
				SourcePVC:      domain.ObjectReference{Namespace: "application", Name: "data-b"},
				DestinationPVC: domain.ObjectReference{Namespace: "application", Name: "copy-b"},
				TransferScope:  scope,
			},
		},
		totalStorage:   resource.MustParse("4Gi"),
		storageByClass: map[string]resource.Quantity{"fast": resource.MustParse("4Gi")},
		pvcsByClass:    map[string]int{"fast": 2},
	}

	estimate := migrationNamespaceResourceEstimates(state)["application"]
	if estimate.PVCs != 2 || estimate.StorageRequests != "4Gi" ||
		estimate.PVCsByStorageClass["fast"] != 2 {
		t.Fatalf("persistent resource estimate=%#v", estimate)
	}

	if estimate.TerminatingPods != 5 || estimate.NotTerminatingPods != 2 ||
		estimate.Pods != 5 {
		t.Fatalf("Pod phase peaks=%#v, want terminating/non-terminating/all=5/2/5", estimate)
	}

	if estimate.Jobs != 1 || estimate.Deployments != 1 || estimate.Services != 1 ||
		estimate.Secrets != 3 || estimate.ServiceAccounts != 2 {
		t.Fatalf("chart resources were scaled by volume count: %#v", estimate)
	}

	if estimate.ConfigMaps != 1 || estimate.Leases != 1 {
		t.Fatalf("session resources were not merged: %#v", estimate)
	}
}

func TestMigrationProbePodPeaksTrackNamespacesAndWorkflowStages(t *testing.T) {
	scope := &domain.TransferScope{SourcePath: "source", DestinationPath: "destination"}
	volumes := []domain.PlannedVolume{
		{
			SourcePVC:      domain.ObjectReference{Namespace: "source", Name: "data-a"},
			DestinationPVC: domain.ObjectReference{Namespace: "stage", Name: "copy-a"},
			TransferScope:  scope,
		},
		{
			SourcePVC:      domain.ObjectReference{Namespace: "source", Name: "data-b"},
			DestinationPVC: domain.ObjectReference{Namespace: "stage", Name: "copy-b"},
			TransferScope:  scope,
		},
	}

	copyPeaks := migrationProbePodPeaks(planOptions{
		Operation:  domain.OperationCopy,
		TargetNode: "worker",
		Strategies: []string{domain.StrategyClusterIP},
	}, volumes)
	if copyPeaks["source"] != 2 || copyPeaks["stage"] != 3 {
		t.Fatalf("copy probe peaks=%v, want source/stage=2/3", copyPeaks)
	}

	localPeaks := migrationProbePodPeaks(planOptions{
		Operation:  domain.OperationCopy,
		TargetNode: "worker",
		Strategies: []string{domain.StrategyLocal},
	}, volumes)
	if localPeaks["source"] != 2 || localPeaks["stage"] != 3 {
		t.Fatalf("local probe peaks=%v, want source/stage=2/3", localPeaks)
	}

	localSameNamespace := make([]domain.PlannedVolume, len(volumes))
	for index := range volumes {
		localSameNamespace[index] = volumes[index]
		localSameNamespace[index].SourcePVC.Namespace = "application"
		localSameNamespace[index].DestinationPVC.Namespace = "application"
	}

	localSameNamespacePeaks := migrationProbePodPeaks(planOptions{
		Operation:  domain.OperationCopy,
		TargetNode: "worker",
		Strategies: []string{domain.StrategyLocal},
	}, localSameNamespace)
	if localSameNamespacePeaks["application"] != 5 {
		t.Fatalf(
			"same-namespace local probe peak=%v, want 5",
			localSameNamespacePeaks,
		)
	}

	offlineMountPeaks := migrationProbePodPeaks(planOptions{
		Operation:  domain.OperationMigrate,
		TargetNode: "worker",
		Strategies: []string{domain.StrategyMount},
	}, volumes)
	if offlineMountPeaks["source"] != 2 || offlineMountPeaks["stage"] != 3 {
		t.Fatalf("offline mount probe peaks=%v, want source/stage=2/3", offlineMountPeaks)
	}

	warmSameNamespace := make([]domain.PlannedVolume, len(volumes))
	for index := range volumes {
		warmSameNamespace[index] = volumes[index]
		warmSameNamespace[index].SourcePVC.Namespace = "application"
		warmSameNamespace[index].DestinationPVC.Namespace = "application"
	}

	warmPeaks := migrationProbePodPeaks(planOptions{
		Operation:     domain.OperationMigratePod,
		TargetNode:    "worker",
		Strategies:    []string{domain.StrategyMount},
		PrecopyPasses: 1,
	}, warmSameNamespace)
	if warmPeaks["application"] != 5 {
		t.Fatalf("warm same-namespace probe peak=%v, want 5", warmPeaks)
	}
}

func TestReserveResourceEstimateExcludesCopyChart(t *testing.T) {
	state := &planState{
		options: planOptions{
			Operation:        domain.OperationReserve,
			SourceNamespace:  "application",
			StagingNamespace: "staging",
			SessionNamespace: "staging",
			TargetNode:       "worker",
			Strategies:       []string{domain.StrategyClusterIP},
		},
		plannedVolumes: []domain.PlannedVolume{{
			SourcePVC:      domain.ObjectReference{Namespace: "application", Name: "data"},
			DestinationPVC: domain.ObjectReference{Namespace: "staging", Name: "copy"},
		}},
		totalStorage: resource.MustParse("1Gi"),
	}

	estimate := migrationNamespaceResourceEstimates(state)["staging"]
	if estimate.Pods != 1 || estimate.TerminatingPods != 1 ||
		estimate.NotTerminatingPods != 1 {
		t.Fatalf("reserve Pod peaks=%#v", estimate)
	}

	if estimate.Jobs != 0 || estimate.Deployments != 0 || estimate.Services != 0 ||
		estimate.Secrets != 0 || estimate.ServiceAccounts != 0 {
		t.Fatalf("reserve estimate includes copy chart objects: %#v", estimate)
	}
}

func TestPlanFiltersStrategiesBeforeQuotaEstimation(t *testing.T) {
	objects := plannerObjects("2Gi")
	for _, object := range objects {
		if pv, ok := object.(*corev1.PersistentVolume); ok {
			pv.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      corev1.LabelHostname,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{"other-worker"},
					}},
				}},
			}}
		}
	}

	objects = append(objects, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "no-jobs"},
		Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
			corev1.ResourceName("count/jobs.batch"): resource.MustParse("0"),
		}},
	})

	plan, err := New(plannerClient(objects...), nil).plan(context.Background(), planOptions{
		Operation:          domain.OperationCopy,
		SessionID:          "filtered-quota",
		SourceNamespace:    "app",
		TemporaryNamespace: "app",
		StagingNamespace:   "app",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{domain.StrategyMount, domain.StrategyLocal},
	})
	if err != nil {
		t.Fatal(err)
	}

	if !plan.Ready {
		t.Fatalf("filtered local strategy should fit zero Job quota: %#v", plan.Checks)
	}

	if plan.TemporaryUsage.Jobs != 0 || len(plan.Strategies) != 1 ||
		plan.Strategies[0] != domain.StrategyLocal {
		t.Fatalf("resources=%#v strategies=%v", plan.TemporaryUsage, plan.Strategies)
	}
}

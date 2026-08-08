package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func plannerClient(objects ...runtime.Object) *kubernetesfake.Clientset {
	client := kubernetesfake.NewClientset(objects...)
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action clienttesting.Action) (bool, runtime.Object, error) {
		review := action.(clienttesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).DeepCopy()
		review.Status.Allowed = true
		return true, review, nil
	})
	return client
}

func TestDestinationPVCNameTrimsTruncatedDNSBoundaries(t *testing.T) {
	name := destinationPVCName(Options{SessionID: "pod-full-v2-20260807"}, "pod-data-a", 0)
	if name != "pod-data-a-migrated-pod-full-v2" {
		t.Fatalf("name=%q", name)
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
		t.Fatalf("name=%q is invalid: %v", name, problems)
	}
	long := destinationPVCName(Options{SessionID: "migration-20260807"}, strings.Repeat("a", 250), 0)
	if problems := validation.IsDNS1123Subdomain(long); len(problems) != 0 || strings.HasSuffix(long, "-") {
		t.Fatalf("long name=%q problems=%v", long, problems)
	}
}

func TestPlanRejectsUnsupportedStrategiesAndDestinationCount(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	plan, err := New(client, nil).Plan(context.Background(), Options{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		DestinationPVCs:    []string{"data-new", "extra"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
		Strategies:         []string{"mount", "unsupported"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "strategy") || !hasFailedCheck(plan, "destination-pvc") {
		t.Fatalf("plan should reject invalid arguments: %#v", plan.Checks)
	}
}

func TestCopySupportsOfflineAndOnlineModesAcrossNamespaces(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "archive"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "node-a"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "writer"},
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Volumes:  []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "default"}},
	)
	base := Options{
		Operation:            domain.OperationCopy,
		SessionID:            "copy",
		SourceNamespace:      "app",
		TemporaryNamespace:   "archive",
		DestinationNamespace: "archive",
		StagingNamespace:     "archive",
		SessionNamespace:     "system",
		SourcePVCs:           []string{"data"},
		TargetNode:           "node-b",
		DestinationClass:     "fast",
	}
	offline, err := New(plannerClient(objects...), nil).Plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if offline.Ready || !hasFailedCheck(offline, "pvc-consumers") {
		t.Fatalf("offline copy checks=%#v", offline.Checks)
	}

	base.Online = true
	online, err := New(plannerClient(objects...), nil).Plan(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if !online.Ready {
		t.Fatalf("online copy checks=%#v", online.Checks)
	}
	if online.SessionSpec.WorkflowOptions().SourceNode != "node-a" || !online.SessionSpec.Online() || online.Volumes[0].DestinationPVC.Name != "data" {
		t.Fatalf("online session=%#v volume=%#v", online.SessionSpec, online.Volumes[0])
	}
	withPod := base
	withPod.SessionID = "copy-pod"
	withPod.SourcePVCs = nil
	withPod.PodName = "writer"
	withPod.SourceNode = ""
	selectedPod, err := New(plannerClient(objects...), nil).Plan(context.Background(), withPod)
	if err != nil {
		t.Fatal(err)
	}
	if !selectedPod.Ready || selectedPod.Workload.Adapter != domain.WorkloadNone || selectedPod.SessionSpec.WorkflowOptions().SourceNode != "node-a" {
		t.Fatalf("pod copy plan=%#v", selectedPod)
	}
}

func plannerObjects(capacity string) []runtime.Object {
	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	storageClass := "fast"
	mode := corev1.PersistentVolumeFilesystem
	pvcUID := types.UID("pvc-uid")
	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "system"}},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "node-b", corev1.LabelTopologyZone: "zone-b"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
		&storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: storageClass},
			Provisioner:       "example.csi.io",
			VolumeBindingMode: &wffc,
			AllowedTopologies: []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{Key: corev1.LabelTopologyZone, Values: []string{"zone-b"}}}}},
		},
		&storagev1.CSINode{
			ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
			Spec:       storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "example.csi.io", NodeID: "node-b"}}},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data", UID: pvcUID, ResourceVersion: "10"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)}},
				StorageClassName: &storageClass,
				VolumeMode:       &mode,
				VolumeName:       "pv-source",
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-source", UID: types.UID("pv-uid"), ResourceVersion: "20"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				ClaimRef:                      &corev1.ObjectReference{Namespace: "app", Name: "data", UID: pvcUID},
				StorageClassName:              storageClass,
			},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
	}
}

func TestPlanModelsTopologyQuotaAndSessionIdentity(t *testing.T) {
	client := plannerClient(plannerObjects("2Gi")...)
	plan, err := New(client, nil).Plan(context.Background(), Options{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Ready {
		t.Fatalf("plan failed checks: %#v", plan.Checks)
	}
	if len(plan.Volumes) != 1 || plan.Volumes[0].BindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		t.Fatalf("volumes: %#v", plan.Volumes)
	}
	if plan.TemporaryUsage.StorageRequests != "2Gi" || plan.RollbackRetention.StorageRequests != "2Gi" {
		t.Fatalf("storage estimates: temporary=%s rollback=%s", plan.TemporaryUsage.StorageRequests, plan.RollbackRetention.StorageRequests)
	}
	if plan.TemporaryUsage.Secrets != 2 {
		t.Fatalf("temporary Secret estimate=%d, want 2", plan.TemporaryUsage.Secrets)
	}
	if plan.SessionSpec.Volumes[0].SourcePVC.UID != types.UID("pvc-uid") || plan.SessionSpec.Volumes[0].SourcePV.UID != types.UID("pv-uid") {
		t.Fatalf("session resource identities: %#v", plan.SessionSpec.Volumes[0])
	}
}

func TestPlanChecksSourceAndSessionNamespaceHelperQuotas(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "source-helper-limit"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceName("count/secrets"): resource.MustParse("0"),
			}},
		},
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "audit", Name: "session-limit"},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				corev1.ResourceName("count/configmaps"): resource.MustParse("0"),
			}},
		},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "audit"}},
	)
	plan, err := New(plannerClient(objects...), nil).Plan(context.Background(), Options{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "audit",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready {
		t.Fatal("plan unexpectedly passed source/session helper quota checks")
	}
	var sourceFound, sessionFound bool
	for _, check := range plan.Checks {
		if check.Name != "resource-quota" || check.Passed {
			continue
		}
		if strings.Contains(check.Message, "app/source-helper-limit") {
			sourceFound = true
		}
		if strings.Contains(check.Message, "audit/session-limit") {
			sessionFound = true
		}
	}
	if !sourceFound || !sessionFound {
		t.Fatalf("missing source/session quota failures: source=%t session=%t checks=%#v", sourceFound, sessionFound, plan.Checks)
	}
}

func TestPlanRejectsUnavailableSourceNode(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Node)
	}{
		{name: "not ready", mutate: func(node *corev1.Node) {
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		}},
		{name: "unschedulable", mutate: func(node *corev1.Node) {
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			node.Spec.Unschedulable = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			objects = append(objects, &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "node-a"}},
			})
			test.mutate(objects[len(objects)-1].(*corev1.Node))
			plan, err := New(plannerClient(objects...), nil).Plan(context.Background(), Options{
				SessionID: "migration", SourceNamespace: "app", TemporaryNamespace: "system", StagingNamespace: "system", SessionNamespace: "system",
				SourcePVCs: []string{"data"}, SourceNode: "node-a", TargetNode: "node-b", DestinationClass: "fast",
			})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Ready {
				t.Fatal("plan unexpectedly accepted an unavailable source node")
			}
			for _, check := range plan.Checks {
				if check.Name == "source-node" && strings.Contains(check.Message, "Ready") {
					return
				}
			}
			t.Fatalf("source-node readiness check missing: %#v", plan.Checks)
		})
	}
}

func TestPlanEvaluatesEveryQuotaAndLimitRangeWithQuantities(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "storage"},
			Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("10Gi")}},
			Status:     corev1.ResourceQuotaStatus{Used: corev1.ResourceList{corev1.ResourceRequestsStorage: resource.MustParse("9Gi")}},
		},
		&corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "claims"},
			Spec:       corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1")}},
			Status:     corev1.ResourceQuotaStatus{Used: corev1.ResourceList{corev1.ResourcePersistentVolumeClaims: resource.MustParse("1")}},
		},
		&corev1.LimitRange{
			ObjectMeta: metav1.ObjectMeta{Namespace: "system", Name: "pvc-size"},
			Spec:       corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{Type: corev1.LimitTypePersistentVolumeClaim, Max: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1536Mi")}}}},
		},
	)
	client := plannerClient(objects...)
	plan, err := New(client, nil).Plan(context.Background(), Options{
		SessionID:          "migration",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		TargetNode:         "node-b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready {
		t.Fatal("plan unexpectedly passed")
	}
	messages := make([]string, 0)
	for _, check := range plan.Checks {
		if !check.Passed {
			messages = append(messages, check.Message)
		}
	}
	joined := strings.Join(messages, " ")
	for _, expected := range []string{"storage", "claims", "pvc-size"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("failed checks omit %q: %s", expected, joined)
		}
	}
}

func TestSchedulingIssuesCoverSelectorAffinityAndTaints(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"disk": "ssd", "count": "3"}},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule}}},
	}
	spec := corev1.PodSpec{
		NodeSelector: map[string]string{"disk": "hdd"},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "count", Operator: corev1.NodeSelectorOpGt, Values: []string{"4"}}}}},
		}}},
	}
	issues := schedulingIssues(spec, node)
	if len(issues) != 3 {
		t.Fatalf("issues=%v", issues)
	}
}

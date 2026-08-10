package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestStorageCapacityReportsSufficientCapacityAndHeadroom(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"topology.kubernetes.io/zone": "zone-b"}}}
	capacity := storageCapacityObject("fast-capacity", "fast", &metav1.LabelSelector{MatchLabels: map[string]string{"topology.kubernetes.io/zone": "zone-b"}}, "12Gi", "8Gi")
	inventory := New(plannerClient(capacity), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	evaluation := inventory.evaluate(node, capacityDemandFor("fast", "example.csi.io", "8Gi", "8Gi", 1))
	if evaluation.report.Status != domain.StorageCapacitySufficient {
		t.Fatalf("status=%q report=%#v", evaluation.report.Status, evaluation.report)
	}
	if evaluation.report.ReportedCapacity != "12Gi" || evaluation.report.MaximumVolumeSize != "8Gi" || evaluation.report.MatchingObjects != 1 {
		t.Fatalf("report=%#v", evaluation.report)
	}
	if evaluation.surplus.Cmp(resource.MustParse("4Gi")) != 0 {
		t.Fatalf("surplus=%s", evaluation.surplus.String())
	}
}

func TestStorageCapacityRejectsCapacityAndMaximumVolumeLimits(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}
	tests := []struct {
		name     string
		capacity string
		maximum  string
		message  string
	}{
		{name: "available capacity", capacity: "4Gi", maximum: "8Gi", message: "insufficient capacity"},
		{name: "maximum volume", capacity: "20Gi", maximum: "4Gi", message: "maximumVolumeSize"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object := storageCapacityObject("capacity", "fast", &metav1.LabelSelector{}, tt.capacity, tt.maximum)
			inventory := New(plannerClient(object), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
			evaluation := inventory.evaluate(node, capacityDemandFor("fast", "example.csi.io", "8Gi", "8Gi", 1))
			if evaluation.report.Status != domain.StorageCapacityInsufficient || !strings.Contains(evaluation.report.Message, tt.message) {
				t.Fatalf("report=%#v", evaluation.report)
			}
		})
	}
}

func TestStorageCapacityMissingObjectsFollowPolicy(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}
	plan := &domain.MigrationPlan{Ready: true}
	inventory := New(plannerClient(), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	volume := plannedCapacityVolume("data", "fast", "4Gi")
	New(nil, nil).checkStorageCapacity(plan, node, []domain.PlannedVolume{volume}, inventory, domain.CapacityAwarenessAuto)
	if !plan.Ready || len(plan.Checks) != 1 || plan.Checks[0].Severity != domain.SeverityWarning || plan.StorageCapacity[0].Status != domain.StorageCapacityUnknown {
		t.Fatalf("auto plan=%#v", plan)
	}

	required := &domain.MigrationPlan{Ready: true}
	New(nil, nil).checkStorageCapacity(required, node, []domain.PlannedVolume{volume}, inventory, domain.CapacityAwarenessRequire)
	if required.Ready || len(required.Checks) != 1 || required.Checks[0].Severity != domain.SeverityError {
		t.Fatalf("required plan=%#v", required)
	}
}

func TestStorageCapacityTopologyMismatchIsInsufficient(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"topology.kubernetes.io/zone": "zone-b"}}}
	object := storageCapacityObject("zone-a", "fast", &metav1.LabelSelector{MatchLabels: map[string]string{"topology.kubernetes.io/zone": "zone-a"}}, "20Gi", "20Gi")
	inventory := New(plannerClient(object), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	evaluation := inventory.evaluate(node, capacityDemandFor("fast", "example.csi.io", "4Gi", "4Gi", 1))
	if evaluation.report.Status != domain.StorageCapacityInsufficient || !strings.Contains(evaluation.report.Message, "matches target node") {
		t.Fatalf("report=%#v", evaluation.report)
	}
}

func TestStorageCapacityNilCapacityRemainsUnknown(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}
	object := storageCapacityObject("unknown", "fast", &metav1.LabelSelector{}, "", "8Gi")
	inventory := New(plannerClient(object), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	evaluation := inventory.evaluate(node, capacityDemandFor("fast", "example.csi.io", "4Gi", "4Gi", 1))
	if evaluation.report.Status != domain.StorageCapacityUnknown || !strings.Contains(evaluation.report.Message, "no reported available capacity") {
		t.Fatalf("report=%#v", evaluation.report)
	}
}

func TestStorageCapacityAggregatesPVCsWithoutSummingOverlappingReports(t *testing.T) {
	volumes := []domain.PlannedVolume{
		plannedCapacityVolume("data", "fast", "3Gi"),
		plannedCapacityVolume("logs", "fast", "5Gi"),
	}
	demands, err := capacityDemands(volumes)
	if err != nil {
		t.Fatal(err)
	}
	demand := demands["fast"]
	if demand.total.Cmp(resource.MustParse("8Gi")) != 0 || demand.largest.Cmp(resource.MustParse("5Gi")) != 0 || demand.volumes != 2 {
		t.Fatalf("demand=%#v", demand)
	}
	objects := []runtime.Object{
		storageCapacityObject("pool-a", "fast", &metav1.LabelSelector{}, "5Gi", "5Gi"),
		storageCapacityObject("pool-b", "fast", &metav1.LabelSelector{}, "5Gi", "5Gi"),
	}
	inventory := New(plannerClient(objects...), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	evaluation := inventory.evaluate(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}, demand)
	if evaluation.report.Status != domain.StorageCapacityInsufficient || evaluation.report.ReportedCapacity != "5Gi" || evaluation.report.MatchingObjects != 2 {
		t.Fatalf("report=%#v", evaluation.report)
	}
}

func TestStorageCapacityTopologyNilAndInvalidRemainUnavailable(t *testing.T) {
	tests := []struct {
		name       string
		topology   *metav1.LabelSelector
		wantStatus domain.StorageCapacityStatus
	}{
		{name: "nil means inaccessible", topology: nil, wantStatus: domain.StorageCapacityInsufficient},
		{name: "invalid selector", topology: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "zone", Operator: "Invalid"}}}, wantStatus: domain.StorageCapacityUnknown},
		{name: "empty selector matches all", topology: &metav1.LabelSelector{}, wantStatus: domain.StorageCapacitySufficient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			object := storageCapacityObject("capacity", "fast", tt.topology, "10Gi", "10Gi")
			inventory := New(plannerClient(object), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
			evaluation := inventory.evaluate(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}, capacityDemandFor("fast", "example.csi.io", "4Gi", "4Gi", 1))
			if evaluation.report.Status != tt.wantStatus {
				t.Fatalf("status=%q report=%#v", evaluation.report.Status, evaluation.report)
			}
		})
	}
}

func TestStorageCapacityOffSkipsAPIRead(t *testing.T) {
	client := kubernetesfake.NewClientset()
	calls := 0
	client.PrependReactor("list", "csistoragecapacities", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		return false, nil, nil
	})
	inventory := New(client, nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessOff)
	if calls != 0 || inventory.loaded || inventory.err != nil {
		t.Fatalf("calls=%d inventory=%#v", calls, inventory)
	}
}

func TestStorageCapacityAutoTargetScorePrefersKnownHeadroom(t *testing.T) {
	objects := []runtime.Object{
		storageCapacityObject("node-a", "fast", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelHostname: "node-a"}}, "8Gi", "8Gi"),
		storageCapacityObject("node-b", "fast", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelHostname: "node-b"}}, "20Gi", "20Gi"),
	}
	inventory := New(plannerClient(objects...), nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	volumes := []domain.PlannedVolume{plannedCapacityVolume("data", "fast", "4Gi")}
	knownA, unknownA, _, compatibleA := capacityScore(inventory, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "node-a"}}}, volumes)
	knownB, unknownB, surplusB, compatibleB := capacityScore(inventory, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{corev1.LabelHostname: "node-b"}}}, volumes)
	if !compatibleA || !compatibleB || knownA != 1 || knownB != 1 || unknownA != 0 || unknownB != 0 || surplusB.Cmp(resource.MustParse("16Gi")) != 0 {
		t.Fatalf("scores A=(%d,%d,%t) B=(%d,%d,%s,%t)", knownA, unknownA, compatibleA, knownB, unknownB, surplusB.String(), compatibleB)
	}
}

func TestPlanAutoTargetPrefersGreaterReportedCapacity(t *testing.T) {
	objects := plannerObjects("2Gi")
	objects = append(objects,
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{corev1.LabelHostname: "node-a", corev1.LabelTopologyZone: "zone-b"}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
		},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: "example.csi.io", NodeID: "node-a"}}}},
		storageCapacityObject("node-a", "fast", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelHostname: "node-a"}}, "20Gi", "20Gi"),
		storageCapacityObject("node-b", "fast", &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelHostname: "node-b"}}, "10Gi", "10Gi"),
	)
	plan, err := New(plannerClient(objects...), nil).Plan(context.Background(), Options{
		SessionID:          "capacity-target",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		DestinationClass:   "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Ready || plan.TargetNode != "node-a" || len(plan.StorageCapacity) != 1 || plan.StorageCapacity[0].ReportedCapacity != "20Gi" {
		t.Fatalf("ready=%t target=%q capacity=%#v checks=%#v", plan.Ready, plan.TargetNode, plan.StorageCapacity, plan.Checks)
	}
}

func TestPlanCapacityAwarenessPolicies(t *testing.T) {
	base := Options{
		SessionID:          "capacity-policy",
		SourceNamespace:    "app",
		TemporaryNamespace: "system",
		StagingNamespace:   "system",
		SessionNamespace:   "system",
		SourcePVCs:         []string{"data"},
		DestinationClass:   "fast",
		TargetNode:         "node-b",
	}
	tests := []struct {
		name        string
		mode        domain.CapacityAwareness
		capacity    *storagev1.CSIStorageCapacity
		wantReady   bool
		wantReports int
		wantStatus  domain.StorageCapacityStatus
	}{
		{name: "auto unknown", mode: domain.CapacityAwarenessAuto, wantReady: true, wantReports: 1, wantStatus: domain.StorageCapacityUnknown},
		{name: "require unknown", mode: domain.CapacityAwarenessRequire, wantReady: false, wantReports: 1, wantStatus: domain.StorageCapacityUnknown},
		{name: "auto insufficient", mode: domain.CapacityAwarenessAuto, capacity: storageCapacityObject("small", "fast", &metav1.LabelSelector{}, "1Gi", "2Gi"), wantReady: false, wantReports: 1, wantStatus: domain.StorageCapacityInsufficient},
		{name: "off ignores report", mode: domain.CapacityAwarenessOff, capacity: storageCapacityObject("small", "fast", &metav1.LabelSelector{}, "1Gi", "2Gi"), wantReady: true, wantReports: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := plannerObjects("2Gi")
			if tt.capacity != nil {
				objects = append(objects, tt.capacity)
			}
			options := base
			options.CapacityAwareness = tt.mode
			plan, err := New(plannerClient(objects...), nil).Plan(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Ready != tt.wantReady || len(plan.StorageCapacity) != tt.wantReports {
				t.Fatalf("ready=%t reports=%#v checks=%#v", plan.Ready, plan.StorageCapacity, plan.Checks)
			}
			if tt.wantReports > 0 && plan.StorageCapacity[0].Status != tt.wantStatus {
				t.Fatalf("status=%q report=%#v", plan.StorageCapacity[0].Status, plan.StorageCapacity[0])
			}
		})
	}
}

func TestStorageCapacityListErrorIsUnknownOrRequired(t *testing.T) {
	client := plannerClient()
	client.PrependReactor("list", "csistoragecapacities", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(storagev1.Resource("csistoragecapacities"), "", errors.New("denied"))
	})
	inventory := New(client, nil).loadStorageCapacity(context.Background(), domain.CapacityAwarenessAuto)
	if inventory.err == nil || inventory.loaded {
		t.Fatalf("inventory=%#v", inventory)
	}
	evaluation := inventory.evaluate(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}, capacityDemandFor("fast", "example.csi.io", "1Gi", "1Gi", 1))
	if evaluation.report.Status != domain.StorageCapacityUnknown || !strings.Contains(evaluation.report.Message, "read CSIStorageCapacity") {
		t.Fatalf("report=%#v", evaluation.report)
	}
	for _, tt := range []struct {
		mode      domain.CapacityAwareness
		wantReady bool
	}{
		{mode: domain.CapacityAwarenessAuto, wantReady: true},
		{mode: domain.CapacityAwarenessRequire, wantReady: false},
	} {
		plan := &domain.MigrationPlan{Ready: true}
		New(nil, nil).checkStorageCapacity(plan, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}, []domain.PlannedVolume{plannedCapacityVolume("data", "fast", "1Gi")}, inventory, tt.mode)
		if plan.Ready != tt.wantReady {
			t.Fatalf("mode=%s ready=%t checks=%#v", tt.mode, plan.Ready, plan.Checks)
		}
	}
}

func TestCapacityAwarenessModes(t *testing.T) {
	for _, mode := range []domain.CapacityAwareness{domain.CapacityAwarenessAuto, domain.CapacityAwarenessRequire, domain.CapacityAwarenessOff} {
		if !validCapacityAwareness(mode) {
			t.Fatalf("mode %q rejected", mode)
		}
	}
	if validCapacityAwareness("strict") {
		t.Fatal("unsupported mode accepted")
	}
	options := Options{
		SessionID: "invalid-capacity-mode", SourceNamespace: "app", TemporaryNamespace: "system", StagingNamespace: "system", SessionNamespace: "system",
		SourcePVCs: []string{"data"}, DestinationClass: "fast", TargetNode: "node-b", CapacityAwareness: "strict",
	}
	plan, err := New(plannerClient(plannerObjects("1Gi")...), nil).Plan(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Ready || !hasFailedCheck(plan, "capacity-awareness") || len(plan.StorageCapacity) != 0 {
		t.Fatalf("plan=%#v", plan)
	}
}

func storageCapacityObject(name, class string, topology *metav1.LabelSelector, capacity, maximum string) *storagev1.CSIStorageCapacity {
	object := &storagev1.CSIStorageCapacity{ObjectMeta: metav1.ObjectMeta{Namespace: "storage-system", Name: name}, StorageClassName: class, NodeTopology: topology}
	if capacity != "" {
		value := resource.MustParse(capacity)
		object.Capacity = &value
	}
	if maximum != "" {
		value := resource.MustParse(maximum)
		object.MaximumVolumeSize = &value
	}
	return object
}

func capacityDemandFor(class, provisioner, total, largest string, volumes int) capacityDemand {
	return capacityDemand{storageClass: class, provisioner: provisioner, total: resource.MustParse(total), largest: resource.MustParse(largest), volumes: volumes}
}

func plannedCapacityVolume(name, class, capacity string) domain.PlannedVolume {
	return domain.PlannedVolume{SourcePVC: domain.ObjectReference{Namespace: "app", Name: name}, StorageClass: class, CSIProvisioner: "example.csi.io", Capacity: capacity}
}

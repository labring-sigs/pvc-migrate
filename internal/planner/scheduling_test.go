package planner

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectorRequirementMatchesOperators(t *testing.T) {
	labels := map[string]string{"zone": "b", "size": "10"}
	tests := []struct {
		name        string
		requirement corev1.NodeSelectorRequirement
		actual      string
		labels      map[string]string
		want        bool
	}{
		{name: "in", requirement: corev1.NodeSelectorRequirement{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a", "b"}}, actual: "b", labels: labels, want: true},
		{name: "in missing", requirement: corev1.NodeSelectorRequirement{Key: "missing", Operator: corev1.NodeSelectorOpIn, Values: []string{""}}, labels: labels},
		{name: "not in missing", requirement: corev1.NodeSelectorRequirement{Key: "missing", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a"}}, labels: labels, want: true},
		{name: "exists", requirement: corev1.NodeSelectorRequirement{Key: "zone", Operator: corev1.NodeSelectorOpExists}, actual: "b", labels: labels, want: true},
		{name: "does not exist", requirement: corev1.NodeSelectorRequirement{Key: "missing", Operator: corev1.NodeSelectorOpDoesNotExist}, labels: labels, want: true},
		{name: "greater than", requirement: corev1.NodeSelectorRequirement{Key: "size", Operator: corev1.NodeSelectorOpGt, Values: []string{"9"}}, actual: "10", labels: labels, want: true},
		{name: "less than", requirement: corev1.NodeSelectorRequirement{Key: "size", Operator: corev1.NodeSelectorOpLt, Values: []string{"11"}}, actual: "10", labels: labels, want: true},
		{name: "numeric malformed value count", requirement: corev1.NodeSelectorRequirement{Key: "size", Operator: corev1.NodeSelectorOpGt, Values: []string{"9", "10"}}, actual: "10", labels: labels},
		{name: "numeric malformed actual", requirement: corev1.NodeSelectorRequirement{Key: "size", Operator: corev1.NodeSelectorOpGt, Values: []string{"9"}}, actual: "large", labels: labels},
		{name: "field exists", requirement: corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-b"}}, actual: "node-b", labels: nil, want: true},
		{name: "unknown operator", requirement: corev1.NodeSelectorRequirement{Key: "zone", Operator: "Unknown"}, actual: "b", labels: labels},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectorRequirementMatches(tt.requirement, tt.actual, tt.labels); got != tt.want {
				t.Fatalf("selectorRequirementMatches()=%t want=%t", got, tt.want)
			}
		})
	}
}

func TestSchedulingAcceptsAlternativeAffinityTermAndTolerations(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"disk": "ssd", "zone": "b", "size": "10"}},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule},
			{Key: "maintenance", Effect: corev1.TaintEffectNoExecute},
			{Key: "cost", Value: "high", Effect: corev1.TaintEffectPreferNoSchedule},
		}},
	}
	spec := corev1.PodSpec{
		NodeSelector: map[string]string{"disk": "ssd"},
		Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}}},
				{
					MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}, {Key: "size", Operator: corev1.NodeSelectorOpGt, Values: []string{"5"}}},
					MatchFields:      []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-b"}}},
				},
			},
		}}},
		Tolerations: []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "db", Effect: corev1.TaintEffectNoSchedule},
			{Key: "maintenance", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
		},
	}
	if issues := schedulingIssues(spec, node); len(issues) != 0 {
		t.Fatalf("scheduling issues: %v", issues)
	}
}

func TestSchedulingIgnoresObservedNodeNameForManagedWorkload(t *testing.T) {
	spec := &corev1.Pod{Spec: corev1.PodSpec{NodeName: "node-a"}}
	issues := schedulingIssuesForTarget(spec, domain.WorkloadSpec{Adapter: domain.WorkloadStatefulSet}, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}})
	if len(issues) != 0 {
		t.Fatalf("issues=%v", issues)
	}
}

func TestResourceFitUsesInitContainerAndOverheadRequests(t *testing.T) {
	spec := corev1.PodSpec{
		Containers: []corev1.Container{
			{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			}}},
			{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("250m"),
			}}},
		},
		InitContainers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}}}},
		Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
	}
	requests := podResourceRequests(spec)
	cpu := requests[corev1.ResourceCPU]
	if cpu.Cmp(resource.MustParse("2.1")) != 0 {
		t.Fatalf("cpu requests=%s", cpu.String())
	}
	memory := requests[corev1.ResourceMemory]
	if memory.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Fatalf("memory requests=%s", memory.String())
	}
	node := &corev1.Node{Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("2"),
		corev1.ResourceMemory: resource.MustParse("2Gi"),
	}}}
	issues, known := resourceFitIssues(spec, node)
	if !known || len(issues) != 1 || issues[0] != "Pod resource request cpu=2100m exceeds node allocatable cpu=2" {
		t.Fatalf("known=%t issues=%v", known, issues)
	}
}

func TestResourceFitReportsUnknownAllocatableResource(t *testing.T) {
	spec := corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceName("example.com/gpu"): resource.MustParse("1"),
	}}}}}
	issues, known := resourceFitIssues(spec, &corev1.Node{Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2"),
	}}})
	if known || len(issues) != 0 {
		t.Fatalf("known=%t issues=%v", known, issues)
	}
}

func TestPodMigrationIssuesRejectsUnverifiablePlacementAndNodeLocalData(t *testing.T) {
	spec := corev1.PodSpec{
		SchedulerName: "custom-scheduler",
		Volumes: []corev1.Volume{
			{Name: "cache", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "node-data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/data"}}},
			{Name: "scratch", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}},
		},
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule}},
	}
	issues := podMigrationIssues(spec, "node-a", "node-b")
	if len(issues) != 4 {
		t.Fatalf("issues=%v", issues)
	}
}

func TestPodMigrationIssuesIgnoresEmptyDir(t *testing.T) {
	spec := corev1.PodSpec{Volumes: []corev1.Volume{{Name: "dshm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}}}}}
	if issues := podMigrationIssues(spec, "node-a", "node-b"); len(issues) != 0 {
		t.Fatalf("issues=%v", issues)
	}
}

func TestPodMigrationIssuesAllowsNodeLocalDataOnSameNode(t *testing.T) {
	spec := corev1.PodSpec{Volumes: []corev1.Volume{{Name: "node-data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/data"}}}}}
	if issues := podMigrationIssues(spec, "node-a", "node-a"); len(issues) != 0 {
		t.Fatalf("issues=%v", issues)
	}
}

func TestToleratesWildcardExistsAndHonorsEffect(t *testing.T) {
	taint := corev1.Taint{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule}
	if !tolerates([]corev1.Toleration{{Operator: corev1.TolerationOpExists}}, taint) {
		t.Fatal("wildcard Exists toleration should match")
	}
	if tolerates([]corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "db", Effect: corev1.TaintEffectNoExecute}}, taint) {
		t.Fatal("different effect should not match")
	}
	if tolerates([]corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "other"}}, taint) {
		t.Fatal("different value should not match")
	}
}

func TestAllowedTopologiesUseORBetweenTermsAndANDBetweenExpressions(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"zone": "b", "rack": "2"}}}
	sc := &storagev1.StorageClass{AllowedTopologies: []corev1.TopologySelectorTerm{
		{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{Key: "zone", Values: []string{"a"}}}},
		{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{Key: "zone", Values: []string{"b"}}, {Key: "rack", Values: []string{"2"}}}},
	}}
	if !matchesAllowedTopologies(sc, node) {
		t.Fatal("second topology term should match")
	}
	sc.AllowedTopologies[1].MatchLabelExpressions[1].Values = []string{"3"}
	if matchesAllowedTopologies(sc, node) {
		t.Fatal("all expressions within a topology term must match")
	}
}

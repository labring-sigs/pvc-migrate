package planner

import (
	"context"
	"fmt"
	"sort"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type storageCapacityInventory struct {
	mode   domain.CapacityAwareness
	loaded bool
	items  []storagev1.CSIStorageCapacity
	err    error
}

type capacityDemand struct {
	storageClass string
	provisioner  string
	total        resource.Quantity
	largest      resource.Quantity
	volumes      int
}

type capacityEvaluation struct {
	report  domain.StorageCapacityReport
	surplus resource.Quantity
}

func validCapacityAwareness(value domain.CapacityAwareness) bool {
	switch value {
	case domain.CapacityAwarenessAuto, domain.CapacityAwarenessRequire, domain.CapacityAwarenessOff:
		return true
	default:
		return false
	}
}

func (p *Planner) loadStorageCapacity(ctx context.Context, mode domain.CapacityAwareness) *storageCapacityInventory {
	inventory := &storageCapacityInventory{mode: mode}
	if mode == domain.CapacityAwarenessOff || !validCapacityAwareness(mode) {
		return inventory
	}
	items, err := p.client.StorageV1().CSIStorageCapacities(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		inventory.err = err
		return inventory
	}
	inventory.loaded = true
	inventory.items = append([]storagev1.CSIStorageCapacity(nil), items.Items...)
	return inventory
}

func capacityDemands(volumes []domain.PlannedVolume) (map[string]capacityDemand, error) {
	demands := make(map[string]capacityDemand)
	for _, volume := range volumes {
		capacity, err := resource.ParseQuantity(volume.Capacity)
		if err != nil {
			return nil, fmt.Errorf("parse PVC %s/%s capacity %q: %w", volume.SourcePVC.Namespace, volume.SourcePVC.Name, volume.Capacity, err)
		}
		if capacity.Sign() <= 0 {
			return nil, fmt.Errorf("PVC %s/%s capacity %s must be positive", volume.SourcePVC.Namespace, volume.SourcePVC.Name, capacity.String())
		}
		demand, ok := demands[volume.StorageClass]
		if !ok {
			demand = capacityDemand{
				storageClass: volume.StorageClass,
				provisioner:  volume.CSIProvisioner,
				total:        resource.MustParse("0"),
				largest:      resource.MustParse("0"),
			}
		}
		demand.total.Add(capacity)
		if capacity.Cmp(demand.largest) > 0 {
			demand.largest = capacity.DeepCopy()
		}
		demand.volumes++
		demands[volume.StorageClass] = demand
	}
	return demands, nil
}

func capacityScore(inventory *storageCapacityInventory, node *corev1.Node, volumes []domain.PlannedVolume) (known, unknown int, surplus resource.Quantity, compatible bool) {
	surplus = resource.MustParse("0")
	if inventory == nil || inventory.mode == domain.CapacityAwarenessOff || !inventory.loaded {
		return 0, 0, surplus, true
	}
	demands, err := capacityDemands(volumes)
	if err != nil {
		return 0, len(demands), surplus, true
	}
	compatible = true
	for _, demand := range demands {
		evaluation := inventory.evaluate(node, demand)
		switch evaluation.report.Status {
		case domain.StorageCapacitySufficient:
			known++
			surplus.Add(evaluation.surplus)
		case domain.StorageCapacityInsufficient:
			compatible = false
		default:
			unknown++
		}
	}
	return known, unknown, surplus, compatible
}

func (inventory *storageCapacityInventory) evaluate(node *corev1.Node, demand capacityDemand) capacityEvaluation {
	report := domain.StorageCapacityReport{
		StorageClass:      demand.storageClass,
		CSIProvisioner:    demand.provisioner,
		TargetNode:        node.Name,
		RequestedCapacity: demand.total.String(),
		LargestVolume:     demand.largest.String(),
		Status:            domain.StorageCapacityUnknown,
	}
	surplus := resource.MustParse("0")
	if inventory == nil || !inventory.loaded {
		if inventory != nil && inventory.err != nil {
			report.Message = fmt.Sprintf("read CSIStorageCapacity: %v", inventory.err)
		} else {
			report.Message = "CSIStorageCapacity lookup is unavailable"
		}
		return capacityEvaluation{report: report, surplus: surplus}
	}

	classObjects := make([]storagev1.CSIStorageCapacity, 0)
	for _, item := range inventory.items {
		if item.StorageClassName == demand.storageClass {
			classObjects = append(classObjects, item)
		}
	}
	sort.SliceStable(classObjects, func(i, j int) bool {
		if classObjects[i].Namespace != classObjects[j].Namespace {
			return classObjects[i].Namespace < classObjects[j].Namespace
		}
		return classObjects[i].Name < classObjects[j].Name
	})
	report.PublishedObjects = len(classObjects)
	if len(classObjects) == 0 {
		report.Message = fmt.Sprintf("StorageClass %s has no published CSIStorageCapacity object; backend capacity is unknown", demand.storageClass)
		return capacityEvaluation{report: report, surplus: surplus}
	}

	nodeLabels := labels.Set{}
	for key, value := range node.Labels {
		nodeLabels[key] = value
	}
	if _, ok := nodeLabels[corev1.LabelHostname]; !ok && node.Name != "" {
		nodeLabels[corev1.LabelHostname] = node.Name
	}
	matching := make([]storagev1.CSIStorageCapacity, 0)
	invalidTopology := 0
	for _, item := range classObjects {
		if item.NodeTopology == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(item.NodeTopology)
		if err != nil {
			invalidTopology++
			continue
		}
		if selector.Matches(nodeLabels) {
			matching = append(matching, item)
		}
	}
	report.MatchingObjects = len(matching)
	if len(matching) == 0 {
		if invalidTopology > 0 {
			report.Message = fmt.Sprintf("StorageClass %s has %d invalid CSIStorageCapacity topology object(s); target node %s cannot be verified", demand.storageClass, invalidTopology, node.Name)
			return capacityEvaluation{report: report, surplus: surplus}
		}
		report.Status = domain.StorageCapacityInsufficient
		report.Message = fmt.Sprintf("no CSIStorageCapacity object for StorageClass %s matches target node %s", demand.storageClass, node.Name)
		return capacityEvaluation{report: report, surplus: surplus}
	}

	maxCapacity := (*resource.Quantity)(nil)
	maxVolume := (*resource.Quantity)(nil)
	unknownCapacity := false
	capacitySufficient := false
	for _, item := range matching {
		if item.Capacity != nil && item.Capacity.Sign() > 0 {
			value := item.Capacity.DeepCopy()
			if maxCapacity == nil || value.Cmp(*maxCapacity) > 0 {
				maxCapacity = &value
			}
		}
		if item.MaximumVolumeSize != nil {
			value := item.MaximumVolumeSize.DeepCopy()
			if maxVolume == nil || value.Cmp(*maxVolume) > 0 {
				maxVolume = &value
			}
			if demand.largest.Cmp(*item.MaximumVolumeSize) > 0 {
				continue
			}
		}
		if item.Capacity == nil {
			unknownCapacity = true
			continue
		}
		if item.Capacity.Sign() <= 0 || item.Capacity.Cmp(demand.total) < 0 {
			continue
		}
		capacitySufficient = true
		value := item.Capacity.DeepCopy()
		candidateSurplus := value.DeepCopy()
		candidateSurplus.Sub(demand.total)
		if candidateSurplus.Cmp(surplus) > 0 {
			surplus = candidateSurplus
		}
	}
	if maxCapacity != nil {
		report.ReportedCapacity = maxCapacity.String()
	}
	if maxVolume != nil {
		report.MaximumVolumeSize = maxVolume.String()
	}
	if capacitySufficient {
		report.Status = domain.StorageCapacitySufficient
		report.Message = fmt.Sprintf("StorageClass %s reports at least %s available on target node %s for %d PVC(s)", demand.storageClass, report.ReportedCapacity, node.Name, demand.volumes)
		return capacityEvaluation{report: report, surplus: surplus}
	}
	if unknownCapacity {
		report.Message = fmt.Sprintf("StorageClass %s has no reported available capacity for target node %s", demand.storageClass, node.Name)
		if report.MaximumVolumeSize != "" {
			report.Message += fmt.Sprintf("; maximumVolumeSize=%s permits the largest request", report.MaximumVolumeSize)
		}
		return capacityEvaluation{report: report, surplus: surplus}
	}
	report.Status = domain.StorageCapacityInsufficient
	report.Message = fmt.Sprintf("StorageClass %s reports insufficient capacity for %s requested on target node %s", demand.storageClass, demand.total.String(), node.Name)
	if report.MaximumVolumeSize != "" && demand.largest.Cmp(resource.MustParse(report.MaximumVolumeSize)) > 0 {
		report.Message = fmt.Sprintf("StorageClass %s maximumVolumeSize=%s is below the largest requested volume %s", demand.storageClass, report.MaximumVolumeSize, demand.largest.String())
	}
	return capacityEvaluation{report: report, surplus: surplus}
}

func (p *Planner) checkStorageCapacity(plan *domain.MigrationPlan, node *corev1.Node, volumes []domain.PlannedVolume, inventory *storageCapacityInventory, mode domain.CapacityAwareness) {
	demands, err := capacityDemands(volumes)
	if err != nil {
		severity := domain.SeverityWarning
		passedResult := true
		if mode == domain.CapacityAwarenessRequire {
			severity = domain.SeverityError
			passedResult = false
		}
		plan.AddCheck(domain.Check{Name: "storage-capacity", Severity: severity, Passed: passedResult, Message: err.Error()})
		return
	}
	classes := make([]string, 0, len(demands))
	for class := range demands {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		evaluation := inventory.evaluate(node, demands[class])
		plan.StorageCapacity = append(plan.StorageCapacity, evaluation.report)
		check := domain.Check{Name: "storage-capacity", Severity: domain.SeverityInfo, Passed: true, Message: evaluation.report.Message}
		switch evaluation.report.Status {
		case domain.StorageCapacityInsufficient:
			check.Severity = domain.SeverityError
			check.Passed = false
		case domain.StorageCapacityUnknown:
			check.Severity = domain.SeverityWarning
			if mode == domain.CapacityAwarenessRequire {
				check.Severity = domain.SeverityError
				check.Passed = false
			}
		}
		plan.AddCheck(check)
	}
}

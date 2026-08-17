package planner

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/labring-sigs/pvc-migrate/internal/parallel"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type pvcReadResult struct {
	pvc *corev1.PersistentVolumeClaim
	err error
}

type pvReadResult struct {
	pv  *corev1.PersistentVolume
	err error
}

type storageClassReadResult struct {
	name string
	sc   *storagev1.StorageClass
	err  error
}

type planInventory struct {
	pvcs              []pvcReadResult
	pvs               []pvReadResult
	storageClasses    map[string]*storagev1.StorageClass
	storageClassError map[string]error
	namespacePods     []corev1.Pod
	namespacePodsErr  error
	targetNode        *corev1.Node
	targetNodeErr     error
	sourceNode        *corev1.Node
	sourceNodeErr     error
	nodes             []corev1.Node
	nodesErr          error
	csiNode           *storagev1.CSINode
	csiNodeErr        error
	capacity          *storageCapacityInventory
}

// loadPlanInventory reads independent Kubernetes objects in parallel, then
// leaves dependent PV and StorageClass reads in explicit stages. Results keep
// their input indexes so callers can preserve deterministic checks and plans.
func (p *Planner) loadPlanInventory(ctx context.Context, options Options, pvcNames []string, autoTargetNode bool) planInventory {
	p.logInfo("loading PVC and Pod inventory", "namespace", options.SourceNamespace, "pvcs", len(pvcNames))
	inventory := planInventory{
		pvcs:              make([]pvcReadResult, len(pvcNames)),
		pvs:               make([]pvReadResult, len(pvcNames)),
		storageClasses:    make(map[string]*storagev1.StorageClass),
		storageClassError: make(map[string]error),
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		parallel.For(len(pvcNames), func(index int) {
			name := pvcNames[index]
			inventory.pvcs[index].pvc, inventory.pvcs[index].err = p.client.CoreV1().PersistentVolumeClaims(options.SourceNamespace).Get(ctx, name, metav1.GetOptions{})
		})
	})
	if len(pvcNames) > 0 {
		wg.Go(func() {
			pods, err := p.client.CoreV1().Pods(options.SourceNamespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				inventory.namespacePodsErr = err
				return
			}
			if pods == nil {
				inventory.namespacePodsErr = fmt.Errorf("list Pods in %s returned an empty object", options.SourceNamespace)
				return
			}
			inventory.namespacePods = pods.Items
		})
		wg.Go(func() {
			inventory.capacity = p.loadStorageCapacity(ctx, options.CapacityAwareness)
		})
	}
	if options.TargetNode != "" {
		wg.Go(func() {
			inventory.targetNode, inventory.targetNodeErr = p.client.CoreV1().Nodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
		})
		if len(pvcNames) > 0 {
			wg.Go(func() {
				inventory.csiNode, inventory.csiNodeErr = p.client.StorageV1().CSINodes().Get(ctx, options.TargetNode, metav1.GetOptions{})
			})
		}
	}
	if options.SourceNode != "" && options.SourceNode != options.TargetNode {
		wg.Go(func() {
			inventory.sourceNode, inventory.sourceNodeErr = p.client.CoreV1().Nodes().Get(ctx, options.SourceNode, metav1.GetOptions{})
		})
	}
	if autoTargetNode && len(pvcNames) > 0 {
		wg.Go(func() {
			nodes, err := p.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if nodes != nil {
				inventory.nodes = nodes.Items
			}
			inventory.nodesErr = err
		})
	}
	wg.Wait()
	if options.SourceNode != "" && options.SourceNode == options.TargetNode {
		inventory.sourceNode = inventory.targetNode
		inventory.sourceNodeErr = inventory.targetNodeErr
	}

	classNames := make(map[string]struct{})
	pvIndexes := make([]int, 0, len(inventory.pvcs))
	for index, result := range inventory.pvcs {
		if result.err != nil || result.pvc == nil || result.pvc.Status.Phase != corev1.ClaimBound || result.pvc.Spec.VolumeName == "" {
			continue
		}
		sourceClassName := ""
		if result.pvc.Spec.StorageClassName != nil {
			sourceClassName = *result.pvc.Spec.StorageClassName
		}
		if sourceClassName != "" {
			classNames[sourceClassName] = struct{}{}
		}
		className := sourceClassName
		if options.DestinationClass != "" {
			className = options.DestinationClass
		}
		if className != "" {
			classNames[className] = struct{}{}
		}
		pvIndexes = append(pvIndexes, index)
	}
	classes := make([]string, 0, len(classNames))
	for name := range classNames {
		classes = append(classes, name)
	}
	sort.Strings(classes)
	results := make([]storageClassReadResult, len(classes))
	var dependentWG sync.WaitGroup
	dependentWG.Go(func() {
		parallel.For(len(pvIndexes), func(resultIndex int) {
			index := pvIndexes[resultIndex]
			volumeName := inventory.pvcs[index].pvc.Spec.VolumeName
			inventory.pvs[index].pv, inventory.pvs[index].err = p.client.CoreV1().PersistentVolumes().Get(ctx, volumeName, metav1.GetOptions{})
		})
	})
	dependentWG.Go(func() {
		parallel.For(len(classes), func(index int) {
			name := classes[index]
			results[index].name = name
			results[index].sc, results[index].err = p.client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
		})
	})
	p.logInfo("loading dependent PV and StorageClass inventory", "namespace", options.SourceNamespace, "pvcs", len(pvIndexes), "storageClasses", len(classes))
	dependentWG.Wait()
	for _, result := range results {
		inventory.storageClasses[result.name] = result.sc
		inventory.storageClassError[result.name] = result.err
	}
	return inventory
}

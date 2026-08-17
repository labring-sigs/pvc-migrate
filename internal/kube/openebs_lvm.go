package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

var openEBSLVMVolumeGVR = schema.GroupVersionResource{
	Group: "local.openebs.io", Version: "v1alpha1", Resource: "lvmvolumes",
}

// OpenEBSLVMSharedVolumeManager reads and explicitly enables the same-node
// concurrent mount setting maintained by the OpenEBS LVM CSI driver.
type OpenEBSLVMSharedVolumeManager interface {
	Shared(context.Context, string) (bool, error)
	EnsureShared(context.Context, string) (OpenEBSLVMSharedResult, error)
}

type OpenEBSLVMSharedResult struct {
	Reference      string
	PreviousShared string
	Changed        bool
}

type openEBSLVMSharedVolumeManager struct {
	typed   kubernetes.Interface
	dynamic dynamic.Interface
}

func NewOpenEBSLVMSharedVolumeManager(typed kubernetes.Interface, dynamicClient dynamic.Interface) OpenEBSLVMSharedVolumeManager {
	return &openEBSLVMSharedVolumeManager{typed: typed, dynamic: dynamicClient}
}

func (m *openEBSLVMSharedVolumeManager) Shared(ctx context.Context, sourcePVName string) (bool, error) {
	volume, shared, err := m.volume(ctx, sourcePVName)
	if err != nil {
		return false, err
	}
	if shared == "" || strings.EqualFold(shared, "no") {
		return false, nil
	}
	if strings.EqualFold(shared, "yes") {
		return true, nil
	}
	return false, domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("LVMVolume %s/%s has unsupported spec.shared value %q", volume.GetNamespace(), volume.GetName(), shared))
}

func (m *openEBSLVMSharedVolumeManager) EnsureShared(ctx context.Context, sourcePVName string) (OpenEBSLVMSharedResult, error) {
	volume, shared, err := m.volume(ctx, sourcePVName)
	if err != nil {
		return OpenEBSLVMSharedResult{}, err
	}
	result := OpenEBSLVMSharedResult{
		Reference:      fmt.Sprintf("LVMVolume %s/%s", volume.GetNamespace(), volume.GetName()),
		PreviousShared: shared,
	}
	if strings.EqualFold(shared, "yes") {
		return result, nil
	}
	if shared != "" && !strings.EqualFold(shared, "no") {
		return result, domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("%s has unsupported spec.shared value %q", result.Reference, shared))
	}
	patch, err := json.Marshal(map[string]any{"spec": map[string]string{"shared": "yes"}})
	if err != nil {
		return result, domain.WrapError(domain.ErrorInternal, "OpenEBS LVM shared mount", "encode LVMVolume patch", err)
	}
	if _, err := m.dynamic.Resource(openEBSLVMVolumeGVR).Namespace(volume.GetNamespace()).Patch(ctx, volume.GetName(), types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return result, domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", fmt.Sprintf("patch %s", result.Reference), err)
	}
	result.Changed = true
	return result, nil
}

func (m *openEBSLVMSharedVolumeManager) volume(ctx context.Context, sourcePVName string) (*metav1.PartialObjectMetadata, string, error) {
	if m == nil || m.typed == nil || m.dynamic == nil {
		return nil, "", domain.NewError(domain.ErrorInternal, "OpenEBS LVM shared mount", "Kubernetes typed and dynamic clients are required")
	}
	if strings.TrimSpace(sourcePVName) == "" {
		return nil, "", domain.NewError(domain.ErrorValidation, "OpenEBS LVM shared mount", "source PV name is required")
	}
	pv, err := m.typed.CoreV1().PersistentVolumes().Get(ctx, sourcePVName, metav1.GetOptions{})
	if err != nil {
		return nil, "", domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", fmt.Sprintf("read source PV %s", sourcePVName), err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "local.csi.openebs.io" || strings.TrimSpace(pv.Spec.CSI.VolumeHandle) == "" {
		return nil, "", domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("source PV %s is not an OpenEBS LVM CSI volume", sourcePVName))
	}
	wantedName := strings.ToLower(strings.TrimSpace(pv.Spec.CSI.VolumeHandle))
	items, err := m.dynamic.Resource(openEBSLVMVolumeGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, "", domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", "list OpenEBS LVMVolumes", err)
	}
	var match *metav1.PartialObjectMetadata
	var shared string
	for index := range items.Items {
		item := &items.Items[index]
		if item.GetName() != wantedName {
			continue
		}
		if match != nil {
			return nil, "", domain.NewError(domain.ErrorConflict, "OpenEBS LVM shared mount", fmt.Sprintf("multiple LVMVolumes named %s were found", wantedName))
		}
		value, found, nestedErr := unstructured.NestedString(item.Object, "spec", "shared")
		if nestedErr != nil {
			return nil, "", domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", fmt.Sprintf("read LVMVolume %s/%s spec.shared", item.GetNamespace(), item.GetName()), nestedErr)
		}
		if !found {
			value = ""
		}
		match = &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: item.GetName(), Namespace: item.GetNamespace()}}
		shared = strings.TrimSpace(value)
	}
	if match == nil {
		return nil, "", domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("LVMVolume %s for source PV %s was not found", wantedName, sourcePVName))
	}
	return match, shared, nil
}

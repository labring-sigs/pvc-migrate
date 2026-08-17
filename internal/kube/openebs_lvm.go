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
	RestoreShared(context.Context, string, string, bool) error
}

type OpenEBSLVMSharedResult struct {
	Reference         string
	PreviousShared    string
	PreviousSharedSet bool
	Changed           bool
}

type openEBSLVMSharedVolumeManager struct {
	typed   kubernetes.Interface
	dynamic dynamic.Interface
}

type openEBSLVMVolume struct {
	namespace string
	name      string
	shared    string
	sharedSet bool
}

func (v openEBSLVMVolume) reference() string {
	return fmt.Sprintf("LVMVolume %s/%s", v.namespace, v.name)
}

func NewOpenEBSLVMSharedVolumeManager(typed kubernetes.Interface, dynamicClient dynamic.Interface) OpenEBSLVMSharedVolumeManager {
	return &openEBSLVMSharedVolumeManager{typed: typed, dynamic: dynamicClient}
}

func (m *openEBSLVMSharedVolumeManager) Shared(ctx context.Context, sourcePVName string) (bool, error) {
	volume, err := m.volume(ctx, sourcePVName)
	if err != nil {
		return false, err
	}
	if volume.shared == "" || strings.EqualFold(volume.shared, "no") {
		return false, nil
	}
	if strings.EqualFold(volume.shared, "yes") {
		return true, nil
	}
	return false, domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("%s has unsupported spec.shared value %q", volume.reference(), volume.shared))
}

func (m *openEBSLVMSharedVolumeManager) EnsureShared(ctx context.Context, sourcePVName string) (OpenEBSLVMSharedResult, error) {
	volume, err := m.volume(ctx, sourcePVName)
	if err != nil {
		return OpenEBSLVMSharedResult{}, err
	}
	result := OpenEBSLVMSharedResult{
		Reference:         volume.reference(),
		PreviousShared:    volume.shared,
		PreviousSharedSet: volume.sharedSet,
	}
	if strings.EqualFold(volume.shared, "yes") {
		return result, nil
	}
	if volume.shared != "" && !strings.EqualFold(volume.shared, "no") {
		return result, domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("%s has unsupported spec.shared value %q", result.Reference, volume.shared))
	}
	if err := m.patchShared(ctx, volume, "yes"); err != nil {
		return result, err
	}
	result.Changed = true
	return result, nil
}

func (m *openEBSLVMSharedVolumeManager) RestoreShared(ctx context.Context, sourcePVName, previousShared string, previousSharedSet bool) error {
	volume, err := m.volume(ctx, sourcePVName)
	if err != nil {
		return err
	}
	if previousSharedSet && volume.sharedSet && volume.shared == previousShared {
		return nil
	}
	if !previousSharedSet && !volume.sharedSet {
		return nil
	}
	if !strings.EqualFold(volume.shared, "yes") {
		return domain.NewError(domain.ErrorConflict, "restore OpenEBS LVM shared mount", fmt.Sprintf("%s changed from session-managed shared=yes to %q; resolve the LVMVolume setting before retrying", volume.reference(), volume.shared))
	}
	var replacement any
	if previousSharedSet {
		replacement = previousShared
	}
	return m.patchShared(ctx, volume, replacement)
}

func (m *openEBSLVMSharedVolumeManager) patchShared(ctx context.Context, volume openEBSLVMVolume, value any) error {
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"shared": value}})
	if err != nil {
		return domain.WrapError(domain.ErrorInternal, "OpenEBS LVM shared mount", "encode LVMVolume patch", err)
	}
	if _, err := m.dynamic.Resource(openEBSLVMVolumeGVR).Namespace(volume.namespace).Patch(ctx, volume.name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", fmt.Sprintf("patch %s", volume.reference()), err)
	}
	return nil
}

func (m *openEBSLVMSharedVolumeManager) volume(ctx context.Context, sourcePVName string) (openEBSLVMVolume, error) {
	if m == nil || m.typed == nil || m.dynamic == nil {
		return openEBSLVMVolume{}, domain.NewError(domain.ErrorInternal, "OpenEBS LVM shared mount", "Kubernetes typed and dynamic clients are required")
	}
	if strings.TrimSpace(sourcePVName) == "" {
		return openEBSLVMVolume{}, domain.NewError(domain.ErrorValidation, "OpenEBS LVM shared mount", "source PV name is required")
	}
	pv, err := m.typed.CoreV1().PersistentVolumes().Get(ctx, sourcePVName, metav1.GetOptions{})
	if err != nil {
		return openEBSLVMVolume{}, domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", fmt.Sprintf("read source PV %s", sourcePVName), err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != "local.csi.openebs.io" || strings.TrimSpace(pv.Spec.CSI.VolumeHandle) == "" {
		return openEBSLVMVolume{}, domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("source PV %s is not an OpenEBS LVM CSI volume", sourcePVName))
	}
	wantedName := strings.ToLower(strings.TrimSpace(pv.Spec.CSI.VolumeHandle))
	items, err := m.dynamic.Resource(openEBSLVMVolumeGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return openEBSLVMVolume{}, domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", "list OpenEBS LVMVolumes", err)
	}
	var match *openEBSLVMVolume
	for index := range items.Items {
		item := &items.Items[index]
		if item.GetName() != wantedName {
			continue
		}
		if match != nil {
			return openEBSLVMVolume{}, domain.NewError(domain.ErrorConflict, "OpenEBS LVM shared mount", fmt.Sprintf("multiple LVMVolumes named %s were found", wantedName))
		}
		value, found, nestedErr := unstructured.NestedString(item.Object, "spec", "shared")
		if nestedErr != nil {
			return openEBSLVMVolume{}, domain.WrapError(domain.ErrorKubernetes, "OpenEBS LVM shared mount", fmt.Sprintf("read LVMVolume %s/%s spec.shared", item.GetNamespace(), item.GetName()), nestedErr)
		}
		match = &openEBSLVMVolume{namespace: item.GetNamespace(), name: item.GetName(), shared: strings.TrimSpace(value), sharedSet: found}
	}
	if match == nil {
		return openEBSLVMVolume{}, domain.NewError(domain.ErrorPrecondition, "OpenEBS LVM shared mount", fmt.Sprintf("LVMVolume %s for source PV %s was not found", wantedName, sourcePVName))
	}
	return *match, nil
}

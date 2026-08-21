package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

const (
	OpenEBSLVMCSIDriver               = "local.csi.openebs.io"
	openEBSLVMSharedSessionAnnotation = "pvc-migrate.io/openebs-shared-session"
)

// OpenEBSLVMSharedVolumeManager reads and explicitly enables the same-node
// concurrent mount setting maintained by the OpenEBS LVM CSI driver.
type OpenEBSLVMSharedVolumeManager interface {
	Shared(ctx context.Context, pvc, pv domain.ObjectReference, sessionID string) (bool, error)
	PrepareShared(ctx context.Context, pv domain.ObjectReference) (OpenEBSLVMSharedResult, error)
	EnableShared(ctx context.Context, sessionID string, mount domain.OpenEBSLVMSharedMount) error
	ValidateRestoreShared(
		ctx context.Context,
		sessionID string,
		mount domain.OpenEBSLVMSharedMount,
	) error
	RestoreShared(ctx context.Context, sessionID string, mount domain.OpenEBSLVMSharedMount) error
}

type OpenEBSLVMSharedResult struct {
	Reference         string
	LVMVolume         domain.ObjectReference
	PreviousShared    string
	PreviousSharedSet bool
	NeedsChange       bool
}

type openEBSLVMSharedVolumeManager struct {
	typed   kubernetes.Interface
	dynamic dynamic.Interface
}

type openEBSLVMVolume struct {
	namespace        string
	name             string
	uid              types.UID
	resourceVersion  string
	shared           string
	sharedSet        bool
	sharedSession    string
	sharedSessionSet bool
}

func (v openEBSLVMVolume) reference() string {
	return fmt.Sprintf("LVMVolume %s/%s", v.namespace, v.name)
}

func NewOpenEBSLVMSharedVolumeManager(
	typed kubernetes.Interface,
	dynamicClient dynamic.Interface,
) OpenEBSLVMSharedVolumeManager {
	return &openEBSLVMSharedVolumeManager{typed: typed, dynamic: dynamicClient}
}

func (m *openEBSLVMSharedVolumeManager) Shared(
	ctx context.Context,
	sourcePV, expectedLVMVolume domain.ObjectReference,
	sessionID string,
) (bool, error) {
	volume, err := m.volume(ctx, sourcePV, expectedLVMVolume)
	if err != nil {
		return false, err
	}

	if sessionID == "" && volume.sharedSessionSet {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"%s temporary shared-mount ownership belongs to session %q",
				volume.reference(),
				volume.sharedSession,
			),
		)
	}

	if sessionID != "" && (!volume.sharedSessionSet || volume.sharedSession != sessionID) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"%s no longer carries temporary shared-mount ownership for session %q",
				volume.reference(),
				sessionID,
			),
		)
	}

	if sessionID != "" && !strings.EqualFold(volume.shared, "yes") {
		return false, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"%s changed from session-managed shared=yes to %q",
				volume.reference(),
				volume.shared,
			),
		)
	}

	if volume.shared == "" || strings.EqualFold(volume.shared, "no") {
		return false, nil
	}

	if strings.EqualFold(volume.shared, "yes") {
		return true, nil
	}

	return false, domain.NewError(
		domain.ErrorPrecondition,
		"OpenEBS LVM shared mount",
		fmt.Sprintf("%s has unsupported spec.shared value %q", volume.reference(), volume.shared),
	)
}

func (m *openEBSLVMSharedVolumeManager) PrepareShared(
	ctx context.Context,
	sourcePV domain.ObjectReference,
) (OpenEBSLVMSharedResult, error) {
	volume, err := m.volume(ctx, sourcePV, domain.ObjectReference{})
	if err != nil {
		return OpenEBSLVMSharedResult{}, err
	}

	result := OpenEBSLVMSharedResult{
		Reference:         volume.reference(),
		LVMVolume:         lvmVolumeReference(volume),
		PreviousShared:    volume.shared,
		PreviousSharedSet: volume.sharedSet,
	}
	if volume.uid == "" {
		return result, domain.NewError(
			domain.ErrorPrecondition,
			"OpenEBS LVM shared mount",
			result.Reference+" has no UID",
		)
	}

	if volume.sharedSessionSet {
		return result, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"%s already has temporary shared-mount ownership %q",
				result.Reference,
				volume.sharedSession,
			),
		)
	}

	if strings.EqualFold(volume.shared, "yes") {
		return result, nil
	}

	if volume.shared != "" && !strings.EqualFold(volume.shared, "no") {
		return result, domain.NewError(
			domain.ErrorPrecondition,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("%s has unsupported spec.shared value %q", result.Reference, volume.shared),
		)
	}

	result.NeedsChange = true

	return result, nil
}

func (m *openEBSLVMSharedVolumeManager) EnableShared(
	ctx context.Context,
	sessionID string,
	state domain.OpenEBSLVMSharedMount,
) error {
	if strings.TrimSpace(sessionID) == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"OpenEBS LVM shared mount",
			"session ID is required",
		)
	}

	volume, err := m.volume(ctx, state.SourcePV, state.LVMVolume)
	if err != nil {
		return err
	}

	if !matchesPreviousShared(volume, state.PreviousShared, state.PreviousSharedSet) ||
		volume.sharedSessionSet {
		return domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			volume.reference()+" changed before the temporary shared mount could be enabled",
		)
	}

	return m.patchShared(ctx, volume, "yes", sessionID)
}

func (m *openEBSLVMSharedVolumeManager) RestoreShared(
	ctx context.Context,
	sessionID string,
	state domain.OpenEBSLVMSharedMount,
) error {
	volume, err := m.volume(ctx, state.SourcePV, state.LVMVolume)
	if err != nil {
		return err
	}

	needsRestore, err := validateOpenEBSLVMSharedRestore(
		volume,
		sessionID,
		state.PreviousShared,
		state.PreviousSharedSet,
	)
	if err != nil {
		return err
	}

	if !needsRestore {
		return nil
	}

	var replacement any
	if state.PreviousSharedSet {
		replacement = state.PreviousShared
	}

	return m.patchShared(ctx, volume, replacement, nil)
}

func (m *openEBSLVMSharedVolumeManager) ValidateRestoreShared(
	ctx context.Context,
	sessionID string,
	state domain.OpenEBSLVMSharedMount,
) error {
	volume, err := m.volume(ctx, state.SourcePV, state.LVMVolume)
	if err != nil {
		return err
	}

	_, err = validateOpenEBSLVMSharedRestore(
		volume,
		sessionID,
		state.PreviousShared,
		state.PreviousSharedSet,
	)

	return err
}

func validateOpenEBSLVMSharedRestore(
	volume openEBSLVMVolume,
	sessionID, previousShared string,
	previousSharedSet bool,
) (bool, error) {
	if !volume.sharedSessionSet {
		if matchesPreviousShared(volume, previousShared, previousSharedSet) {
			return false, nil
		}

		return false, domain.NewError(
			domain.ErrorConflict,
			"restore OpenEBS LVM shared mount",
			volume.reference()+" no longer carries this session's temporary shared-mount marker",
		)
	}

	if volume.sharedSession != sessionID {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore OpenEBS LVM shared mount",
			fmt.Sprintf(
				"%s temporary shared-mount ownership changed to %q",
				volume.reference(),
				volume.sharedSession,
			),
		)
	}

	if !strings.EqualFold(volume.shared, "yes") &&
		!matchesPreviousShared(volume, previousShared, previousSharedSet) {
		return false, domain.NewError(
			domain.ErrorConflict,
			"restore OpenEBS LVM shared mount",
			fmt.Sprintf(
				"%s changed from session-managed shared=yes to %q; resolve the LVMVolume setting before retrying",
				volume.reference(),
				volume.shared,
			),
		)
	}

	return true, nil
}

func matchesPreviousShared(
	volume openEBSLVMVolume,
	previousShared string,
	previousSharedSet bool,
) bool {
	return volume.sharedSet == previousSharedSet &&
		(!previousSharedSet || volume.shared == previousShared)
}

func (m *openEBSLVMSharedVolumeManager) patchShared(
	ctx context.Context,
	volume openEBSLVMVolume,
	value, sessionAnnotation any,
) error {
	if volume.uid == "" || volume.resourceVersion == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"OpenEBS LVM shared mount",
			volume.reference()+" has no stable UID or resourceVersion",
		)
	}

	metadata := map[string]any{
		"annotations":     map[string]any{openEBSLVMSharedSessionAnnotation: sessionAnnotation},
		"uid":             string(volume.uid),
		"resourceVersion": volume.resourceVersion,
	}

	patch, err := json.Marshal(
		map[string]any{"metadata": metadata, "spec": map[string]any{"shared": value}},
	)
	if err != nil {
		return domain.WrapError(
			domain.ErrorInternal,
			"OpenEBS LVM shared mount",
			"encode LVMVolume patch",
			err,
		)
	}

	if _, err := m.dynamic.Resource(openEBSLVMVolumeGVR).
		Namespace(volume.namespace).
		Patch(ctx, volume.name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		if apierrors.IsConflict(err) {
			return domain.WrapError(
				domain.ErrorConflict,
				"OpenEBS LVM shared mount",
				"patch "+volume.reference()+" was rejected because the resource changed concurrently; retry the operation",
				err,
			)
		}
		return domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"patch "+volume.reference(),
			err,
		)
	}

	return nil
}

func (m *openEBSLVMSharedVolumeManager) volume(
	ctx context.Context,
	sourcePV, expectedLVMVolume domain.ObjectReference,
) (openEBSLVMVolume, error) {
	if m == nil || m.typed == nil || m.dynamic == nil {
		return openEBSLVMVolume{}, domain.NewError(
			domain.ErrorInternal,
			"OpenEBS LVM shared mount",
			"Kubernetes typed and dynamic clients are required",
		)
	}

	sourcePVName := strings.TrimSpace(sourcePV.Name)
	if sourcePVName == "" || sourcePV.UID == "" {
		return openEBSLVMVolume{}, domain.NewError(
			domain.ErrorValidation,
			"OpenEBS LVM shared mount",
			"source PV name and UID are required",
		)
	}

	pv, err := m.typed.CoreV1().PersistentVolumes().Get(ctx, sourcePVName, metav1.GetOptions{})
	if err != nil {
		return openEBSLVMVolume{}, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"read source PV "+sourcePVName,
			err,
		)
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != OpenEBSLVMCSIDriver ||
		strings.TrimSpace(pv.Spec.CSI.VolumeHandle) == "" {
		return openEBSLVMVolume{}, domain.NewError(
			domain.ErrorPrecondition,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("source PV %s is not an OpenEBS LVM CSI volume", sourcePVName),
		)
	}

	if pv.UID != sourcePV.UID {
		return openEBSLVMVolume{}, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("source PV %s UID changed", sourcePVName),
		)
	}

	wantedName := strings.ToLower(strings.TrimSpace(pv.Spec.CSI.VolumeHandle))

	items, err := m.dynamic.Resource(openEBSLVMVolumeGVR).
		Namespace(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return openEBSLVMVolume{}, domain.WrapError(
			domain.ErrorKubernetes,
			"OpenEBS LVM shared mount",
			"list OpenEBS LVMVolumes",
			err,
		)
	}

	var match *openEBSLVMVolume
	for index := range items.Items {
		item := &items.Items[index]
		if item.GetName() != wantedName {
			continue
		}

		if match != nil {
			return openEBSLVMVolume{}, domain.NewError(
				domain.ErrorConflict,
				"OpenEBS LVM shared mount",
				fmt.Sprintf("multiple LVMVolumes named %s were found", wantedName),
			)
		}

		value, found, nestedErr := unstructured.NestedString(item.Object, "spec", "shared")
		if nestedErr != nil {
			return openEBSLVMVolume{}, domain.WrapError(
				domain.ErrorKubernetes,
				"OpenEBS LVM shared mount",
				fmt.Sprintf(
					"read LVMVolume %s/%s spec.shared",
					item.GetNamespace(),
					item.GetName(),
				),
				nestedErr,
			)
		}

		annotations := item.GetAnnotations()
		sharedSession, sharedSessionSet := annotations[openEBSLVMSharedSessionAnnotation]
		match = &openEBSLVMVolume{
			namespace:        item.GetNamespace(),
			name:             item.GetName(),
			uid:              item.GetUID(),
			resourceVersion:  item.GetResourceVersion(),
			shared:           strings.TrimSpace(value),
			sharedSet:        found,
			sharedSession:    sharedSession,
			sharedSessionSet: sharedSessionSet,
		}
	}

	if match == nil {
		return openEBSLVMVolume{}, domain.NewError(
			domain.ErrorPrecondition,
			"OpenEBS LVM shared mount",
			fmt.Sprintf("LVMVolume %s for source PV %s was not found", wantedName, sourcePVName),
		)
	}

	if expectedLVMVolume.Name != "" &&
		(match.name != expectedLVMVolume.Name || match.namespace != expectedLVMVolume.Namespace || expectedLVMVolume.UID == "" || match.uid != expectedLVMVolume.UID) {
		return openEBSLVMVolume{}, domain.NewError(
			domain.ErrorConflict,
			"OpenEBS LVM shared mount",
			fmt.Sprintf(
				"LVMVolume %s/%s identity changed",
				expectedLVMVolume.Namespace,
				expectedLVMVolume.Name,
			),
		)
	}

	return *match, nil
}

func lvmVolumeReference(volume openEBSLVMVolume) domain.ObjectReference {
	return domain.ObjectReference{
		APIVersion:      openEBSLVMVolumeGVR.Group + "/" + openEBSLVMVolumeGVR.Version,
		Kind:            "LVMVolume",
		Namespace:       volume.namespace,
		Name:            volume.name,
		UID:             volume.uid,
		ResourceVersion: volume.resourceVersion,
	}
}

package app

import (
	"context"
	"slices"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
	"k8s.io/apimachinery/pkg/types"
)

// recordingOpenEBSLVMSharedVolumeManager is the shared-volume manager double:
// it records which PVs were touched and can inject failures.
type recordingOpenEBSLVMSharedVolumeManager struct {
	shared             bool
	ensurePVCs         []string
	sharedPVs          []string
	enablePVs          []string
	restorePVs         []string
	restored           bool
	ensureErr          error
	restoreErr         error
	onEnable           func()
	restoreContextErrs []error
}

func (m *recordingOpenEBSLVMSharedVolumeManager) Shared(
	_ context.Context,
	sourcePV, _ v1alpha1.ObjectReference,
	_ string,
) (bool, error) {
	m.sharedPVs = append(m.sharedPVs, sourcePV.Name)
	if m.shared {
		return true, nil
	}

	// Once a mount is enabled the source stays writable through warm passes.
	if slices.Contains(m.enablePVs, sourcePV.Name) {
		return true, nil
	}

	return false, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) PrepareShared(
	_ context.Context,
	pv v1alpha1.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	return kube.OpenEBSLVMSharedResult{
		NeedsChange: true,
		LVMVolume: v1alpha1.ObjectReference{
			Namespace: "openebs",
			Name:      "lvm-" + pv.Name,
			UID:       types.UID("lvm-" + pv.Name + "-uid"),
		},
		PreviousShared:    "false",
		PreviousSharedSet: true,
	}, nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) EnsureShared(
	_ context.Context,
	pvc, _ v1alpha1.ObjectReference,
) (kube.OpenEBSLVMSharedResult, error) {
	m.ensurePVCs = append(m.ensurePVCs, pvc.Namespace+"/"+pvc.Name)
	return kube.OpenEBSLVMSharedResult{}, m.ensureErr
}

func (m *recordingOpenEBSLVMSharedVolumeManager) EnableShared(
	_ context.Context,
	_ string,
	mount v1alpha1.SharedMountStatus,
) error {
	m.enablePVs = append(m.enablePVs, mount.SourcePV.Name)
	if m.onEnable != nil {
		m.onEnable()
	}

	return nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) ValidateRestoreShared(
	context.Context,
	string,
	v1alpha1.SharedMountStatus,
) error {
	return nil
}

func (m *recordingOpenEBSLVMSharedVolumeManager) RestoreShared(
	ctx context.Context,
	_ string,
	mount v1alpha1.SharedMountStatus,
) error {
	m.restoreContextErrs = append(m.restoreContextErrs, ctx.Err())
	m.restorePVs = append(m.restorePVs, mount.SourcePV.Name)
	m.restored = true
	return m.restoreErr
}

// staticVolumeUsageReader is a fixed source-usage double.
type staticVolumeUsageReader struct {
	result kube.VolumeUsageReadResult
	err    error
	calls  int
}

func (r *staticVolumeUsageReader) Read(
	context.Context,
	kube.VolumeUsageReadOptions,
) (kube.VolumeUsageReadResult, error) {
	r.calls++
	return r.result, r.err
}

// volumeUsageReaderFunc adapts a function to the usage-reader interface.
type volumeUsageReaderFunc func(context.Context, kube.VolumeUsageReadOptions) (kube.VolumeUsageReadResult, error)

func (f volumeUsageReaderFunc) Read(
	ctx context.Context,
	options kube.VolumeUsageReadOptions,
) (kube.VolumeUsageReadResult, error) {
	return f(ctx, options)
}

// recordingToolImageProber records probe options and replays results.
type recordingToolImageProber struct {
	calls   []kube.ToolImageProbeOptions
	results []kube.ToolImageProbeResult
	err     error
	onProbe func(context.Context)
}

func (p *recordingToolImageProber) Probe(
	ctx context.Context,
	options kube.ToolImageProbeOptions,
) ([]kube.ToolImageProbeResult, error) {
	p.calls = append(p.calls, options)
	if p.onProbe != nil {
		p.onProbe(ctx)
	}

	if p.err != nil {
		return nil, p.err
	}

	if p.results != nil {
		return slices.Clone(p.results), nil
	}

	results := make([]kube.ToolImageProbeResult, len(options.Targets))
	for index, target := range options.Targets {
		nodeName := target.NodeName
		if nodeName == "" {
			nodeName = "scheduler-node"
		}

		results[index] = kube.ToolImageProbeResult{Target: target, NodeName: nodeName}
	}

	return results, nil
}

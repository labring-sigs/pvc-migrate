package kube

import (
	"context"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// VolumeUsageReadOptions identifies a source volume whose used bytes can be
// read from trusted storage-backend metadata. Implementations must not mount
// the volume or create a Pod.
type VolumeUsageReadOptions struct {
	SourcePVC domain.ObjectReference
	SourcePV  domain.ObjectReference
}

// VolumeUsageReadResult reports a conservative upper bound for the source
// data that must fit in the destination volume.
type VolumeUsageReadResult struct {
	UsedBytes int64
	Source    string
}

// VolumeUsageReader reads usage only from a known storage backend API or CRD.
// An unsupported backend must return an error instead of estimating usage from
// provisioned capacity.
type VolumeUsageReader interface {
	Read(context.Context, VolumeUsageReadOptions) (VolumeUsageReadResult, error)
}

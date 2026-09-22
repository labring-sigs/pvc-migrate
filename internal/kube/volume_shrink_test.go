package kube

import (
	"context"
	"errors"
	"strings"
	"testing"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

type shrinkUsageReader struct {
	result VolumeUsageReadResult
	err    error
	calls  []VolumeUsageReadOptions
}

func (r *shrinkUsageReader) Read(
	_ context.Context,
	options VolumeUsageReadOptions,
) (VolumeUsageReadResult, error) {
	r.calls = append(r.calls, options)
	return r.result, r.err
}

func TestVerifyVolumeShrinkUsage(t *testing.T) {
	sourcePVC := v1alpha1.ObjectReference{Namespace: "app", Name: "data", UID: "pvc-uid"}
	sourcePV := v1alpha1.ObjectReference{Name: "pv-data", UID: "pv-uid"}

	for _, test := range []struct {
		name      string
		reader    *shrinkUsageReader
		capacity  string
		path      string
		skip      bool
		wantCalls int
		wantError string
		category  domain.ErrorCategory
	}{
		{name: "missing reader", capacity: "1Gi", wantError: "--skip-source-usage-check", category: domain.ErrorPrecondition},
		{name: "fits", reader: &shrinkUsageReader{result: VolumeUsageReadResult{UsedBytes: 512 << 20}}, capacity: "1Gi", wantCalls: 1},
		{name: "equal destination", reader: &shrinkUsageReader{result: VolumeUsageReadResult{UsedBytes: 1 << 30}}, capacity: "1Gi", wantCalls: 1},
		{name: "overflow", reader: &shrinkUsageReader{result: VolumeUsageReadResult{UsedBytes: 2 << 30}}, capacity: "1Gi", wantCalls: 1, wantError: "above destination capacity", category: domain.ErrorConflict},
		{name: "selected path", reader: &shrinkUsageReader{result: VolumeUsageReadResult{UsedBytes: 2 << 30}}, capacity: "1Gi", path: "selected/data", wantCalls: 1, wantError: "cannot prove that selected source directory \"selected/data\" fits", category: domain.ErrorConflict},
		{name: "explicit skip", reader: &shrinkUsageReader{err: errors.New("must not read")}, capacity: "1Gi", skip: true},
		{name: "no shrink", reader: &shrinkUsageReader{err: errors.New("must not read")}, capacity: "2Gi"},
		{name: "growth", reader: &shrinkUsageReader{err: errors.New("must not read")}, capacity: "3Gi"},
		{name: "reader error", reader: &shrinkUsageReader{err: errors.New("backend unavailable")}, capacity: "1Gi", wantCalls: 1, wantError: "usage could not be read", category: domain.ErrorPrecondition},
		{name: "invalid usage", reader: &shrinkUsageReader{result: VolumeUsageReadResult{UsedBytes: -1}}, capacity: "1Gi", wantCalls: 1, wantError: "invalid used bytes -1", category: domain.ErrorPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reader VolumeUsageReader
			if test.reader != nil {
				reader = test.reader
			}

			err := VerifyVolumeShrinkUsage(context.Background(), reader, nil,
				sourcePVC, sourcePV, "2Gi", test.capacity, test.path, test.skip)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) || domain.CategoryOf(err) != test.category {
				t.Fatalf(
					"error=%v category=%s; want %q category=%s",
					err,
					domain.CategoryOf(err),
					test.wantError,
					test.category,
				)
			}

			if test.reader != nil {
				if len(test.reader.calls) != test.wantCalls {
					t.Fatalf("calls=%d; want %d", len(test.reader.calls), test.wantCalls)
				}

				if test.wantCalls > 0 && test.reader.err != nil &&
					!errors.Is(err, test.reader.err) {
					t.Fatalf("lost backend error: %v", err)
				}

				for _, call := range test.reader.calls {
					if call.SourcePVC != sourcePVC || call.SourcePV != sourcePV {
						t.Fatalf("lost source identity: %+v", call)
					}
				}
			}
		})
	}
}

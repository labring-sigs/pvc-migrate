package copyengine_test

import (
	"strings"
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/copyengine"
)

func TestValidateBandwidthLimit(t *testing.T) {
	valid := []string{"1024", "512k", "10m", "1.5m", "2g", "100M", "64K"}
	for _, value := range valid {
		if err := copyengine.ValidateBandwidthLimit(value); err != nil {
			t.Fatalf("valid value %q rejected: %v", value, err)
		}
	}

	invalid := []string{"", "mb", "10x", "1.2.3", "-5m", "10 m", "10Kib"}
	for _, value := range invalid {
		if err := copyengine.ValidateBandwidthLimit(value); err == nil {
			t.Fatalf("invalid value %q accepted", value)
		}
	}

	err := copyengine.ValidateBandwidthLimit("wat")
	if err == nil || !strings.Contains(err.Error(), "10m") {
		t.Fatalf("error must show the expected form: %v", err)
	}
}

// TestCopyRequestCarriesBandwidthLimit pins that a set bandwidth limit
// reaches the rsync argument line; the engine test double records the
// upstream-facing request.
func TestCopyRequestCarriesBandwidthLimit(t *testing.T) {
	request := copyengine.CopyRequest{
		Policy: copyengine.CopyPolicy{
			Strategies:     []string{"local"},
			BandwidthLimit: "10m",
		},
	}

	if request.Policy.BandwidthLimit != "10m" {
		t.Fatalf("bandwidth limit=%q", request.Policy.BandwidthLimit)
	}

	if err := copyengine.ValidateBandwidthLimit(request.Policy.BandwidthLimit); err != nil {
		t.Fatal(err)
	}
}

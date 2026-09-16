package controller

import (
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
)

func TestApplyRetryPolicy(t *testing.T) {
	retries := int32(5)
	backoff := "30s"
	timeout := "2h"

	t.Run("overrides only set fields", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3, RetryBackoff: 2 * time.Second}

		applyRetryPolicy(&config, &v1alpha1.RetryPolicySpec{
			Retries: &retries,
		}, func(string) {})

		if config.Retries != 5 || config.RetryBackoff != 2*time.Second {
			t.Fatalf("retries=%d backoff=%s", config.Retries, config.RetryBackoff)
		}
	})

	t.Run("parses durations", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3}

		applyRetryPolicy(&config, &v1alpha1.RetryPolicySpec{
			RetryBackoff: &backoff,
			CopyTimeout:  &timeout,
		}, func(string) {})

		if config.RetryBackoff != 30*time.Second {
			t.Fatalf("backoff=%s", config.RetryBackoff)
		}

		if config.CopyTimeout != 2*time.Hour {
			t.Fatalf("copyTimeout=%s", config.CopyTimeout)
		}
	})

	t.Run("invalid duration keeps default and reports", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3, RetryBackoff: 2 * time.Second}
		reports := []string{}

		applyRetryPolicy(&config, &v1alpha1.RetryPolicySpec{
			RetryBackoff: new("tomorrow"),
		}, func(message string) {
			reports = append(reports, message)
		})

		if config.RetryBackoff != 2*time.Second {
			t.Fatalf("invalid backoff changed the default: %s", config.RetryBackoff)
		}

		if len(reports) != 1 {
			t.Fatalf("reports=%v", reports)
		}
	})

	t.Run("nil policy keeps defaults", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3}

		applyRetryPolicy(&config, nil, func(string) {})

		if config.Retries != 3 {
			t.Fatalf("retries=%d", config.Retries)
		}
	})
}

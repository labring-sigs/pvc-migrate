package controller

import (
	"testing"
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
)

func TestApplyTransferPolicy(t *testing.T) {
	retries := int32(5)
	backoff := "30s"
	timeout := "2h"
	rsyncRetries := int32(7)

	t.Run("retry policy overrides only set fields", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3, RetryBackoff: 2 * time.Second}

		applyTransferPolicy(&config, &v1alpha1.TransferOptions{
			RetryPolicy: &v1alpha1.RetryPolicySpec{Retries: &retries},
		}, func(string) {})

		if config.Retries != 5 || config.RetryBackoff != 2*time.Second {
			t.Fatalf("retries=%d backoff=%s", config.Retries, config.RetryBackoff)
		}
	})

	t.Run("parses durations", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3}

		applyTransferPolicy(&config, &v1alpha1.TransferOptions{
			CopyTimeout:     &timeout,
			RsyncMaxRetries: &rsyncRetries,
			RetryPolicy: &v1alpha1.RetryPolicySpec{
				RetryBackoff: &backoff,
			},
		}, func(string) {})

		if config.RetryBackoff != 30*time.Second {
			t.Fatalf("backoff=%s", config.RetryBackoff)
		}

		if config.CopyTimeout != 2*time.Hour {
			t.Fatalf("copyTimeout=%s", config.CopyTimeout)
		}

		if config.RsyncMaxRetries != 7 {
			t.Fatalf("rsyncMaxRetries=%d", config.RsyncMaxRetries)
		}
	})

	t.Run("invalid duration keeps default and reports", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3, RetryBackoff: 2 * time.Second}
		reports := []string{}
		bad := "tomorrow"

		applyTransferPolicy(&config, &v1alpha1.TransferOptions{
			RetryPolicy: &v1alpha1.RetryPolicySpec{RetryBackoff: &bad},
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

	t.Run("nil options keep defaults", func(t *testing.T) {
		config := app.VolumeCopyConfig{Retries: 3}

		applyTransferPolicy(&config, nil, func(string) {})

		if config.Retries != 3 {
			t.Fatalf("retries=%d", config.Retries)
		}
	})
}

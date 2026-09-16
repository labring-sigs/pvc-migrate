package controller

import (
	"time"

	v1alpha1 "github.com/labring-sigs/pvc-migrate/api/v1alpha1"
	"github.com/labring-sigs/pvc-migrate/internal/app"
)

// applyRetryPolicy overlays a workflow's retry policy onto the controller's
// transfer defaults. Only fields the workflow explicitly sets are applied, so
// controller deployment arguments remain the fallback. Invalid durations are
// reported through the callback and keep the defaults: the CRD schema
// constrains the shape at admission, and planning rejects what admission
// cannot, so reaching execution with a parse failure is a stale-spec edge.
func applyRetryPolicy(
	config *app.VolumeCopyConfig,
	policy *v1alpha1.RetryPolicySpec,
	report func(string),
) {
	if policy == nil {
		return
	}

	if policy.Retries != nil && *policy.Retries >= 0 {
		config.Retries = int(*policy.Retries)
	}

	if policy.RetryBackoff != nil {
		if backoff, err := time.ParseDuration(*policy.RetryBackoff); err == nil && backoff > 0 {
			config.RetryBackoff = backoff
		} else if err != nil {
			report("retryBackoff " + *policy.RetryBackoff + ": " + err.Error())
		}
	}

	if policy.CopyTimeout != nil {
		if timeout, err := time.ParseDuration(*policy.CopyTimeout); err == nil {
			config.CopyTimeout = timeout
		} else if err != nil {
			report("copyTimeout " + *policy.CopyTimeout + ": " + err.Error())
		}
	}
}

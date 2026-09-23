package planner

import (
	"strings"
	"testing"
)

func networkPolicyReviewFor(namespace string, verbs ...string) networkPolicyReview {
	allowed := make(map[string]bool, len(networkPolicyLifecycleVerbs))
	for _, verb := range networkPolicyLifecycleVerbs {
		allowed[verb] = false
	}

	for _, verb := range verbs {
		allowed[verb] = true
	}

	return networkPolicyReview{namespace: namespace, allowed: allowed}
}

func TestClassifyNetworkPolicyAccess(t *testing.T) {
	for _, test := range []struct {
		name       string
		reviews    []networkPolicyReview
		wantPass   bool
		wantFailed bool
		contains   string
	}{
		{
			name: "full lifecycle enables the transfer policies",
			reviews: []networkPolicyReview{
				networkPolicyReviewFor("app", networkPolicyLifecycleVerbs...),
				networkPolicyReviewFor("stage", networkPolicyLifecycleVerbs...),
			},
			wantPass: true,
			contains: "allowed in app, stage",
		},
		{
			name: "create without get fails fast",
			reviews: []networkPolicyReview{
				networkPolicyReviewFor("app", "list", "create", "update", "delete"),
				networkPolicyReviewFor("stage", networkPolicyLifecycleVerbs...),
			},
			wantFailed: true,
			contains:   "partial network policy permissions in app",
		},
		{
			name: "create with only list fails fast",
			reviews: []networkPolicyReview{
				networkPolicyReviewFor("app", "list", "create"),
			},
			wantFailed: true,
			contains:   "grant get, list, create, update, and delete together",
		},
		{
			name: "no create skips the policies with a warning",
			reviews: []networkPolicyReview{
				networkPolicyReviewFor("app", "list", "get", "update", "delete"),
			},
			wantPass: true,
			contains: "must not isolate Pods with a default-deny policy",
		},
		{
			name: "no list cannot verify isolation",
			reviews: []networkPolicyReview{
				networkPolicyReviewFor("app", "get", "create", "update", "delete"),
			},
			wantFailed: true,
			contains:   "required to verify namespace isolation",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			check := classifyNetworkPolicyAccess(test.reviews)

			if check.Passed != test.wantPass || (check.Severity == "error") != test.wantFailed {
				t.Fatalf(
					"check = %+v, want passed=%v failed=%v",
					check,
					test.wantPass,
					test.wantFailed,
				)
			}

			if !strings.Contains(check.Message, test.contains) {
				t.Fatalf("message %q omits %q", check.Message, test.contains)
			}
		})
	}
}

package controller

import (
	"os"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// The placement evaluator reads CSINode objects by name to decide whether a
// target node can provision the destination StorageClass, and lists
// CSIStorageCapacity objects for the capacity report. The upgraded-controller
// incident on a legacy cluster showed an RBAC missing the csinodes grant
// fails every plan with an opaque forbidden error, so the shipped manifests
// are contract tested.
func TestShippedRBACGrantsStorageTopologyRead(t *testing.T) {
	want := map[string][]string{
		"csinodes":             {"get"},
		"csistoragecapacities": {"list"},
	}

	for _, manifest := range []string{"../../config/rbac/role.yaml"} {
		t.Run(manifest, func(t *testing.T) {
			rules := decodeClusterRoleRules(t, manifest)

			for resource, verbs := range want {
				rule := findRuleWithResource(rules, resource)
				if rule == nil {
					t.Errorf("%s grants no %s rule", manifest, resource)
					continue
				}

				for _, verb := range verbs {
					if !slices.Contains(rule.Verbs, verb) {
						t.Errorf("%s: %s rule misses verb %s", manifest, resource, verb)
					}
				}
			}
		})
	}
}

func findRuleWithResource(rules []rbacv1.PolicyRule, resource string) *rbacv1.PolicyRule {
	for i := range rules {
		if slices.Contains(rules[i].Resources, resource) {
			return &rules[i]
		}
	}

	return nil
}

func decodeClusterRoleRules(t *testing.T, path string) []rbacv1.PolicyRule {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	var document struct {
		Items []rbacv1.ClusterRole `yaml:"items"`
	}
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}

	var rules []rbacv1.PolicyRule
	if len(document.Items) == 0 {
		var role rbacv1.ClusterRole
		if err := yaml.Unmarshal(raw, &role); err != nil {
			t.Fatal(err)
		}

		rules = role.Rules
	} else {
		for _, item := range document.Items {
			rules = append(rules, item.Rules...)
		}
	}

	if len(rules) == 0 {
		t.Fatalf("%s decoded to no rules", path)
	}

	return rules
}

package kube

import (
	"fmt"
	"strings"

	"github.com/distribution/reference"
	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// DefaultToolImageRepository is the build-time default repository for the
// unified runtime image. Container builds replace it with their own repository.
const DefaultToolImageRepository = "ghcr.io/labring-sigs/pvc-migrate"

// DefaultToolImage returns the tool image associated with a CLI build.
// Development builds use the main tag so a locally built CLI has a usable
// default while released builds follow their release tag.
func DefaultToolImage(repository, version string) string {
	repository = strings.TrimSuffix(strings.TrimSpace(repository), "/")
	if repository == "" {
		repository = DefaultToolImageRepository
	}
	tag := strings.TrimPrefix(strings.TrimSpace(version), "v")
	if tag == "" || tag == "dev" {
		tag = "main"
	}
	return repository + ":" + tag
}

// NormalizeToolImage validates the single image reference accepted by all
// helper roles and returns a canonical repository:tag reference. Helm's chart
// renders repository and tag as separate values, so digest-only references are
// deliberately rejected until the embedded chart exposes digest fields.
func NormalizeToolImage(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = DefaultToolImageRepository + ":main"
	}
	if strings.Contains(value, "@") {
		return "", domain.NewError(domain.ErrorValidation, "tool image", "digest references are unsupported; use repository:tag")
	}
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", domain.WrapError(domain.ErrorValidation, "tool image", fmt.Sprintf("invalid image reference %q", value), err)
	}
	tagged, ok := named.(reference.NamedTagged)
	if !ok || tagged.Tag() == "" {
		return "", domain.NewError(domain.ErrorValidation, "tool image", fmt.Sprintf("image %q must include an explicit tag", value))
	}
	return tagged.Name() + ":" + tagged.Tag(), nil
}

// ToolImageHelmValues makes all embedded pv-migrate chart components use
// the same repository and tag. The chart only renders components that are
// enabled by the selected strategy, so unused roles incur no Pod pull.
func ToolImageHelmValues(image string) ([]string, error) {
	normalized, err := NormalizeToolImage(image)
	if err != nil {
		return nil, err
	}
	named, err := reference.ParseNormalizedNamed(normalized)
	if err != nil {
		return nil, domain.WrapError(domain.ErrorInternal, "tool image", "parse normalized image", err)
	}
	tagged := named.(reference.NamedTagged)
	values := make([]string, 0, 6)
	for _, component := range []string{"rsync", "sshd", "rclone"} {
		values = append(values, component+".image.repository="+tagged.Name(), component+".image.tag="+tagged.Tag())
	}
	return values, nil
}

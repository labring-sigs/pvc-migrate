package backup

import (
	"fmt"
	"os"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

const rcloneConfigHelmKey = "rclone.config"

// upstreamRawConfigSentinel keeps pv-migrate in raw-config mode without
// creating a local file. That mode is also how the upstream backup path avoids
// uploading its own metadata sidecar; this project publishes and verifies its
// own completion manifest instead.
func upstreamRawConfigSentinel() string {
	return os.DevNull
}

// escapeHelmString escapes the strvals delimiters so values containing commas
// or backslashes survive the Helm value merge.
func escapeHelmString(value string) string {
	var escaped strings.Builder
	escaped.Grow(len(value))

	for _, char := range value {
		if char == '\\' || char == ',' {
			escaped.WriteByte('\\')
		}

		escaped.WriteRune(char)
	}

	return escaped.String()
}

// rcloneConfigHelmValue carries the complete generated config through the
// upstream Helm value merge. The upstream API only accepts raw config files,
// while its chart already accepts the config content as a value. Escaping the
// strvals delimiters keeps credentials containing commas or backslashes intact.
func rcloneConfigHelmValue(config string) (string, error) {
	if strings.TrimSpace(config) == "" {
		return "", domain.NewError(
			domain.ErrorValidation,
			"S3 configuration",
			"rclone config must not be empty",
		)
	}

	return rcloneConfigHelmKey + "=" + escapeHelmString(config), nil
}

// requireSecretReleaseStorage rejects credential-bearing transfer requests when
// the process environment selects Helm release storage other than the default
// Secret driver. The transfer library passes HELM_DRIVER to Helm's own
// case-sensitive storage driver switch, so any other value would persist the
// generated rclone config — including repository credentials in cleartext — in
// a ConfigMap readable by principals without Secret access, or leave releases
// in ephemeral storage this tool cannot converge during cleanup.
func requireSecretReleaseStorage() error {
	class := kube.HelmReleaseStorageFromEnv()
	if class == kube.HelmReleaseStorageSecret {
		return nil
	}

	driver, _ := os.LookupEnv("HELM_DRIVER")

	return domain.NewError(
		domain.ErrorValidation,
		"S3 configuration",
		fmt.Sprintf(
			"HELM_DRIVER=%q resolves to %q release storage, which would persist "+
				"repository credentials outside Secret objects; unset HELM_DRIVER or "+
				"set it to \"secret\" for S3 backup and restore",
			driver, class,
		),
	)
}

package backup

import (
	"os"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

const rcloneConfigHelmKey = "rclone.config"

// upstreamRawConfigSentinel keeps pv-migrate in raw-config mode without
// creating a local file. That mode is also how the upstream backup path avoids
// uploading its own metadata sidecar; this project publishes and verifies its
// own completion manifest instead.
func upstreamRawConfigSentinel() string {
	return os.DevNull
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

	var escaped strings.Builder
	escaped.Grow(len(config))

	for _, char := range config {
		if char == '\\' || char == ',' {
			escaped.WriteByte('\\')
		}

		escaped.WriteRune(char)
	}

	return rcloneConfigHelmKey + "=" + escaped.String(), nil
}

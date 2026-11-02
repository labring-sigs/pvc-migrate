package backup

import (
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

// redactionPlaceholder replaces repository credential values embedded in tool
// error text.
const redactionPlaceholder = "[redacted]"

// redactCredentialText removes repository credential values from tool error
// text before the text is embedded into a workflow status message. Transfer
// tool Pods receive the complete rclone config, so their failure output can
// echo configuration lines, while workflow status is readable by principals
// without Secret access. Both the raw values and their strvals-escaped
// spellings are replaced because the config reaches the tool as an escaped
// Helm string value.
func redactCredentialText(message string, credentials objectstore.Credentials) string {
	for _, value := range []string{credentials.AccessKey, credentials.SecretKey, credentials.SessionToken} {
		if value == "" {
			continue
		}

		message = strings.ReplaceAll(message, value, redactionPlaceholder)
		if escaped := escapeHelmString(value); escaped != value {
			message = strings.ReplaceAll(message, escaped, redactionPlaceholder)
		}
	}

	return message
}

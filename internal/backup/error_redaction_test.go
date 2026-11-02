package backup

import (
	"testing"

	"github.com/labring-sigs/pvc-migrate/internal/objectstore"
)

func TestRedactCredentialText(t *testing.T) {
	credentials := objectstore.Credentials{
		AccessKey:    "AKIA-mft-access",
		SecretKey:    `top,secret\key`,
		SessionToken: "session-token-value",
	}

	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "raw config line",
			message: "rclone failed: config access_key_id = AKIA-mft-access rejected",
			want:    "rclone failed: config access_key_id = " + redactionPlaceholder + " rejected",
		},
		{
			name:    "strvals escaped spelling",
			message: `secret_access_key = top\,secret\\key rejected`,
			want:    "secret_access_key = " + redactionPlaceholder + " rejected",
		},
		{
			name:    "session token",
			message: "token session-token-value expired",
			want:    "token " + redactionPlaceholder + " expired",
		},
		{
			name:    "no credentials present",
			message: "cannot mount unformatted disk",
			want:    "cannot mount unformatted disk",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactCredentialText(tc.message, credentials); got != tc.want {
				t.Errorf("redactCredentialText(%q) = %q, want %q", tc.message, got, tc.want)
			}
		})
	}
}

func TestRedactCredentialTextSkipsEmptyValues(t *testing.T) {
	message := "abc abc abc"
	if got := redactCredentialText(message, objectstore.Credentials{}); got != message {
		t.Errorf("empty credentials must not alter the message, got %q", got)
	}
}

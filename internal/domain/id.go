package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func NewSessionID(now time.Time) (string, error) {
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		return "", WrapError(ErrorInternal, "session ID", "read cryptographic randomness", err)
	}

	return fmt.Sprintf(
		"mig-%s-%s",
		now.UTC().Format("20060102-150405"),
		hex.EncodeToString(random),
	), nil
}

package domain

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
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

func ValidateSessionID(id string) error {
	if problems := validation.IsDNS1123Label(id); len(problems) > 0 {
		return NewError(
			ErrorValidation,
			"session ID",
			"must be a DNS-1123 label: "+strings.Join(problems, "; "),
		)
	}
	return nil
}

package copyengine

import (
	"fmt"
	"regexp"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

// bandwidthLimitPattern accepts rsync --bwlimit syntax: a bare byte rate per
// second, or a fractional value with a binary decimal multiplier suffix
// (K/M/G). rsync rejects anything else at transfer time; validating here
// turns a typo into a CLI admission error instead of a failed tool job.
var bandwidthLimitPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?[kKmMgG]?$`)

// ValidateBandwidthLimit accepts the rsync --bwlimit value forms this project
// allows and rejects empty values and malformed units.
func ValidateBandwidthLimit(value string) error {
	if value == "" {
		return domain.NewError(
			domain.ErrorValidation,
			"bandwidth limit",
			"value is required",
		)
	}

	if !bandwidthLimitPattern.MatchString(value) {
		return domain.NewError(
			domain.ErrorValidation,
			"bandwidth limit",
			fmt.Sprintf(
				"%q must be a rate in KiB/s or a fractional value with a K/M/G suffix, for example 10m",
				value,
			),
		)
	}

	return nil
}

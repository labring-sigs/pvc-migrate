package kube

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const maxDNSLabelLength = 63

// BoundedName creates a DNS label while retaining the final semantic part and
// a digest of the complete name. The digest keeps long inputs distinct.
func BoundedName(prefix string, parts ...string) string {
	rawParts := append([]string{prefix}, parts...)
	raw := strings.Join(rawParts, "-")
	cleanPrefix := cleanNamePart(prefix)

	cleanParts := make([]string, len(parts))
	for i, part := range parts {
		cleanParts[i] = cleanNamePart(part)
	}

	clean := strings.Trim(strings.Join(append([]string{cleanPrefix}, cleanParts...), "-"), "-.")
	if len(clean) <= maxDNSLabelLength {
		return clean
	}

	digest := sha256.Sum256([]byte(raw))
	hash := hex.EncodeToString(digest[:])[:10]

	if len(cleanParts) == 0 {
		cleanParts = []string{"item"}
	}

	last := cleanParts[len(cleanParts)-1]

	fixed := cleanPrefix + "-" + last + "-" + hash
	if len(fixed) >= maxDNSLabelLength {
		last = last[:maxDNSLabelLength-len(cleanPrefix)-len(hash)-2]
		fixed = cleanPrefix + "-" + strings.Trim(last, "-.") + "-" + hash
	}

	middle := strings.Join(cleanParts[:len(cleanParts)-1], "-")

	available := max(maxDNSLabelLength-len(fixed)-1, 0)

	if len(middle) > available {
		middle = strings.TrimRight(middle[:available], "-.")
	}

	name := fixed
	if middle != "" {
		name = cleanPrefix + "-" + middle + "-" + last + "-" + hash
	}

	return strings.Trim(name, "-.")
}

func cleanNamePart(value string) string {
	value = strings.ToLower(value)

	var builder strings.Builder
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' ||
			char == '.' {
			builder.WriteRune(char)
		} else {
			builder.WriteByte('-')
		}
	}

	return strings.Trim(builder.String(), "-.")
}

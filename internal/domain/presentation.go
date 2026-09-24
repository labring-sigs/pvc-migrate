package domain

import "strings"

// Presentation is the audience a guidance message is rendered for. CLI
// planning quotes command flags; controller planning records messages
// verbatim on workflow CRs and events, which must stay with the spec-field
// spelling. One producer serves both surfaces by rendering field references
// through the audience instead of duplicating whole messages.
type Presentation int

const (
	PresentationCLI Presentation = iota
	PresentationController
)

// FieldRef spells a spec field for the audience: the matching CLI flag
// (kebab-case, -- prefixed) or the spec field itself.
func (p Presentation) FieldRef(field string) string {
	if p == PresentationCLI {
		return "--" + kebabCase(field)
	}

	return field
}

// FieldUse phrases the act of setting the field for the audience: "use
// --flag" for the CLI, "set field" for controller-recorded messages.
func (p Presentation) FieldUse(field string) string {
	if p == PresentationCLI {
		return "use --" + kebabCase(field)
	}

	return "set " + field
}

// kebabCase converts lowerCamelCase to kebab-case.
func kebabCase(value string) string {
	var b strings.Builder

	for index, char := range value {
		if char >= 'A' && char <= 'Z' {
			if index > 0 {
				b.WriteByte('-')
			}

			b.WriteRune(char - 'A' + 'a')

			continue
		}

		b.WriteRune(char)
	}

	return b.String()
}

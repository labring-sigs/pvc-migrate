package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
)

const (
	colorAuto   = "auto"
	colorAlways = "always"
	colorNever  = "never"
	ansiReset   = "\x1b[0m"
)

var (
	logLevelToken          = regexp.MustCompile(`(^|[ \t])level=(DEBUG|INFO|WARN|ERROR)`)
	componentToken         = regexp.MustCompile(`(^|[ \t])component=("[^"]*"|[^\s]+)`)
	toolPrefix             = regexp.MustCompile(`\[tool [^\]\r\n]+\]`)
	guidanceTitle          = regexp.MustCompile(`^(\s*Next steps for session .+ \(phase )([^)]+)(\):\s*)$`)
	guidanceCompletedToken = regexp.MustCompile(`(?i)\bcompleted\b`)
	componentColors        = map[string]string{
		"controller": "36",
		"planner":    "34",
		"reserver":   "33",
		"switcher":   "32",
		"migration":  "35",
		"backup":     "96",
		"tool":       "94",
		"kubernetes": "95",
		"rsync":      "96",
		"sshd":       "95",
		"rclone":     "94",
	}
)

// colorOutputWriter adds terminal presentation to text-mode stderr. It sits
// below logOutputWriter, so JSON records remain structured and uncolored.
type colorOutputWriter struct {
	target  io.Writer
	enabled func() bool
	mu      sync.Mutex
}

func newColorOutputWriter(target io.Writer, enabled func() bool) io.Writer {
	if target == nil {
		target = io.Discard
	}
	return &colorOutputWriter{target: target, enabled: enabled}
}

func (w *colorOutputWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.enabled == nil || !w.enabled() || bytes.Contains(data, []byte("\x1b[")) {
		return w.target.Write(data)
	}
	colored := colorizeLogText(data)
	n, err := w.target.Write(colored)
	if err != nil {
		return 0, err
	}
	if n != len(colored) {
		return 0, io.ErrShortWrite
	}
	return len(data), nil
}

func parseColorMode(value string) (string, error) {
	switch strings.ToLower(value) {
	case "", colorAuto:
		return colorAuto, nil
	case colorAlways:
		return colorAlways, nil
	case colorNever:
		return colorNever, nil
	default:
		return "", domain.NewError(domain.ErrorValidation, "flags", fmt.Sprintf("unsupported color mode %q", value))
	}
}

func colorEnabled(mode string, target io.Writer) bool {
	parsed, err := parseColorMode(mode)
	if err != nil || parsed == colorNever {
		return false
	}
	if parsed == colorAlways {
		return true
	}
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	file, ok := target.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func colorizeLogText(data []byte) []byte {
	var output strings.Builder
	output.Grow(len(data) + 32)
	for start := 0; start < len(data); {
		end := bytes.IndexByte(data[start:], '\n')
		if end < 0 {
			end = len(data)
		} else {
			end += start
		}
		output.WriteString(colorizeLogLine(string(data[start:end])))
		if end < len(data) {
			output.WriteByte('\n')
			end++
		}
		start = end
	}
	return []byte(output.String())
}

func colorizeLogLine(line string) string {
	trimmed := strings.TrimSpace(line)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "error:") {
		return ansi("1;31", line)
	}
	if strings.HasPrefix(lower, "warning:") {
		return ansi("1;33", line)
	}
	line = colorizeGuidanceLine(line)

	line = logLevelToken.ReplaceAllStringFunc(line, func(token string) string {
		prefix, token := splitFieldPrefix(token)
		level := strings.TrimPrefix(token, "level=")
		return prefix + "level=" + ansi(levelColor(level), level)
	})
	line = componentToken.ReplaceAllStringFunc(line, func(token string) string {
		prefix, token := splitFieldPrefix(token)
		value := strings.TrimPrefix(token, "component=")
		return prefix + "component=" + ansi(componentColor(value), value)
	})
	return toolPrefix.ReplaceAllStringFunc(line, func(token string) string {
		fields := strings.Fields(strings.TrimSuffix(strings.TrimPrefix(token, "[tool "), "]"))
		component := token
		if len(fields) > 0 {
			component = fields[len(fields)-1]
		}
		return ansi(componentColor(component), token)
	})
}

func colorizeGuidanceLine(line string) string {
	if match := guidanceTitle.FindStringSubmatchIndex(line); match != nil {
		return ansi("1;36", line[match[2]:match[3]]) +
			ansi(guidancePhaseColor(line[match[4]:match[5]]), line[match[4]:match[5]]) +
			ansi("1;36", line[match[6]:match[7]])
	}

	leadingLength := len(line) - len(strings.TrimLeft(line, " \t"))
	leading, content := line[:leadingLength], line[leadingLength:]
	if label, rest, found := strings.Cut(content, ":"); found {
		if color := guidanceLabelColor(label); color != "" {
			return leading + ansi(color, label+":") + rest
		}
	}
	if strings.HasPrefix(strings.ToLower(content), "verify ") {
		return leading + ansi("36", content)
	}
	return guidanceCompletedToken.ReplaceAllStringFunc(line, func(token string) string {
		return ansi("1;32", token)
	})
}

func guidancePhaseColor(phase string) string {
	switch domain.Phase(phase) {
	case domain.PhaseCompleted:
		return "1;32"
	case domain.PhaseFailed:
		return "1;31"
	default:
		return "1;33"
	}
}

func guidanceLabelColor(label string) string {
	normalized := strings.ToLower(strings.TrimSpace(label))
	switch {
	case strings.HasPrefix(normalized, "record"), strings.HasPrefix(normalized, "inspect"), strings.HasPrefix(normalized, "verify"):
		return "36"
	case strings.HasPrefix(normalized, "validate"), strings.HasPrefix(normalized, "cleanup action"):
		return "1;33"
	case strings.HasPrefix(normalized, "continue"), strings.HasPrefix(normalized, "resume"), strings.HasPrefix(normalized, "keep"):
		return "1;32"
	case strings.HasPrefix(normalized, "abort"), strings.HasPrefix(normalized, "roll back"), strings.HasPrefix(normalized, "finalize"), strings.HasPrefix(normalized, "discard"), strings.HasPrefix(normalized, "close retained"), strings.HasPrefix(normalized, "delete"):
		return "1;31"
	default:
		return ""
	}
}

func splitFieldPrefix(token string) (string, string) {
	if len(token) > 0 && (token[0] == ' ' || token[0] == '\t') {
		return token[:1], token[1:]
	}
	return "", token
}

func levelColor(level string) string {
	switch level {
	case "DEBUG":
		return "2;36"
	case "INFO":
		return "32"
	case "WARN":
		return "1;33"
	case "ERROR":
		return "1;31"
	default:
		return "0"
	}
}

func componentColor(value string) string {
	value = strings.Trim(value, `"'`)
	if color, ok := componentColors[value]; ok {
		return color
	}
	var hash uint32 = 2166136261
	for index := 0; index < len(value); index++ {
		hash ^= uint32(value[index])
		hash *= 16777619
	}
	const paletteSize = 12
	palette := [paletteSize]string{"36", "35", "34", "32", "33", "31", "96", "95", "94", "92", "93", "91"}
	return palette[hash%paletteSize]
}

func ansi(code, value string) string {
	return "\x1b[" + code + "m" + value + ansiReset
}

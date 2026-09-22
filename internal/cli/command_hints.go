package cli

import (
	"os"
	"runtime"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
)

// sessionCommandPrefixForCommand reconstructs the pvc-migrate invocation a
// guidance hint should print, carrying over connection and tuning flags the
// operator already chose.
func sessionCommandPrefixForCommand(value any, namespace string) string {
	args := []string{"pvc-migrate"}

	command, ok := value.(*cobra.Command)
	if ok {
		rootFlags := command.Root().PersistentFlags()
		for _, name := range []string{"kubeconfig", "context"} {
			if flag := rootFlags.Lookup(name); flag != nil && flag.Value.String() != "" {
				args = append(args, "--"+name, shellQuote(flag.Value.String()))
			}
		}

		for _, name := range []string{"timeout", "retries", "retry-backoff", "helm-timeout", "stream-tool-logs", "no-compress"} {
			if flag := rootFlags.Lookup(name); flag != nil && flag.Changed {
				args = append(args, "--"+name+"="+shellQuote(flag.Value.String()))
			}
		}
	}

	if namespace != "" && namespace != "pvc-migrate-system" {
		args = append(args, "--workflow-namespace", shellQuote(namespace))
	}

	return strings.Join(args, " ")
}

func kubectlCommandPrefixForCommand(value any) string {
	args := []string{"kubectl"}

	command, ok := value.(*cobra.Command)
	if !ok {
		return args[0]
	}

	rootFlags := command.Root().PersistentFlags()
	for _, name := range []string{"kubeconfig", "context"} {
		if flag := rootFlags.Lookup(name); flag != nil && flag.Value.String() != "" {
			args = append(args, "--"+name, shellQuote(flag.Value.String()))
		}
	}

	return strings.Join(args, " ")
}

type guidanceShell int

const (
	guidanceShellPOSIX guidanceShell = iota
	guidanceShellPowerShell
)

func shellQuote(value string) string {
	return shellQuoteFor(value, detectGuidanceShell(runtime.GOOS, os.Getenv))
}

func detectGuidanceShell(goos string, getenv func(string) string) guidanceShell {
	if goos == "windows" {
		if getenv("MSYSTEM") != "" {
			return guidanceShellPOSIX
		}
		return guidanceShellPowerShell
	}

	if getenv("PSModulePath") != "" {
		return guidanceShellPowerShell
	}

	return guidanceShellPOSIX
}

func shellQuoteFor(value string, shell guidanceShell) string {
	if value != "" {
		safe := true
		for _, char := range value {
			if unicode.IsLetter(char) || unicode.IsDigit(char) ||
				strings.ContainsRune("-._/:%+=,", char) {
				continue
			}

			safe = false

			break
		}

		if safe {
			return value
		}
	}

	if shell == guidanceShellPowerShell {
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}

	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

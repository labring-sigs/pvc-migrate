package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func targetsExistingSession(sessionID string, sourcePVCs []string, podName string) bool {
	return sessionID != "" && len(sourcePVCs) == 0 && podName == ""
}

func validateDestinationCapacityFlags(
	operation domain.Operation,
	existingSession bool,
	destinationCapacities []string,
	allowVolumeShrink bool,
	skipSourceUsageCheck bool,
	sourcePaths []string,
	destinationPaths []string,
) error {
	if err := validateTransferPathFlags(
		operation,
		existingSession,
		sourcePaths,
		destinationPaths,
	); err != nil {
		return err
	}

	if existingSession &&
		(len(destinationCapacities) > 0 || allowVolumeShrink || skipSourceUsageCheck) {
		return domain.NewError(
			domain.ErrorValidation,
			string(operation),
			"destination capacity, shrink, and source usage check flags cannot modify an existing session; create a new session with the requested options",
		)
	}

	if skipSourceUsageCheck && !allowVolumeShrink {
		return domain.NewError(
			domain.ErrorValidation,
			string(operation),
			"--skip-source-usage-check requires --allow-volume-shrink",
		)
	}

	if allowVolumeShrink && len(destinationCapacities) == 0 {
		return domain.NewError(
			domain.ErrorValidation,
			string(operation),
			"--allow-volume-shrink requires --destination-capacity",
		)
	}

	for _, value := range destinationCapacities {
		capacityValue := strings.TrimSpace(value)
		if key, mapped, ok := strings.Cut(capacityValue, "="); ok {
			if strings.TrimSpace(key) == "" || strings.TrimSpace(mapped) == "" {
				return domain.NewError(
					domain.ErrorValidation,
					string(operation),
					fmt.Sprintf(
						"--destination-capacity %q must use source-pvc-name=capacity",
						value,
					),
				)
			}

			capacityValue = strings.TrimSpace(mapped)
		}

		parsed, err := resource.ParseQuantity(capacityValue)
		if err != nil {
			return domain.NewError(
				domain.ErrorValidation,
				string(operation),
				fmt.Sprintf("--destination-capacity %q is invalid: %v", value, err),
			)
		}

		if parsed.Sign() <= 0 {
			return domain.NewError(
				domain.ErrorValidation,
				string(operation),
				fmt.Sprintf("--destination-capacity %q must be positive", value),
			)
		}
	}

	return nil
}

func validateTransferPathFlags(
	operation domain.Operation,
	existingSession bool,
	sourcePaths []string,
	destinationPaths []string,
) error {
	if len(sourcePaths) == 0 && len(destinationPaths) == 0 {
		return nil
	}

	if existingSession {
		return domain.NewError(
			domain.ErrorValidation,
			string(operation),
			"transfer paths cannot modify an existing session; set them when creating the session",
		)
	}

	for _, input := range append(append([]string(nil), sourcePaths...), destinationPaths...) {
		value := strings.TrimSpace(input)
		if _, mapped, ok := strings.Cut(value, "="); ok {
			value = strings.TrimSpace(mapped)
		}

		if value == "" {
			return domain.NewError(
				domain.ErrorValidation,
				string(operation),
				fmt.Sprintf("transfer path %q is empty; use . for the PVC root", input),
			)
		}

		if _, err := domain.NormalizeTransferPath(value); err != nil {
			return domain.NewError(
				domain.ErrorValidation,
				string(operation),
				fmt.Sprintf("transfer path %q is invalid: %v", input, err),
			)
		}
	}

	return nil
}

func printCopyDryRunResult(
	cmd *cobra.Command,
	runtime *commandRuntime,
	object crclient.Object,
	namespace string,
) error {
	if err := runtime.printer.Print(object); err != nil {
		return err
	}

	args := []string{
		sessionCommandPrefixForCommand(cmd, namespace),
		"copy", "--session", shellQuote(object.GetName()),
	}
	for _, name := range []string{"online", "source-node", "strategy", "verify-checksum", "delete-extraneous", "unused-storage-policy"} {
		if flag := cmd.Flags().Lookup(name); flag != nil && flag.Changed {
			if flag.Value.Type() == "bool" {
				args = append(args, "--"+name+"="+flag.Value.String())
			} else {
				value := flag.Value.String()
				if name == "strategy" {
					values, err := cmd.Flags().GetStringSlice(name)
					if err != nil {
						return err
					}

					value = strings.Join(values, ",")
				}

				args = append(args, "--"+name, shellQuote(value))
			}
		}
	}

	args = append(args, "--dry-run=false")
	_, err := fmt.Fprintf(
		cmd.ErrOrStderr(),
		"\nCopy validation passed without persistent changes. The displayed Copy spec is a preview. Execute:\n  %s\n",
		strings.Join(args, " "),
	)

	return err
}

func (r *rootState) confirm(ctx context.Context, command *cobra.Command, expected string) error {
	if err := ctx.Err(); err != nil {
		return domain.WrapError(domain.ErrorTimeout, "approval", "approval canceled", err)
	}

	if r.global.assumeYes {
		return nil
	}

	if expected == "" {
		return domain.NewError(domain.ErrorValidation, "approval", "approval identity is empty")
	}

	if _, err := fmt.Fprintf(command.ErrOrStderr(), "Type %s to approve: ", expected); err != nil {
		return err
	}

	type scanResult struct {
		actual string
		err    error
	}

	result := make(chan scanResult, 1)
	go func() {
		var actual string

		_, err := fmt.Fscan(command.InOrStdin(), &actual)
		result <- scanResult{actual: actual, err: err}
	}()

	var scanned scanResult
	select {
	case <-ctx.Done():
		return domain.WrapError(
			domain.ErrorTimeout,
			"approval",
			"typed approval canceled",
			ctx.Err(),
		)
	case scanned = <-result:
		if err := ctx.Err(); err != nil {
			return domain.WrapError(domain.ErrorTimeout, "approval", "typed approval canceled", err)
		}
	}

	if scanned.err != nil {
		return domain.WrapError(
			domain.ErrorPrecondition,
			"approval",
			"typed approval or --yes is required",
			scanned.err,
		)
	}

	if scanned.actual != expected {
		return domain.NewError(domain.ErrorPrecondition, "approval", "typed approval did not match")
	}

	return nil
}

func requireReady(plan domain.PlanSummaryReader) error {
	summary := plan.Summary()
	if summary != nil && summary.Ready {
		return nil
	}

	return domain.NewError(
		domain.ErrorPrecondition,
		"plan",
		"migration plan contains failed checks",
	)
}

func requireReadyWithOutput(
	runtime *commandRuntime,
	plan *domain.TransferPlan,
	guidance io.Writer,
	advice func(domain.Check) string,
) error {
	if plan.Summary().Ready {
		return nil
	}

	if err := runtime.printer.Print(plan); err != nil {
		return err
	}

	if _, err := fmt.Fprintln(
		guidance,
		"\nNo session or migration resources were created. Resolve the failed plan checks, then rerun the command.",
	); err != nil {
		return err
	}

	if err := writePlanFailureGuidance(guidance, plan.Checks, advice); err != nil {
		return err
	}

	return requireReady(plan)
}

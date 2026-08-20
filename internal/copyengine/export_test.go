package copyengine

import (
	"context"

	"github.com/utkuozdemir/pv-migrate/pvmigrate"
)

func StrategyValueForTest(value string) (pvmigrate.Strategy, error) { return strategyValue(value) }

func TransferEnginePathForTest(value string) string { return transferEnginePath(value) }

func ClassifyRunErrorForTest(
	ctx context.Context,
	operationID string,
	err error,
	timedOut bool,
) error {
	return classifyRunError(ctx, operationID, err, timedOut)
}

func NewPVMigrateForTest(run func(context.Context, pvmigrate.Migration) error) *PVMigrate {
	return &PVMigrate{run: run}
}

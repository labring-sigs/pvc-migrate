package planner

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

func validateDestinationCapacityValue(value string) error {
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return err
	}

	if quantity.Sign() <= 0 {
		return fmt.Errorf("capacity %s must be positive", quantity.String())
	}

	return nil
}

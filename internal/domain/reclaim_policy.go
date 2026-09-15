package domain

func ValidateReclaimPolicies(source, destination string) error {
	for _, field := range []struct{ name, value string }{
		{"sourcePVReclaimPolicy", source}, {"destinationPVCReclaimPolicy", destination},
	} {
		if field.value != "" && field.value != "Retain" && field.value != "Delete" {
			return NewError(
				ErrorValidation,
				"reclaim policy",
				field.name+" must be Retain or Delete",
			)
		}
	}

	return nil
}

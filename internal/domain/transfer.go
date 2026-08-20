package domain

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode"
)

const VolumeRootPath = "."

// TransferScope selects directory contents inside one source and destination PVC.
// A nil scope represents the full volume root on both sides.
type TransferScope struct {
	SourcePath      string `json:"sourcePath"      yaml:"sourcePath"`
	DestinationPath string `json:"destinationPath" yaml:"destinationPath"`
}

func NewTransferScope(sourcePath, destinationPath string) (*TransferScope, error) {
	source, err := NormalizeTransferPath(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("source path: %w", err)
	}

	destination, err := NormalizeTransferPath(destinationPath)
	if err != nil {
		return nil, fmt.Errorf("destination path: %w", err)
	}

	if source == VolumeRootPath && destination == VolumeRootPath {
		return nil, nil
	}

	return &TransferScope{SourcePath: source, DestinationPath: destination}, nil
}

func NormalizeTransferPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == VolumeRootPath {
		return VolumeRootPath, nil
	}

	if len(value) > 4096 {
		return "", errors.New("path exceeds 4096 bytes")
	}

	if path.IsAbs(value) {
		return "", fmt.Errorf("path %q must be relative to the PVC root", value)
	}

	if strings.Contains(value, `\`) {
		return "", fmt.Errorf("path %q contains a backslash; use slash-separated PVC paths", value)
	}

	for _, character := range value {
		if character == 0 || unicode.IsControl(character) {
			return "", errors.New("path contains a control character")
		}
	}

	if slices.Contains(strings.Split(value, "/"), "..") {
		return "", fmt.Errorf("path %q contains parent traversal", value)
	}

	cleaned := path.Clean(value)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path %q escapes the PVC root", value)
	}

	return cleaned, nil
}

func ValidateTransferScope(scope *TransferScope) error {
	if scope == nil {
		return nil
	}

	source, err := NormalizeTransferPath(scope.SourcePath)
	if err != nil {
		return fmt.Errorf("source path: %w", err)
	}

	destination, err := NormalizeTransferPath(scope.DestinationPath)
	if err != nil {
		return fmt.Errorf("destination path: %w", err)
	}

	if source != scope.SourcePath || destination != scope.DestinationPath {
		return errors.New("paths must be stored in normalized form")
	}

	if source == VolumeRootPath && destination == VolumeRootPath {
		return errors.New("full-volume transfers must omit transferScope")
	}

	return nil
}

func CloneTransferScope(scope *TransferScope) *TransferScope {
	if scope == nil {
		return nil
	}

	cloned := *scope

	return &cloned
}

func SourceTransferPath(scope *TransferScope) string {
	if scope == nil {
		return VolumeRootPath
	}
	return scope.SourcePath
}

func DestinationTransferPath(scope *TransferScope) string {
	if scope == nil {
		return VolumeRootPath
	}
	return scope.DestinationPath
}

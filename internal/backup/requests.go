package backup

import (
	"io"
	"log/slog"
	"time"

	"github.com/labring-sigs/pvc-migrate/internal/kube"
)

// ToolRuntime contains process and tool services shared by repository
// transfers. Workflow input, persistence, and repository identity belong to
// their callers; executors receive those from the concrete CRD objects.
type ToolRuntime struct {
	HelmTimeout            time.Duration
	KubeconfigPath         string
	KubeContext            string
	StreamToolLogs         bool
	StructuredLogs         bool
	ToolServiceAccountName string
	Writer                 io.Writer
	Logger                 *slog.Logger
	ToolImageProber        kube.ToolImageProber
}

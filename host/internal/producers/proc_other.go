//go:build !windows

package producers

import (
	"context"
	"errors"
)

const useWindowsProcSupport = false

func (w *ProcWatcher) listAgentPIDsWindows(ctx context.Context) (map[int]string, error) {
	return nil, errors.New("windows process support unavailable")
}

// sessionsForWindowsCWD is a no-op on non-Windows platforms, which use
// lsof-based open-file correlation instead.
func (w *ProcWatcher) sessionsForWindowsCWD(pid int, agent string) map[string]struct{} {
	return nil
}

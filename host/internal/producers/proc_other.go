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

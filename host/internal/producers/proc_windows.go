//go:build windows

package producers

import (
	"context"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const useWindowsProcSupport = true

func (w *ProcWatcher) listAgentPIDsWindows(ctx context.Context) (map[int]string, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(snapshot)

	result := make(map[int]string)
	var pe windows.ProcessEntry32
	pe.Size = uint32(unsafe.Sizeof(pe))
	if err := windows.Process32First(snapshot, &pe); err != nil {
		return result, nil
	}
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
		name := windows.UTF16ToString(pe.ExeFile[:])
		if isAgentExeName(name) {
			result[int(pe.ProcessID)] = name
		}
		if err := windows.Process32Next(snapshot, &pe); err != nil {
			break
		}
	}
	return result, nil
}

func isAgentExeName(name string) bool {
	switch normalizeAgentName(strings.TrimSpace(name)) {
	case "claude", "codex", "cursor":
		return true
	default:
		return false
	}
}

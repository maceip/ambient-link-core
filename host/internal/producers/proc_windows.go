//go:build windows

package producers

import (
	"context"
	"errors"
	"strings"
	"unsafe"

	"github.com/maceip/ambient-link-core/host/internal/proto"
	"golang.org/x/sys/windows"
)

const useWindowsProcSupport = true

var errReadPEB = errors.New("proc: read peb failed")

var (
	ntdll                      = windows.NewLazySystemDLL("ntdll.dll")
	procNtQueryInformationProc = ntdll.NewProc("NtQueryInformationProcess")
)

// processBasicInformation mirrors PROCESS_BASIC_INFORMATION (x64). Only
// PebBaseAddress is used; the rest preserves layout/alignment.
type processBasicInformation struct {
	ExitStatus                   uint32
	_                            uint32
	PebBaseAddress               uintptr
	AffinityMask                 uintptr
	BasePriority                 int32
	_                            int32
	UniqueProcessId              uintptr
	InheritedFromUniqueProcessId uintptr
}

// unicodeStringRemote mirrors UNICODE_STRING (x64): two USHORTs, 4 bytes
// padding, then a 64-bit Buffer pointer into the target's address space.
type unicodeStringRemote struct {
	Length        uint16
	MaximumLength uint16
	_             uint32
	Buffer        uintptr
}

// Well-known x64 offsets, stable across supported Windows versions.
const (
	pebOffsetProcessParameters = 0x20 // PEB.ProcessParameters
	rtlOffsetCurrentDirectory  = 0x38 // RTL_USER_PROCESS_PARAMETERS.CurrentDirectory.DosPath
	rtlOffsetCommandLine       = 0x70 // RTL_USER_PROCESS_PARAMETERS.CommandLine
)

// processParams reads a process's command line and current working directory by
// walking its PEB. This is the Windows equivalent of the lsof/ps correlation the
// watcher uses elsewhere — there is no other reliable way to read another
// process's cwd or full argv on Windows.
func processParams(pid int) (cmdline, cwd string, err error) {
	h, err := windows.OpenProcess(
		windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.PROCESS_VM_READ,
		false, uint32(pid))
	if err != nil {
		return "", "", err
	}
	defer windows.CloseHandle(h)

	var pbi processBasicInformation
	r, _, _ := procNtQueryInformationProc.Call(
		uintptr(h), 0 /*ProcessBasicInformation*/, uintptr(unsafe.Pointer(&pbi)),
		unsafe.Sizeof(pbi), 0)
	if r != 0 || pbi.PebBaseAddress == 0 {
		return "", "", errReadPEB
	}

	var procParams uintptr
	if e := readRemote(h, pbi.PebBaseAddress+pebOffsetProcessParameters,
		(*byte)(unsafe.Pointer(&procParams)), unsafe.Sizeof(procParams)); e != nil || procParams == 0 {
		return "", "", errReadPEB
	}

	cmdline = readRemoteUnicodeString(h, procParams+rtlOffsetCommandLine)
	cwd = readRemoteUnicodeString(h, procParams+rtlOffsetCurrentDirectory)
	return cmdline, cwd, nil
}

func readRemote(h windows.Handle, addr uintptr, dst *byte, n uintptr) error {
	var read uintptr
	return windows.ReadProcessMemory(h, addr, dst, n, &read)
}

func readRemoteUnicodeString(h windows.Handle, addr uintptr) string {
	var us unicodeStringRemote
	if err := readRemote(h, addr, (*byte)(unsafe.Pointer(&us)), unsafe.Sizeof(us)); err != nil {
		return ""
	}
	if us.Length == 0 || us.Buffer == 0 || us.Length > 0x8000 {
		return ""
	}
	buf := make([]uint16, us.Length/2)
	if err := readRemote(h, us.Buffer, (*byte)(unsafe.Pointer(&buf[0])), uintptr(us.Length)); err != nil {
		return ""
	}
	return windows.UTF16ToString(buf)
}

// listAgentPIDsWindows enumerates candidate processes and, for those that could
// host a coding agent (node/claude/codex/cursor-agent images), reads the full
// command line so node-launched agents (node.exe running cursor-agent/codex) are
// correctly identified — matching by image name alone misses them.
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
		pid := int(pe.ProcessID)
		if isCandidateImage(name) {
			comm := name
			if cmdline, _, perr := processParams(pid); perr == nil && cmdline != "" {
				comm = cmdline
			}
			if looksLikeAgentProcess(comm) || isAgentExeName(name) {
				result[pid] = comm
			}
		}
		if err := windows.Process32Next(snapshot, &pe); err != nil {
			break
		}
	}
	return result, nil
}

// isCandidateImage limits the (relatively costly) PEB reads to images that could
// be a coding-agent CLI or its node host.
func isCandidateImage(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "node.exe", "node_repl.exe", "claude.exe", "codex.exe", "cursor-agent.exe", "agent.exe":
		return true
	default:
		return false
	}
}

func isAgentExeName(name string) bool {
	switch normalizeAgentName(strings.TrimSpace(name)) {
	case "claude", "codex", "cursor":
		return true
	default:
		return false
	}
}

// sessionsForWindowsCWD correlates a live agent PID to a mux session by matching
// agent type and normalized working directory — the Windows replacement for
// lsof-based open-file correlation. Robust when several agents share a parent
// directory (e.g. claude and codex both in C:\Users\mac) because the agent type
// disambiguates.
func (w *ProcWatcher) sessionsForWindowsCWD(pid int, agent string) map[string]struct{} {
	if w.cfg.LiveSessions == nil {
		return nil
	}
	_, cwd, err := processParams(pid)
	if err != nil || cwd == "" {
		if w.cfg.Logger != nil {
			w.cfg.Logger.Info("proc: cwd read failed", "pid", pid, "agent", agent, "err", err)
		}
		return nil
	}
	wantAgent := normalizeAgentName(agent)
	wantCWD := normalizeCWD(cwd)
	if wantAgent == "" || wantCWD == "" {
		return nil
	}
	out := make(map[string]struct{})
	for _, s := range w.cfg.LiveSessions() {
		if s.SessionID == "" || s.State == proto.StateDead {
			continue
		}
		if normalizeAgentName(s.Agent) != wantAgent {
			continue
		}
		if normalizeCWD(s.CWD) != wantCWD {
			continue
		}
		out[s.SessionID] = struct{}{}
	}
	if len(out) == 0 {
		if w.cfg.Logger != nil {
			// Debug-level: fires every sweep for agents in dirs with no live
			// session (e.g. a cursor-agent in another project) — expected noise.
			w.cfg.Logger.Debug("proc: cwd no-match", "pid", pid, "agent", wantAgent,
				"proc_cwd", cwd, "proc_cwd_norm", wantCWD)
		}
		return nil
	}
	return out
}

//go:build windows

package delivery

import (
	"fmt"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	keyEvent       = 0x0001
	vkReturn       = 0x0D
	stdInputHandle = uint32(0xfffffff6)
)

var (
	kernel32               = syscall.NewLazyDLL("kernel32.dll")
	procAttachConsole      = kernel32.NewProc("AttachConsole")
	procFreeConsole        = kernel32.NewProc("FreeConsole")
	procWriteConsoleInputW = kernel32.NewProc("WriteConsoleInputW")
)

type inputRecord struct {
	EventType uint16
	_         uint16
	KeyEvent  keyEventRecord
}

type keyEventRecord struct {
	KeyDown         int32
	RepeatCount     uint16
	VirtualKeyCode  uint16
	VirtualScanCode uint16
	UnicodeChar     uint16
	ControlKeyState uint32
}

// SendProcessInput writes keyboard input to the console owned by pid. It is the
// Windows equivalent of the Unix TTY adapter and does not require focusing the
// terminal window.
func SendProcessInput(pid int, text string, enter bool) error {
	if pid <= 0 {
		return fmt.Errorf("delivery: invalid pid %d", pid)
	}
	if text == "" && !enter {
		return nil
	}
	// A process can only be attached to one console at a time. Service launches
	// usually have none, but detach first so foreground shells do not poison
	// delivery during local development.
	_, _, _ = procFreeConsole.Call()
	r1, _, e1 := procAttachConsole.Call(uintptr(uint32(pid)))
	if r1 == 0 {
		return fmt.Errorf("delivery: attach console %d: %w", pid, e1)
	}
	defer procFreeConsole.Call()

	h, closeInput, err := consoleInputHandle()
	if err != nil {
		return fmt.Errorf("delivery: console input handle: %w", err)
	}
	defer closeInput()
	if h == windows.InvalidHandle || h == 0 {
		return fmt.Errorf("delivery: invalid console input handle")
	}

	records := make([]inputRecord, 0, len(text)*2+2)
	for _, ch := range utf16.Encode([]rune(text)) {
		records = appendKey(records, ch, 0)
	}
	if enter {
		records = appendKey(records, '\r', vkReturn)
	}
	if len(records) == 0 {
		return nil
	}
	var written uint32
	r1, _, e1 = procWriteConsoleInputW.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&records[0])),
		uintptr(uint32(len(records))),
		uintptr(unsafe.Pointer(&written)),
	)
	if r1 == 0 {
		return fmt.Errorf("delivery: write console input %d: %w", pid, e1)
	}
	if written != uint32(len(records)) {
		return fmt.Errorf("delivery: short console input write %d/%d", written, len(records))
	}
	return nil
}

func consoleInputHandle() (windows.Handle, func(), error) {
	name, err := windows.UTF16PtrFromString("CONIN$")
	if err != nil {
		return 0, func() {}, err
	}
	h, err := windows.CreateFile(
		name,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		0,
		0,
	)
	if err == nil {
		return h, func() { _ = windows.CloseHandle(h) }, nil
	}
	stdin, stdinErr := windows.GetStdHandle(stdInputHandle)
	if stdinErr != nil {
		return 0, func() {}, fmt.Errorf("open CONIN$: %w; std input: %w", err, stdinErr)
	}
	return stdin, func() {}, nil
}

func appendKey(records []inputRecord, ch uint16, vk uint16) []inputRecord {
	records = append(records, inputRecord{
		EventType: keyEvent,
		KeyEvent: keyEventRecord{
			KeyDown:        1,
			RepeatCount:    1,
			VirtualKeyCode: vk,
			UnicodeChar:    ch,
		},
	})
	records = append(records, inputRecord{
		EventType: keyEvent,
		KeyEvent: keyEventRecord{
			KeyDown:        0,
			RepeatCount:    1,
			VirtualKeyCode: vk,
			UnicodeChar:    ch,
		},
	})
	return records
}

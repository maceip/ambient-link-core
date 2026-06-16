//go:build !windows

package delivery

import (
	"fmt"
	"os"
	"strings"
)

// WriteTTY injects text into a running terminal session by writing to its
// controlling device (e.g. /dev/ttys006). enter sends a carriage return after
// the text, which terminal apps expect for submission.
func WriteTTY(tty, text string, enter bool) error {
	dev := ttyDevice(tty)
	if dev == "" {
		return fmt.Errorf("delivery: invalid tty %q", tty)
	}
	f, err := os.OpenFile(dev, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("delivery: open %s: %w", dev, err)
	}
	defer f.Close()
	if text != "" {
		if _, err := f.Write([]byte(text)); err != nil {
			return fmt.Errorf("delivery: write %s: %w", dev, err)
		}
	}
	if enter {
		if _, err := f.Write([]byte("\r")); err != nil {
			return fmt.Errorf("delivery: enter %s: %w", dev, err)
		}
	}
	return nil
}

func ttyDevice(tty string) string {
	tty = strings.TrimSpace(tty)
	if tty == "" || tty == "?" || tty == "??" {
		return ""
	}
	if strings.HasPrefix(tty, "/dev/") {
		return tty
	}
	return "/dev/" + strings.TrimPrefix(tty, "/dev/")
}

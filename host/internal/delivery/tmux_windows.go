//go:build windows

package delivery

import "fmt"

func SendTmuxPID(pid int, text string, enter bool) error {
	return fmt.Errorf("delivery: tmux not available on windows")
}

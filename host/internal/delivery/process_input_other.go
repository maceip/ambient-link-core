//go:build !windows

package delivery

import "fmt"

func SendProcessInput(pid int, text string, enter bool) error {
	return fmt.Errorf("delivery: process input not available on this platform")
}

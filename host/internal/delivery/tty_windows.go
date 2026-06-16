//go:build windows

package delivery

import "fmt"

func WriteTTY(tty, text string, enter bool) error {
	return fmt.Errorf("delivery: tty not available on windows")
}

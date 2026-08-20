//go:build !windows

package persistence

import (
	"os/exec"
	"syscall"
)

// setChildProcessGroup puts the child process in its own process group so
// that SIGKILL targets only the child, not the parent's group.
func setChildProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

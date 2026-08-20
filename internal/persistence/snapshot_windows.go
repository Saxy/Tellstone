//go:build windows

package persistence

import "os/exec"

// setChildProcessGroup is a no-op on Windows — process groups are not
// used; the child is killed individually via os.Process.Kill.
func setChildProcessGroup(_ *exec.Cmd) {}

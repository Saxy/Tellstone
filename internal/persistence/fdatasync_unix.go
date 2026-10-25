//go:build !windows

package persistence

import (
	"os"

	"golang.org/x/sys/unix"
)

func fdatasyncFile(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

func runtimeFSync(f *os.File) error {
	if err := fdatasyncFile(f); err == nil {
		return nil
	}
	return f.Sync()
}

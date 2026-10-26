//go:build !windows && (aix || linux || solaris || zos)

package persistence

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func fdatasyncFile(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}

func runtimeFSync(f *os.File) error {
	err := fdatasyncFile(f)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) {
		return f.Sync()
	}
	return err
}

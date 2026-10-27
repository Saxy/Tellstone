//go:build !windows && !(aix || linux || solaris || zos)

package persistence

import (
	"os"
)

func runtimeFSync(f *os.File) error {
	return f.Sync()
}

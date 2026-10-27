//go:build windows

package persistence

import (
	"os"
)

func runtimeFSync(f *os.File) error {
	return f.Sync()
}

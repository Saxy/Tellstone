package version

import "fmt"

// These variables are set at build time via -ldflags.
var (
	Version = "0.0.0-dev"
	Commit  = "unknown"
	Date    = "unknown"
)

func Print() {
	fmt.Printf("tellstone %s (commit: %s, built: %s)\n", Version, Commit, Date)
}

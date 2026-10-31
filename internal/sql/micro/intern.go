// Package micro contains micro-optimization passes for Phase 5.
package micro

import "sync"

// stringIntern is a simple string interner that ensures equal strings
// share the same underlying memory. This avoids allocations when the same
// string is used repeatedly as a key (e.g., column names, table names).
//
// Usage: call Intern(str) to get an interned reference. The interner is
// per-process and safe for concurrent use.
type stringIntern struct {
	mu     sync.Mutex
	strs   map[string]string
}

// interned stores the global interner instance.
var interned *stringIntern

// init initializes the interner.
func init() {
	interned = newStringIntern()
}

// newStringIntern creates a new string interner.
func newStringIntern() *stringIntern {
	return &stringIntern{strs: make(map[string]string)}
}

// Intern returns a "interned" version of str that guarantees that equal
// strings map to the same underlying byte slice. This is useful for keys
// that are compared often, such as column names, table names, and error
// messages, because string equality checks become pointer comparisons.
////
// The interner is safe for concurrent use from multiple goroutines.
func Intern(str string) string {
	interned.mu.Lock()
	defer interned.mu.Unlock()
	if existing, ok := interned.strs[str]; ok {
		return existing
	}
	interned.strs[str] = str
	return str
}

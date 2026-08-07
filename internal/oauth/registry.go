/*
Package oauth
Tellstone OAuth Provider Registry
File: registry.go
Description: Name-based provider registry. Providers are wired in during init() or early
startup, before any connection is served; reads after startup therefore never contend with
writes, so the backing map is plain and lock-free by design. Failures are logged through
the project logger instead of panicking: a bad registration must be visible, but it must
never take the whole server down with it.

Authors:

	Maximilian Hagen
*/
package oauth

import (
	"errors"
	"fmt"
	"sort"

	"github.com/Saxy/Tellstone/internal/log"
)

// registry holds every provider under its configured name. It is populated
// before the server starts serving and is read-only afterward, which is what
// makes a bare map safe without synchronization. Providers enter through
// Register only — never by pre-seeding this map — so every entry has passed
// the same validation.
var registry = make(map[string]Provider)

// logger receives registration failures. NoOp until SetLogger installs the
// real logger at startup; a provider registered before then that fails will be
// caught by the caller's own error handling.
var logger = log.NewNoOpLogger()

// SetLogger installs the logger used for registration failures. It must be
// called before providers are registered, so failures are never silent.
func SetLogger(l log.Logger) {
	if l != nil {
		logger = l
	}
}

// Register installs p under name. It is meant for package-level registration
// (init() or an explicit startup wiring list).
//
// A conflict (duplicate name, empty name, nil provider) is logged at error
// level and returned as an error instead of panicking — the first registration
// wins and state is left unchanged, so a misconfiguration can never silently
// replace a working provider. The returned error lets a startup wiring list
// fail fast; init()-time callers still get the log line.
func Register(name string, p Provider) error {
	if name == "" {
		err := errors.New("oauth: provider name must not be empty")
		logger.Log(log.LevelError, "oauth: refusing to register unnamed provider")
		return err
	}
	if p == nil {
		err := fmt.Errorf("oauth: nil provider registered as %q", name)
		logger.Log(log.LevelError, "oauth: refusing to register nil provider", log.String("provider", name))
		return err
	}
	if _, exists := registry[name]; exists {
		err := fmt.Errorf("oauth: provider %q already registered", name)
		logger.Log(log.LevelError, "oauth: provider already registered; keeping the original", log.String("provider", name))
		return err
	}
	registry[name] = p
	return nil
}

// Lookup returns the provider registered under name and whether one exists.
func Lookup(name string) (Provider, bool) {
	p, ok := registry[name]
	return p, ok
}

// Providers return the sorted names of all registered providers. The sort
// keeps help output and error messages deterministic.
func Providers() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

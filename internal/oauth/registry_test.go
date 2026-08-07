/*
Package oauth
Tellstone OAuth Provider Registry
File: registry_test.go
Description: Behavior tests for Register/Lookup/Providers and the logger-backed failure
path. Registration is reset between tests by rebuilding the registry and the logger from
scratch, mirroring how providers are wired in before the server starts.

Authors:

	Maximilian Hagen
*/
package oauth

import (
	"reflect"
	"testing"

	"github.com/Saxy/Tellstone/internal/log"
)

// recordingLogger captures levels and messages so tests can assert that
// failures were reported through the project logger and not swallowed.
type recordingLogger struct {
	levels []log.Level
	msgs   []string
}

func (r *recordingLogger) Enabled(log.Level) bool { return true }

func (r *recordingLogger) Log(level log.Level, msg string, _ ...log.Field) {
	r.levels = append(r.levels, level)
	r.msgs = append(r.msgs, msg)
}

// newMockProvider returns a provider whose Config identifies it by name, so
// tests can assert that Lookup returns the exact instance that was registered.
func newMockProvider(name string) Provider {
	return &mockProvider{cfg: Config{Issuer: "https://" + name + ".example.com"}}
}

func TestRegisterAndLookup(t *testing.T) {
	defer resetRegistry(t)
	p := newMockProvider("stackit")
	if err := Register("stackit", p); err != nil {
		t.Fatalf("Register() error = %v, want nil", err)
	}

	got, ok := Lookup("stackit")
	if !ok {
		t.Fatal("Lookup(stackit) ok = false, want true")
	}
	if got != p {
		t.Fatal("Lookup(stackit) returned a different instance than registered")
	}
	if got.Config().Issuer != "https://stackit.example.com" {
		t.Errorf("Config().Issuer = %q, want registered instance identity", got.Config().Issuer)
	}
}

func TestLookupMissing(t *testing.T) {
	defer resetRegistry(t)
	if _, ok := Lookup("no-such-provider"); ok {
		t.Fatal("Lookup(missing) ok = true, want false")
	}
}

func TestRegisterDuplicate(t *testing.T) {
	defer resetRegistry(t)
	rec := &recordingLogger{}
	SetLogger(rec)

	original := newMockProvider("first")
	if err := Register("dup", original); err != nil {
		t.Fatalf("initial Register() error = %v, want nil", err)
	}
	replacement := newMockProvider("second")
	if err := Register("dup", replacement); err == nil {
		t.Fatal("Register(duplicate) error = nil, want a conflict error")
	}

	// The original must win: a misconfiguration must never silently replace a
	// working provider.
	got, _ := Lookup("dup")
	if got != original {
		t.Fatal("Lookup(dup) did not keep the first registration")
	}
	if len(rec.msgs) != 1 || rec.levels[0] != log.LevelError {
		t.Errorf("logged %d messages with levels %v, want one LevelError", len(rec.msgs), rec.levels)
	}
}

func TestRegisterEmptyName(t *testing.T) {
	defer resetRegistry(t)
	rec := &recordingLogger{}
	SetLogger(rec)

	if err := Register("", newMockProvider("a")); err == nil {
		t.Fatal("Register(empty name) error = nil, want an error")
	}
	if len(rec.msgs) != 1 || rec.levels[0] != log.LevelError {
		t.Errorf("logged %d messages with levels %v, want one LevelError", len(rec.msgs), rec.levels)
	}
}

func TestRegisterNilProvider(t *testing.T) {
	defer resetRegistry(t)
	rec := &recordingLogger{}
	SetLogger(rec)

	if err := Register("nil", nil); err == nil {
		t.Fatal("Register(nil provider) error = nil, want an error")
	}
	if len(rec.msgs) != 1 || rec.levels[0] != log.LevelError {
		t.Errorf("logged %d messages with levels %v, want one LevelError", len(rec.msgs), rec.levels)
	}
}

func TestProvidersSorted(t *testing.T) {
	defer resetRegistry(t)
	// Register out of order; Providers must return sorted names for
	// deterministic help output and error messages.
	if err := Register("z-provider", newMockProvider("z")); err != nil {
		t.Fatal(err)
	}
	if err := Register("a-provider", newMockProvider("a")); err != nil {
		t.Fatal(err)
	}
	if err := Register("m-provider", newMockProvider("m")); err != nil {
		t.Fatal(err)
	}

	want := []string{"a-provider", "m-provider", "z-provider"}
	if got := Providers(); !reflect.DeepEqual(got, want) {
		t.Errorf("Providers() = %v, want %v", got, want)
	}
}

// resetRegistry rebuilds the backing map and logger so tests are independent
// of each other and of any init()-registered providers added later.
func resetRegistry(t *testing.T) {
	t.Helper()
	registry = make(map[string]Provider)
	logger = log.NewNoOpLogger()
}

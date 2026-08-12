package server

import (
	"testing"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/app/tellstone"
	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/rbac"
)

// newReplayServer builds a Server wired to the given flags, with RBAC enabled so
// there is a buffer to seed. Run() is never called: seedAuditReplay is exercised
// directly, since the surrounding startup opens listeners.
func newReplayServer(t *testing.T, rbacEnabled bool, args ...string) *Server {
	t.Helper()
	app := &tellstone.App{}
	app.Start(config.LoadConfig(args), log.NewNoOpLogger())
	s := NewServer(app)
	if rbacEnabled {
		s.policy = rbac.NewStore(&rbac.PolicyStore{}, log.NewNoOpLogger())
	}
	// seedAuditReplay reads the engine's own state, so the engine has to exist
	// first — the same order Run() uses.
	if err := s.initAudit(nil, nil); err != nil {
		t.Fatal("initAudit:", err)
	}
	t.Cleanup(func() { _ = s.audit.Close() })
	return s
}

// writePriorAuditHistory simulates a previous run by writing audit records to
// dir through a real engine and closing it.
func writePriorAuditHistory(t *testing.T, dir string) {
	t.Helper()
	e, err := audit.NewLogEngine(true, audit.ParseEventTypes("all"), dir, log.NewNoOpLogger(), false, nil, nil)
	if err != nil {
		t.Fatal("NewLogEngine:", err)
	}
	e.Record(audit.EventAuthFailure, "authentication failed",
		log.String("user", "alice"),
		log.String("remote_addr", "10.0.0.1:5000"),
		log.String("reason", "invalid password"),
	)
	e.Record(audit.EventACLDeny, "command denied by rbac policy",
		log.String("user", "bob"),
		log.String("command", "set"),
		log.String("key", "forbidden"),
		log.String("remote_addr", "10.0.0.2:5001"),
	)
	if err := e.Close(); err != nil {
		t.Fatal("Close:", err)
	}
}

// TestSeedAuditReplayRestoresHistory verifies ACL LOG carries the previous run's
// events across a restart when audit records go to a directory.
func TestSeedAuditReplayRestoresHistory(t *testing.T) {
	dir := t.TempDir()
	writePriorAuditHistory(t, dir)

	s := newReplayServer(t, true, "-enable-audit", "-audit-log-path", dir)
	s.seedAuditReplay()

	entries := s.policy.AuthLog()
	if len(entries) != 2 {
		t.Fatalf("AuthLog len = %d, want 2", len(entries))
	}
	if entries[0].Username != "alice" || entries[0].Reason != "invalid password" {
		t.Fatalf("entry 0 = %+v, want alice/invalid password", entries[0])
	}
	if entries[1].Username != "bob" || entries[1].Reason != "NOPERM command=set key=forbidden" {
		t.Fatalf("entry 1 = %+v, want bob/NOPERM", entries[1])
	}
}

// TestSeedAuditReplayNoopWhenAuditDisabled verifies nothing is restored when the
// operator never enabled audit logging, even with files present.
func TestSeedAuditReplayNoopWhenAuditDisabled(t *testing.T) {
	dir := t.TempDir()
	writePriorAuditHistory(t, dir)

	s := newReplayServer(t, true, "-audit-log-path", dir)
	s.seedAuditReplay()

	if entries := s.policy.AuthLog(); entries != nil {
		t.Fatalf("AuthLog = %+v, want nil", entries)
	}
}

// TestSeedAuditReplayNoopWhenStdout verifies the stdout sentinel is skipped
// rather than treated as a directory name.
func TestSeedAuditReplayNoopWhenStdout(t *testing.T) {
	s := newReplayServer(t, true, "-enable-audit", "-audit-log-path", "stdout")
	s.seedAuditReplay()

	if entries := s.policy.AuthLog(); entries != nil {
		t.Fatalf("AuthLog = %+v, want nil", entries)
	}
}

// TestSeedAuditReplayNoopWhenRBACDisabled verifies a nil policy is handled: with
// RBAC off there is no ACL LOG to restore.
func TestSeedAuditReplayNoopWhenRBACDisabled(t *testing.T) {
	dir := t.TempDir()
	writePriorAuditHistory(t, dir)

	s := newReplayServer(t, false, "-enable-audit", "-audit-log-path", dir)
	s.seedAuditReplay()

	if s.policy != nil {
		t.Fatal("policy should stay nil when RBAC is disabled")
	}
}

// TestSeedAuditReplayEmptyDirectory verifies a first run against a fresh audit
// directory restores nothing and does not fail.
func TestSeedAuditReplayEmptyDirectory(t *testing.T) {
	s := newReplayServer(t, true, "-enable-audit", "-audit-log-path", t.TempDir())
	s.seedAuditReplay()

	if entries := s.policy.AuthLog(); entries != nil {
		t.Fatalf("AuthLog = %+v, want nil", entries)
	}
}

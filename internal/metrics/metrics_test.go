package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Saxy/Tellstone/internal/audit"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/network"
	"github.com/Saxy/Tellstone/internal/storage"
)

// newNoOpAudit returns a disabled audit engine so the listener harness
// exercises the unguarded Record() hooks with zero audit output.
func newNoOpAudit() *audit.LogEngine {
	e, _ := audit.NewLogEngine(false, nil, "stdout", log.NewNoOpLogger(), false, nil, nil)
	return e
}

// TestCollectorEngineSnapshot verifies that the engine snapshot reflects
// the values reported by the storage engine after a few operations.
type fakeTLSMetrics struct {
	reloads uint64
	errors  uint64
	expiry  int64
}

func (m fakeTLSMetrics) ReloadTotal() uint64             { return m.reloads }
func (m fakeTLSMetrics) ReloadErrorsTotal() uint64       { return m.errors }
func (m fakeTLSMetrics) CertificateExpirySeconds() int64 { return m.expiry }

type fakeRBACMetrics struct {
	authFailures   uint64
	deniedCommands uint64
	roleCounts     map[string]uint64
}

func (m fakeRBACMetrics) AuthFailures() uint64                 { return m.authFailures }
func (m fakeRBACMetrics) DeniedCommands() uint64               { return m.deniedCommands }
func (m fakeRBACMetrics) RoleCommandCounts() map[string]uint64 { return m.roleCounts }

func TestAggregateCollectorTLSMetrics(t *testing.T) {
	collector := NewAggregateCollector(nil, nil, fakeTLSMetrics{reloads: 2, errors: 1, expiry: 1234}, nil)
	var output bytes.Buffer
	collector.WritePrometheus(&output)
	got := output.String()
	for _, want := range []string{
		"tellstone_tls_cert_reload_total 2",
		"tellstone_tls_cert_reload_errors_total 1",
		"tellstone_tls_cert_expiry_seconds 1234",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing TLS metric %q in output:\n%s", want, got)
		}
	}

	output.Reset()
	NewAggregateCollector(nil, nil, nil, nil).WritePrometheus(&output)
	if strings.Contains(output.String(), "tellstone_tls_") {
		t.Fatalf("TLS metrics must be omitted when rotation is disabled:\n%s", output.String())
	}
}

func TestAggregateCollectorRBACMetrics(t *testing.T) {
	collector := NewAggregateCollector(nil, nil, nil, fakeRBACMetrics{
		authFailures:   3,
		deniedCommands: 7,
		roleCounts:     map[string]uint64{"admin": 10, "readonly": 2},
	})
	var output bytes.Buffer
	collector.WritePrometheus(&output)
	got := output.String()
	for _, want := range []string{
		"tellstone_rbac_auth_failures_total 3",
		"tellstone_rbac_denied_commands_total 7",
		`tellstone_rbac_commands_total{role="admin"} 10`,
		`tellstone_rbac_commands_total{role="readonly"} 2`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing RBAC metric %q in output:\n%s", want, got)
		}
	}
	// HELP/TYPE metadata must be emitted once, before the per-role samples.
	if n := strings.Count(got, "# TYPE tellstone_rbac_commands_total counter"); n != 1 {
		t.Fatalf("tellstone_rbac_commands_total TYPE declared %d times, want 1:\n%s", n, got)
	}
	// Sorted role output: admin must be rendered before readonly.
	adminIdx := strings.Index(got, `role="admin"`)
	readonlyIdx := strings.Index(got, `role="readonly"`)
	if adminIdx < 0 || readonlyIdx < 0 || adminIdx > readonlyIdx {
		t.Fatalf("role metrics not sorted alphabetically:\n%s", got)
	}

	output.Reset()
	NewAggregateCollector(nil, nil, nil, nil).WritePrometheus(&output)
	if strings.Contains(output.String(), "tellstone_rbac_") {
		t.Fatalf("RBAC metrics must be omitted when RBAC is disabled:\n%s", output.String())
	}
}

func TestEscapeLabelValue(t *testing.T) {
	got := escapeLabelValue(`a"b\c
d`)
	if got != `a\"b\\c\nd` {
		t.Fatalf("escapeLabelValue = %q, want %q", got, `a\"b\\c\nd`)
	}
}

func TestCollectorEngineSnapshot(t *testing.T) {
	// Create a simple storage engine with a minimal chronometer (interval 1ms, 1 slot).
	eng := storage.NewEngine(time.Millisecond, 1, 0, log.NewNoOpLogger(), nil)
	// Perform a basic Set operation to affect counters.
	if err := eng.Set("key1", []byte("value"), 0); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	// Perform a Get to increase hit/miss counters.
	if _, ok := eng.Get("key1"); !ok {
		t.Fatalf("expected key to exist after Set")
	}
	// Create a dummy network server (no handler, no activity).
	srv := network.NewServer("", 0, nil, nil, log.NewNoOpLogger(), nil, "", nil, nil, newNoOpAudit())

	col := NewCollector(eng, srv, log.NewNoOpLogger())
	snap := col.GetEngineSnapshot()
	if snap.KeyCount != 1 {
		t.Fatalf("expected KeyCount 1, got %d", snap.KeyCount)
	}
	if snap.TotalCommands == 0 {
		t.Fatalf("expected TotalCommands > 0 after Set/Get")
	}
	// Snapshot fields that should be non‑negative.
	if snap.AllocatedBytes == 0 {
		t.Fatalf("AllocatedBytes should be >0 after storing a value")
	}
	// Ensure no panic when calling GetNetworkSnapshot.
	netSnap := col.GetNetworkSnapshot()
	_ = netSnap // silence unused variable warning
}

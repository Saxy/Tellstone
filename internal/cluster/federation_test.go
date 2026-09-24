/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: federation_test.go
Description: Tests for the Phase 7 federation policy engine: longest-prefix
home-cluster matching, wire round-trips, bootstrap/set via embedded PD etcd,
and FederationManager etcd Watch convergence.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestFederationPolicyMatch(t *testing.T) {
	p := FederationPolicy{
		Version: 2,
		Rules: []FederationRule{
			{Prefix: "user:europe:", Cluster: 2},
			{Prefix: "user:us:", Cluster: 3},
			{Prefix: "user:", Cluster: 2},
			{Prefix: "", Cluster: 1},
		},
	}
	cases := []struct {
		key  string
		want uint64
	}{
		{"user:europe:alice", 2}, // exact prefix match
		{"user:us:bob", 3},
		{"user:canada:curtis", 2}, // falls back to "user:"
		{"user:alice", 2},         // "user:" prefix
		{"global:thing", 1},       // only catch-all matches
		{"anything", 1},           // no rule at all → catch-all
		{"", 1},                   // empty key → catch-all
	}
	for _, c := range cases {
		if got, ok := p.Match([]byte(c.key)); !ok || got != c.want {
			t.Errorf("Match(%q) = %d ok=%v, want %d", c.key, got, ok, c.want)
		}
	}
}

func TestFederationPolicyLongestPrefixAndFallback(t *testing.T) {
	p := FederationPolicy{
		Rules: []FederationRule{
			{Prefix: "a", Cluster: 10},
			{Prefix: "ab", Cluster: 20},
			{Prefix: "abc", Cluster: 30},
		},
	}
	cases := map[string]uint64{
		"a":      10,
		"ab":     20,
		"abc":    30,
		"abcd":   30,
		"abcxyz": 30,
	}
	for key, want := range cases {
		if got, _ := p.Match([]byte(key)); got != want {
			t.Errorf("Match(%q) = %d, want %d", key, got, want)
		}
	}
	// Unmatched keys fall back to the local cluster, never to zero.
	if got := p.Home([]byte("zzzz"), 7); got != 7 {
		t.Errorf("Home(unmatched, local=7) = %d, want 7", got)
	}
}

func TestDefaultFederationPolicy(t *testing.T) {
	p := DefaultFederationPolicy(42)
	if len(p.Rules) != 1 || p.Rules[0].Prefix != "" || p.Rules[0].Cluster != 42 {
		t.Fatalf("default policy = %+v, want single catch-all to 42", p.Rules)
	}
	if got := p.Home([]byte("anything"), 42); got != 42 {
		t.Errorf("Home(anything) = %d, want 42 (everything local)", got)
	}
}

func TestEncodeDecodeFederationPolicy(t *testing.T) {
	p := FederationPolicy{
		Version: 3,
		Rules: []FederationRule{
			{Prefix: "user:eu:", Cluster: 2},
			{Prefix: "user:us:", Cluster: 3},
			{Prefix: "", Cluster: 1},
		},
	}
	enc, err := encodeFederationPolicy(p)
	if err != nil {
		t.Fatalf("encodeFederationPolicy: %v", err)
	}
	got, ok := decodeFederationPolicy(enc)
	if !ok {
		t.Fatal("decodeFederationPolicy returned ok=false")
	}
	if !reflect.DeepEqual(got, p) {
		t.Errorf("round-trip = %+v, want %+v", got, p)
	}
	for _, cut := range []int{0, 2, 9, 11, 15} {
		if g, ok := decodeFederationPolicy(enc[:cut]); ok {
			t.Errorf("decodeFederationPolicy(%d bytes) unexpectedly ok: %+v", cut, g)
		}
	}
}

func TestFederationPolicyBootstrapAndSet(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	def, err := BootstrapFederationPolicy(ctx, cli, 5)
	if err != nil {
		t.Fatalf("BootstrapFederationPolicy: %v", err)
	}
	if def.Version != 1 || def.Home([]byte("anything"), 5) != 5 {
		t.Errorf("bootstrapped policy = %+v, want version 1 all-local", def)
	}
	again, err := BootstrapFederationPolicy(ctx, cli, 5)
	if err != nil {
		t.Fatalf("BootstrapFederationPolicy #2: %v", err)
	}
	if again.Version != def.Version {
		t.Errorf("re-bootstrap version = %d, want %d", again.Version, def.Version)
	}

	// No policy present → get defaults to all-local, then Set bumps version.
	got, err := GetFederationPolicy(ctx, cli, 5)
	if err != nil {
		t.Fatalf("GetFederationPolicy: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("GetFederationPolicy version = %d, want 1", got.Version)
	}
	p := FederationPolicy{Rules: []FederationRule{
		{Prefix: "user:eu:", Cluster: 7},
	}}
	if err := SetFederationPolicy(ctx, cli, p); err != nil {
		t.Fatalf("SetFederationPolicy: %v", err)
	}
	got, err = GetFederationPolicy(ctx, cli, 5)
	if err != nil {
		t.Fatalf("GetFederationPolicy after set: %v", err)
	}
	if got.Version != 2 {
		t.Errorf("version after set = %d, want 2", got.Version)
	}
	if got.Home([]byte("user:eu:alice"), 5) != 7 {
		t.Errorf("Home(user:eu:alice) = %d, want 7 (remote)", got.Home([]byte("user:eu:alice"), 5))
	}
	if got.Home([]byte("other"), 5) != 5 {
		t.Errorf("Home(other) = %d, want 5 (local fallback)", got.Home([]byte("other"), 5))
	}

	// A second SetFederationPolicy from a different base must keep the version
	// monotonically increasing.
	if err := SetFederationPolicy(ctx, cli, FederationPolicy{Rules: []FederationRule{
		{Prefix: "user:us:", Cluster: 9},
	}}); err != nil {
		t.Fatalf("SetFederationPolicy #2: %v", err)
	}
	got, _ = GetFederationPolicy(ctx, cli, 5)
	if got.Version != 3 {
		t.Errorf("version after second set = %d, want 3", got.Version)
	}
}

// TestEncodeFederationPolicyPrefixTooLong verifies a prefix that cannot fit in
// the uint16 wire length field is rejected before appendBytesField wraps it.
func TestEncodeFederationPolicyPrefixTooLong(t *testing.T) {
	if _, err := encodeFederationPolicy(FederationPolicy{
		Rules: []FederationRule{{Prefix: "x", Cluster: 2}, {Prefix: strings.Repeat("a", math.MaxUint16+1), Cluster: 3}},
	}); err == nil {
		t.Fatal("encodeFederationPolicy accepted an over-length prefix")
	}
	if _, err := encodeFederationPolicy(FederationPolicy{
		Rules: []FederationRule{{Prefix: strings.Repeat("a", math.MaxUint16), Cluster: 3}},
	}); err != nil {
		t.Fatalf("encodeFederationPolicy rejected a max-length prefix: %v", err)
	}
}

// TestSetFederationPolicyCorrupt verifies a corrupt stored policy surfaces as
// an error instead of being silently overwritten (or treated as absent).
func TestSetFederationPolicyCorrupt(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	corrupt := strings.Repeat("x", 32) // not a valid policy frame (version+count read ok, then truncated)
	if _, err := cli.Put(ctx, federationPolicyKey, corrupt); err != nil {
		t.Fatalf("Put corrupt policy: %v", err)
	}
	if err := SetFederationPolicy(ctx, cli, FederationPolicy{Rules: []FederationRule{{Prefix: "a", Cluster: 2}}}); err == nil {
		t.Fatal("SetFederationPolicy succeeded over a corrupt stored policy")
	}
	// A manager seeded against the corrupt value must report the corruption,
	// not silently fall back to the all-local default.
	if _, err := NewFederationManager(cli, 5, DefaultFederationPolicy(5)).seed(ctx); err == nil {
		t.Fatal("seed succeeded against a corrupt stored policy")
	}
}

func TestFederationManagerWatch(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	mgr := NewFederationManager(cli, 5, DefaultFederationPolicy(5))
	if got := mgr.Home([]byte("user:eu:alice")); got != 5 {
		t.Fatalf("pre-watch Home = %d, want 5 (all-local default)", got)
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = mgr.Run(watchCtx)
		close(runDone)
	}()

	op := FederationPolicy{Rules: []FederationRule{
		{Prefix: "user:eu:", Cluster: 7},
	}}
	if err := SetFederationPolicy(ctx, cli, op); err != nil {
		t.Fatalf("SetFederationPolicy: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		if got := mgr.Home([]byte("user:eu:alice")); got == 7 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("watcher never converged on operator policy")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if got := mgr.Home([]byte("other")); got != 5 {
		t.Errorf("Home(other) = %d, want 5 (local fallback)", got)
	}

	watchCancel()
	<-runDone
}

/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: geo_test.go
Description: Tests for the Phase 6 geo policy engine and node registry:
prefix matching (longest-wins, global default), replica counts, NodeInfo and
GeoPolicy wire round-trips, and NodeZones lookups.

Authors:

	Maximilian Hagen
*/
package cluster

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestGeoPolicyMatch(t *testing.T) {
	p := GeoPolicy{
		Version: 2,
		Rules: []GeoRule{
			{Prefix: "user:europe:", Zone: "eu-west-1", Replicas: 3},
			{Prefix: "user:us:", Zone: "us-east-1", Replicas: 3},
			{Prefix: "user:", Zone: "eu-west-1", Replicas: 4},
			{Prefix: "", Zone: "*", Replicas: 3},
		},
	}

	cases := []struct {
		key  string
		want string
	}{
		{"user:europe:alice", "eu-west-1"}, // exact prefix match
		{"user:us:bob", "us-east-1"},
		{"user:canada:curtis", "eu-west-1"}, // falls back to "user:"
		{"user:alice", "eu-west-1"},         // "user:" prefix
		{"global:thing", "*"},               // only catch-all matches
		{"anything", "*"},                   // no rule at all → global
		{"", "*"},                           // empty key → global
	}
	for _, c := range cases {
		if got := p.Match([]byte(c.key)); got != c.want {
			t.Errorf("Match(%q) = %q, want %q", c.key, got, c.want)
		}
	}
}

func TestGeoPolicyMatchLongestPrefix(t *testing.T) {
	p := GeoPolicy{
		Rules: []GeoRule{
			{Prefix: "a", Zone: "z1"},
			{Prefix: "ab", Zone: "z2"},
			{Prefix: "abc", Zone: "z3"},
		},
	}
	cases := map[string]string{
		"a":      "z1",
		"ab":     "z2",
		"abc":    "z3",
		"abcd":   "z3",
		"abcxyz": "z3",
		"b":      "*",
	}
	for key, want := range cases {
		if got := p.Match([]byte(key)); got != want {
			t.Errorf("Match(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestGeoPolicyReplicaCount(t *testing.T) {
	p := GeoPolicy{
		Rules: []GeoRule{
			{Prefix: "user:", Zone: "eu-west-1", Replicas: 5},
			{Prefix: "", Zone: "*"}, // no explicit replicas → default
		},
	}
	if got := p.ReplicaCount([]byte("user:alice")); got != 5 {
		t.Errorf("ReplicaCount(user:alice) = %d, want 5", got)
	}
	if got := p.ReplicaCount([]byte("unknown")); got != GeoDefaultReplicas {
		t.Errorf("ReplicaCount(unknown) = %d, want %d", got, GeoDefaultReplicas)
	}
}

func TestDefaultGeoPolicy(t *testing.T) {
	p := DefaultGeoPolicy()
	if len(p.Rules) != 1 {
		t.Fatalf("default policy has %d rules, want 1", len(p.Rules))
	}
	if p.Rules[0].Prefix != "" || p.Rules[0].Zone != GeoZoneGlobal {
		t.Errorf("default rule = %+v, want catch-all global", p.Rules[0])
	}
	if got := p.Match([]byte("anything")); got != GeoZoneGlobal {
		t.Errorf("Match(anything) = %q, want %q", got, GeoZoneGlobal)
	}
}

func TestEncodeDecodeNodeInfo(t *testing.T) {
	n := NodeInfo{ID: 7, Zone: "eu-west-1", Addr: "10.0.0.7:9988"}
	enc := encodeNodeInfo(n)
	got, ok := decodeNodeInfo(enc)
	if !ok {
		t.Fatal("decodeNodeInfo returned ok=false")
	}
	if got != n {
		t.Errorf("round-trip = %+v, want %+v", got, n)
	}
}

func TestDecodeNodeInfoTruncated(t *testing.T) {
	n := NodeInfo{ID: 7, Zone: "eu-west-1", Addr: "10.0.0.7:9988"}
	enc := encodeNodeInfo(n)
	for _, cut := range []int{0, 1, 4, 8, 10, 12, len(enc) - 1} {
		if got, ok := decodeNodeInfo(enc[:cut]); ok {
			t.Errorf("decodeNodeInfo(%d bytes) unexpectedly ok: %+v", cut, got)
		}
	}
	if got, ok := decodeNodeInfo(enc); !ok {
		t.Errorf("full decode of %d bytes unexpectedly failed", len(enc))
	} else if got != n {
		t.Errorf("round-trip mismatch")
	}
}

func TestEncodeDecodeGeoPolicy(t *testing.T) {
	p := GeoPolicy{
		Version: 3,
		Rules: []GeoRule{
			{Prefix: "user:europe:", Zone: "eu-west-1", Replicas: 3},
			{Prefix: "", Zone: "*", Replicas: 5},
		},
	}
	enc := encodeGeoPolicy(p)
	got, ok := decodeGeoPolicy(enc)
	if !ok {
		t.Fatal("decodeGeoPolicy returned ok=false")
	}
	if !reflect.DeepEqual(got, p) {
		t.Errorf("round-trip = %+v, want %+v", got, p)
	}
}

func TestDecodeGeoPolicyTruncated(t *testing.T) {
	p := GeoPolicy{Rules: []GeoRule{{Prefix: "x", Zone: "z", Replicas: 3}}}
	enc := encodeGeoPolicy(p)
	for _, cut := range []int{0, 2, 9, 11, len(enc) - 4, len(enc) - 1} {
		if got, ok := decodeGeoPolicy(enc[:cut]); ok {
			t.Errorf("decodeGeoPolicy(%d bytes) unexpectedly ok: %+v", cut, got)
		}
	}
	if got, ok := decodeGeoPolicy(enc); !ok || !reflect.DeepEqual(got, p) {
		t.Errorf("full decode round-trip failed: got %+v ok=%v", got, ok)
	}
}

func TestNodeZones(t *testing.T) {
	z := NewNodeZones()
	if got := z.Zone(1); got != "" {
		t.Errorf("Zone(1) before any update = %q, want empty", got)
	}
	z.Update(NodeInfo{ID: 1, Zone: "us-east-1"})
	z.Update(NodeInfo{ID: 2, Zone: "eu-west-1"})
	if got := z.Zone(1); got != "us-east-1" {
		t.Errorf("Zone(1) = %q, want us-east-1", got)
	}
	// An unknown-zone re-registration must not downgrade a known zone.
	z.Update(NodeInfo{ID: 1, Zone: ""})
	if got := z.Zone(1); got != "us-east-1" {
		t.Errorf("Zone(1) after empty update = %q, want us-east-1", got)
	}
	// But a real zone change is allowed.
	z.Update(NodeInfo{ID: 1, Zone: "us-west-1"})
	if got := z.Zone(1); got != "us-west-1" {
		t.Errorf("Zone(1) after zone change = %q, want us-west-1", got)
	}
	zones := z.NodesInZone("eu-west-1")
	if len(zones) != 1 || zones[0] != 2 {
		t.Errorf("NodesInZone(eu-west-1) = %v, want [2]", zones)
	}
	if got := len(z.NodesInZone("nonexistent")); got != 0 {
		t.Errorf("NodesInZone(nonexistent) = %d entries, want 0", got)
	}
	z.Remove(2)
	if got := z.Zone(2); got != "" {
		t.Errorf("Zone(2) after remove = %q, want empty", got)
	}
	if got := len(z.Snapshot()); got != 1 {
		t.Errorf("Snapshot has %d entries, want 1", got)
	}
}

func TestGeoPolicyBootstrapAndSet(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	def, err := BootstrapGeoPolicy(ctx, cli)
	if err != nil {
		t.Fatalf("BootstrapGeoPolicy: %v", err)
	}
	if got := def.Match([]byte("anything")); got != GeoZoneGlobal {
		t.Errorf("bootstrapped policy Match = %q, want global", got)
	}
	if def.Version != 1 {
		t.Errorf("bootstrapped policy version = %d, want 1", def.Version)
	}

	// Idempotent bootstrap: second call returns the existing policy.
	again, err := BootstrapGeoPolicy(ctx, cli)
	if err != nil {
		t.Fatalf("BootstrapGeoPolicy #2: %v", err)
	}
	if again.Version != def.Version {
		t.Errorf("re-bootstrap version = %d, want %d", again.Version, def.Version)
	}

	// Set bumps the version and stores new rules.
	got, err := GetGeoPolicy(ctx, cli)
	if err != nil {
		t.Fatalf("GetGeoPolicy: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("GetGeoPolicy version = %d, want 1", got.Version)
	}
	p := GeoPolicy{Rules: []GeoRule{
		{Prefix: "user:europe:", Zone: "eu-west-1", Replicas: 3},
	}}
	if err := SetGeoPolicy(ctx, cli, p); err != nil {
		t.Fatalf("SetGeoPolicy: %v", err)
	}
	got, err = GetGeoPolicy(ctx, cli)
	if err != nil {
		t.Fatalf("GetGeoPolicy after set: %v", err)
	}
	if got.Version != 2 {
		t.Errorf("version after set = %d, want 2", got.Version)
	}
	if got.Match([]byte("user:europe:alice")) != "eu-west-1" {
		t.Errorf("Match after set = %q, want eu-west-1", got.Match([]byte("user:europe:alice")))
	}
	if got.Match([]byte("other")) != GeoZoneGlobal {
		t.Errorf("Match(unmatched) = %q, want global", got.Match([]byte("other")))
	}
}

func TestGeoManagerRegistrationAndWatch(t *testing.T) {
	cli, ctx, cancel := startRegionTest(t)
	defer cancel()

	mgr := NewGeoManager(cli, 1)
	if err := mgr.RegisterNode(ctx, "us-east-1", "10.0.0.1:9988"); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if got := mgr.Zones().Zone(1); got != "us-east-1" {
		t.Errorf("Zone(1) after register = %q, want us-east-1", got)
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		_ = mgr.Run(watchCtx)
		close(runDone)
	}()

	// A second node registers; the watcher must converge.
	if err := NewGeoManager(cli, 2).RegisterNode(ctx, "eu-west-1", "10.0.0.2:9988"); err != nil {
		t.Fatalf("RegisterNode(2): %v", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		if mgr.Zones().Zone(2) == "eu-west-1" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("watcher never learned zone of node 2")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// A policy write must also reach the watcher.
	op := GeoPolicy{Rules: []GeoRule{
		{Prefix: "user:europe:", Zone: "eu-west-1", Replicas: 3},
	}}
	if err := SetGeoPolicy(ctx, cli, op); err != nil {
		t.Fatalf("SetGeoPolicy: %v", err)
	}
	deadline = time.After(5 * time.Second)
	for {
		if got := mgr.Policy().Match([]byte("user:europe:alice")); got == "eu-west-1" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("watcher never converged on operator policy")
		case <-time.After(50 * time.Millisecond):
		}
	}

	watchCancel()
	<-runDone
}
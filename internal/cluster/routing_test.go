/*
Package cluster
Tellstone Cloud-Native In-Memory Database
File: routing_test.go
Description: Unit tests for the Phase 3 routing table.
*/
package cluster

import (
	"testing"
)

func TestRoutingTableFind(t *testing.T) {
	rt := NewRoutingTable()
	rt.Update(Region{ID: 1, StartKey: []byte(""), EndKey: []byte("user:fff"), Leader: 1, Epoch: 1})
	rt.Update(Region{ID: 2, StartKey: []byte("user:fff"), EndKey: []byte("user:zzz"), Leader: 3, Epoch: 1})
	rt.Update(Region{ID: 3, StartKey: []byte("user:zzz"), EndKey: nil, Leader: 5, Epoch: 1})

	cases := []struct {
		key  string
		want uint64
	}{
		{"", 1},
		{"a", 1},
		{"user:aaa", 1},
		{"user:fff", 3}, // inclusive start of region 2
		{"user:mid", 3},
		{"user:zzz", 5}, // inclusive start of region 3
		{"user:zzz\x00", 5},
		{"z", 5},
	}
	for _, c := range cases {
		r := rt.Find([]byte(c.key))
		if r == nil {
			t.Fatalf("Find(%q): nil route", c.key)
		}
		if r.Leader != c.want {
			t.Fatalf("Find(%q): leader = %d, want %d", c.key, r.Leader, c.want)
		}
	}
}

func TestRoutingTableUpdateStaleEpochDropped(t *testing.T) {
	rt := NewRoutingTable()
	rt.Update(Region{ID: 1, StartKey: []byte(""), EndKey: nil, Leader: 1, Epoch: 5})
	// Older epoch must be ignored.
	rt.Update(Region{ID: 1, StartKey: []byte(""), EndKey: nil, Leader: 9, Epoch: 4})
	if got := rt.Find([]byte("x")); got.Leader != 1 {
		t.Fatalf("stale update applied: leader = %d, want 1", got.Leader)
	}
	// Newer epoch must win.
	rt.Update(Region{ID: 1, StartKey: []byte(""), EndKey: nil, Leader: 9, Epoch: 6})
	if got := rt.Find([]byte("x")); got.Leader != 9 {
		t.Fatalf("fresh update ignored: leader = %d, want 9", got.Leader)
	}
}

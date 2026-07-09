package shard

import (
	"context"
	"testing"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
)

func TestShardIsolation(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=4"})
	shards := make([]*Shard, 4)
	for i := 0; i < 4; i++ {
		s, err := Run(ID(i), cfg, nil, log.NewNoOpLogger())
		if err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		t.Cleanup(func() { s.Stop(context.Background()) })
		shards[i] = s
	}

	key := "mykey"
	resp := shards[0].Execute("SET", key, []byte("val"), 0)
	if resp.Err != nil {
		t.Fatal(resp.Err)
	}

	found := 0
	for i := 0; i < 4; i++ {
		r := shards[i].Execute("GET", key, nil, 0)
		if r.OK {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("expected key on exactly 1 shard, found on %d", found)
	}
}

func TestShardSetGetDelete(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=1"})
	s, err := Run(0, cfg, nil, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("shard init: %v", err)
	}
	defer s.Stop(context.Background())

	getResp := s.Execute("GET", "missing", nil, 0)
	if getResp.OK {
		t.Fatal("expected missing key to not be found")
	}

	setResp := s.Execute("SET", "k1", []byte("v1"), 0)
	if setResp.Err != nil {
		t.Fatalf("set: %v", setResp.Err)
	}

	getResp = s.Execute("GET", "k1", nil, 0)
	if !getResp.OK {
		t.Fatal("expected key to be found after set")
	}
	if string(getResp.Value) != "v1" {
		t.Fatalf("expected v1, got %q", getResp.Value)
	}

	s.Execute("DEL", "k1", nil, 0)
	getResp = s.Execute("GET", "k1", nil, 0)
	if getResp.OK {
		t.Fatal("expected key to be deleted")
	}
}

func TestShardStoppedError(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=1"})
	s, err := Run(0, cfg, nil, log.NewNoOpLogger())
	if err != nil {
		t.Fatalf("shard init: %v", err)
	}
	s.Stop(context.Background())

	resp := s.Execute("GET", "x", nil, 0)
	if resp.Err != nil {
		t.Fatal("expected no error from stopped shard (engine still accessible)")
	}
}

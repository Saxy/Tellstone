package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/shard"
)

func testDistribution(t *testing.T, numShards int) {
	t.Helper()
	cfg := config.LoadConfig([]string{"-shards", fmt.Sprint(numShards)})
	shards := make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		s, err := shard.Run(shard.ID(i), cfg, nil, log.NewNoOpLogger())
		if err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		t.Cleanup(func() { s.Stop(context.Background()) })
		shards[i] = s
	}

	r := New(shards)

	counts := make([]int, numShards)
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := "key:" + string(rune(i))
		sid := hashKey(key) % r.numShards
		counts[sid]++
	}

	for i, c := range counts {
		if c == 0 {
			t.Errorf("shard %d received 0 keys out of %d with %d shards", i, numKeys, numShards)
		}
	}
}

func TestRouterDistributionPowerOfTwo(t *testing.T) {
	testDistribution(t, 16)
}

func TestRouterDistributionNonPowerOfTwo(t *testing.T) {
	testDistribution(t, 10)
	testDistribution(t, 7)
	testDistribution(t, 3)
}

func TestRouterSetGet(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=4"})
	shards := make([]*shard.Shard, 4)
	for i := 0; i < 4; i++ {
		s, err := shard.Run(shard.ID(i), cfg, nil, log.NewNoOpLogger())
		if err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		t.Cleanup(func() { s.Stop(context.Background()) })
		shards[i] = s
	}

	r := New(shards)

	setResp := r.Dispatch("SET", "mykey", []byte("myvalue"), 0)
	if setResp.Err != nil {
		t.Fatalf("set: %v", setResp.Err)
	}

	getResp := r.Dispatch("GET", "mykey", nil, 0)
	if !getResp.OK {
		t.Fatal("expected key to be found")
	}
	if string(getResp.Value) != "myvalue" {
		t.Fatalf("expected myvalue, got %q", getResp.Value)
	}

	delResp := r.Dispatch("DEL", "mykey", nil, 0)
	if !delResp.OK {
		t.Fatal("expected del to succeed")
	}

	getResp = r.Dispatch("GET", "mykey", nil, 0)
	if getResp.OK {
		t.Fatal("expected key to be deleted")
	}
}

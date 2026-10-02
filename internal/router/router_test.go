package router

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
		s, err := shard.Run(shard.ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
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
		s, err := shard.Run(shard.ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
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

// TestScanPrefixMergesShardsInKeyOrder is the property a cross-shard range read
// has to hold: a row's column keys hash to different shards, so the merge is the
// only thing standing between the caller and a row whose columns arrive out of
// order or not at all.
func TestScanPrefixMergesShardsInKeyOrder(t *testing.T) {
	const numShards = 8
	cfg := config.LoadConfig([]string{"-shards", fmt.Sprint(numShards)})
	shards := make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		s, err := shard.Run(shard.ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
		if err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		t.Cleanup(func() { s.Stop(context.Background()) })
		shards[i] = s
	}
	r := New(shards)

	// Rows written in an order unrelated to their key order, so a merge that
	// returned shard order would be visibly wrong.
	var want []string
	for i := 0; i < 200; i++ {
		rowID := fmt.Sprintf("%04d", i*7%200)
		for _, col := range []string{"age", "lastname", "name"} {
			key := fmt.Sprintf("tellstone/users/%s/%s", rowID, col)
			if err := shards[hashKey(key)%uint32(numShards)].Engine.Set(key, []byte(col), 0); err != nil {
				t.Fatal(err)
			}
		}
		want = append(want, fmt.Sprintf("tellstone/users/%s/", rowID))
	}
	sort.Strings(want)

	for _, prefix := range []string{
		"tellstone/users/",
		"tellstone/users/0007/",
		"tellstone/users/000",
		"nonexistent/",
	} {
		var got []string
		n := r.ScanPrefix(prefix, func(k, _ []byte) bool {
			got = append(got, string(k))
			return true
		})
		// Every key under the prefix, in order, with nothing else mixed in.
		var expect []string
		for _, rowPrefix := range want {
			if strings.HasPrefix(rowPrefix, prefix) {
				for _, col := range []string{"age", "lastname", "name"} {
					expect = append(expect, strings.TrimSuffix(rowPrefix, "/")+"/"+col)
				}
			}
		}
		if n != len(expect) {
			t.Fatalf("prefix %q: delivered %d keys, want %d", prefix, n, len(expect))
		}
		if !sort.StringsAreSorted(got) {
			t.Fatalf("prefix %q: merge returned keys out of order", prefix)
		}
		for i := range expect {
			if got[i] != expect[i] {
				t.Fatalf("prefix %q: key %d is %q, want %q", prefix, i, got[i], expect[i])
			}
		}
	}
}

// TestScanPrefixEarlyStop checks a caller can abandon a scan without the merge
// walking the rest of the range.
func TestScanPrefixEarlyStop(t *testing.T) {
	const numShards = 4
	cfg := config.LoadConfig([]string{"-shards", fmt.Sprint(numShards)})
	shards := make([]*shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		s, err := shard.Run(shard.ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
		if err != nil {
			t.Fatalf("shard %d: %v", i, err)
		}
		t.Cleanup(func() { s.Stop(context.Background()) })
		shards[i] = s
	}
	r := New(shards)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("p/%03d", i)
		if err := shards[hashKey(key)%uint32(numShards)].Engine.Set(key, []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	r.ScanPrefix("p/", func(_, _ []byte) bool {
		n++
		return n < 5
	})
	if n != 5 {
		t.Fatalf("early stop delivered %d keys, want 5", n)
	}
}

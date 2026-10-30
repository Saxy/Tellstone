package shard

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
	"github.com/Saxy/Tellstone/internal/persistence"
	"github.com/Saxy/Tellstone/internal/storage"
)

func TestShardIsolation(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=4"})
	shards := make([]*Shard, 4)
	for i := 0; i < 4; i++ {
		s, err := Run(ID(i), cfg, nil, nil, log.NewNoOpLogger(), nil)
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
	s, err := Run(0, cfg, nil, nil, log.NewNoOpLogger(), nil)
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

	delResp := s.Execute("DEL", "k1", nil, 0)
	if !delResp.OK {
		t.Fatal("expected DEL of an existing key to report a deletion")
	}
	getResp = s.Execute("GET", "k1", nil, 0)
	if getResp.OK {
		t.Fatal("expected key to be deleted")
	}
	if miss := s.Execute("DEL", "k1", nil, 0); miss.OK {
		t.Fatal("expected DEL of a missing key to report no deletion")
	}
}

func TestShardStoppedError(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=1"})
	s, err := Run(0, cfg, nil, nil, log.NewNoOpLogger(), nil)
	if err != nil {
		t.Fatalf("shard init: %v", err)
	}
	s.Stop(context.Background())

	resp := s.Execute("GET", "x", nil, 0)
	if resp.Err != nil {
		t.Fatal("expected no error from stopped shard (engine still accessible)")
	}
}

// TestExecuteBatchSetDelete applies a mixed batch through the real WAL, checks
// the responses are in input order, and replays the WAL to prove the batch was
// written durably as one unit.
func TestExecuteBatchSetDelete(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=1"})
	dir := t.TempDir()
	ps, err := persistence.NewStorage(true, log.NewNoOpLogger(), dir)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	s, err := Run(0, cfg, nil, nil, log.NewNoOpLogger(), ps)
	if err != nil {
		t.Fatalf("shard init: %v", err)
	}
	defer s.Stop(context.Background())

	if resp := s.Execute(CmdSet, "a", []byte("old-a"), 0); resp.Err != nil {
		t.Fatalf("seed set: %v", resp.Err)
	}
	if resp := s.Execute(CmdSet, "c", []byte("old-c"), 0); resp.Err != nil {
		t.Fatalf("seed set: %v", resp.Err)
	}

	ops := []BatchOp{
		{Op: CmdSet, Key: "a", Value: []byte("new-a")},
		{Op: CmdSet, Key: "b", Value: []byte("bee")},
		{Op: CmdDel, Key: "c"},
		{Op: CmdDel, Key: "missing"},
	}
	resps := s.ExecuteBatch(ops)
	if len(resps) != len(ops) {
		t.Fatalf("got %d responses, want %d", len(resps), len(ops))
	}
	wantOK := []bool{true, true, true, false}
	for i, r := range resps {
		if r.Err != nil {
			t.Fatalf("response %d: %v", i, r.Err)
		}
		if r.OK != wantOK[i] {
			t.Fatalf("response %d OK = %v, want %v", i, r.OK, wantOK[i])
		}
	}

	if v, _ := s.Engine.Get("a"); string(v) != "new-a" {
		t.Fatalf("a = %q, want new-a", v)
	}
	if v, _ := s.Engine.Get("b"); string(v) != "bee" {
		t.Fatalf("b = %q, want bee", v)
	}
	if _, ok := s.Engine.Get("c"); ok {
		t.Fatal("c should be deleted")
	}

	// Replay the WAL into a fresh engine and confirm the batch is durable.
	s.Stop(context.Background())
	if err := ps.CloseShard(0); err != nil {
		t.Fatalf("CloseShard: %v", err)
	}
	if err := ps.OpenShard(0, nil); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ps.CloseShard(0)
	engine2 := storage.NewEngine(0, 0, 0, nil, nil)
	defer engine2.Close()
	if err := ps.LoadShard(0, engine2); err != nil {
		t.Fatalf("LoadShard: %v", err)
	}
	if v, _ := engine2.Get("a"); string(v) != "new-a" {
		t.Fatalf("replayed a = %q, want new-a", v)
	}
	if v, _ := engine2.Get("b"); string(v) != "bee" {
		t.Fatalf("replayed b = %q, want bee", v)
	}
	if _, ok := engine2.Get("c"); ok {
		t.Fatal("replayed c should be deleted")
	}
}

// TestExecuteBatchRejectsUnknownOp checks a batch containing an unsupported
// operation applies nothing and reports the error on every response.
func TestExecuteBatchRejectsUnknownOp(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=1"})
	s, err := Run(0, cfg, nil, nil, log.NewNoOpLogger(), nil)
	if err != nil {
		t.Fatalf("shard init: %v", err)
	}
	defer s.Stop(context.Background())

	resps := s.ExecuteBatch([]BatchOp{{Op: "NOPE", Key: "x"}})
	if len(resps) != 1 || resps[0].Err == nil {
		t.Fatalf("expected one error response, got %+v", resps)
	}
	if _, ok := s.Engine.Get("x"); ok {
		t.Fatal("rejected batch mutated the engine")
	}
}

// TestExecuteBatchActorFallback drives the actor-enabled path, which keeps the
// per-operation semantics even though it loses the single-flush win.
func TestExecuteBatchActorFallback(t *testing.T) {
	cfg := config.LoadConfig([]string{"-shards=1"})
	s, err := Run(0, cfg, nil, nil, log.NewNoOpLogger(), nil)
	if err != nil {
		t.Fatalf("shard init: %v", err)
	}
	defer s.Stop(context.Background())
	s.EnableActor(false)

	resps := s.ExecuteBatch([]BatchOp{
		{Op: CmdSet, Key: "x", Value: []byte("1")},
		{Op: CmdDel, Key: "y"},
	})
	if len(resps) != 2 {
		t.Fatalf("got %d responses, want 2", len(resps))
	}
	if resps[0].Err != nil || !resps[0].OK {
		t.Fatalf("set response: %+v", resps[0])
	}
	if resps[1].Err != nil || resps[1].OK {
		t.Fatalf("delete response: %+v", resps[1])
	}
	if v, ok := s.Engine.Get("x"); !ok || string(v) != "1" {
		t.Fatalf("x = %q, %v; want 1, true", v, ok)
	}
}

func TestShardEnvelopeStartup(t *testing.T) {
	kek := bytes.Repeat([]byte{0x2a}, 32)
	cfg := config.LoadConfig([]string{
		"-shards=1",
		"-enable-encryption",
		"-enable-envelope",
		"-encryption-key=" + base64.StdEncoding.EncodeToString(kek),
		"-persistence-dir=" + t.TempDir(),
	})

	// First boot generates a per-shard DEK and wraps it with the KEK.
	s1, err := Run(0, cfg, kek, nil, log.NewNoOpLogger(), nil)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	s1.Stop(context.Background())

	// A restart with the same KEK unwraps the stored DEK instead of minting
	// a fresh one, so existing data stays decryptable.
	s2, err := Run(0, cfg, kek, nil, log.NewNoOpLogger(), nil)
	if err != nil {
		t.Fatalf("restart with same KEK: %v", err)
	}
	s2.Stop(context.Background())

	// A changed KEK must fail startup (fingerprint mismatch) rather than
	// silently generating fresh DEKs and bricking the dataset.
	other := bytes.Repeat([]byte{0x11}, 32)
	if s3, err := Run(0, cfg, other, nil, log.NewNoOpLogger(), nil); err == nil {
		s3.Stop(context.Background())
		t.Fatal("startup with changed KEK should fail")
	}
}

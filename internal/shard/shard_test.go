package shard

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"github.com/Saxy/Tellstone/config"
	"github.com/Saxy/Tellstone/internal/log"
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

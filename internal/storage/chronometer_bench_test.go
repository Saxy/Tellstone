package storage

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkChronometerEvictionPipeline(b *testing.B) {
	engine := NewEngine(1*time.Millisecond, 1000, 0, nil, nil)
	defer engine.Close()
	payload := []byte("raw_protobuf_bytes_32_bytes_long")
	numKeys := 50000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("tellstone:session:cluster-node-a:active:user:%d", i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localEngine := engine
		localPayload := payload
		localKeys := keys
		localNum := numKeys
		i := 0
		for pb.Next() {
			key := localKeys[i%localNum]
			ttl := time.Duration(5+(i%21)) * time.Millisecond
			localEngine.Set(key, localPayload, ttl)
			_, _ = localEngine.Get(key)
			i++
		}
	})
}

func BenchmarkChronometerEvictionPipelineSequential(b *testing.B) {
	engine := NewEngine(1*time.Millisecond, 1000, 0, nil, nil)
	defer engine.Close()
	payload := []byte("raw_protobuf_bytes_32_bytes_long")
	numKeys := 50000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("tellstone:session:cluster-node-a:active:user:%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%numKeys]
		ttl := time.Duration(5+(i%21)) * time.Millisecond

		engine.Set(key, payload, ttl)
		_, _ = engine.Get(key)
	}
}

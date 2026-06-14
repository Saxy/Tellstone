package storage

import (
	"sync"
	"testing"
	"time"
)

func TestChronometer_BasicEviction(t *testing.T) {
	var (
		mu          sync.Mutex
		expiredKeys []string
	)
	mockDeletion := func(key string) {
		mu.Lock()
		expiredKeys = append(expiredKeys, key)
		mu.Unlock()
	}
	interval := 5 * time.Millisecond
	c := NewChronometer(mockDeletion, interval, 10, nil)
	c.Start()
	defer c.Stop()
	c.Register("key:alpha", 5*time.Millisecond)
	c.Register("key:beta", 15*time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(expiredKeys) != 2 {
		t.Fatalf("expected 2 expired keys, got %d (found: %v)", len(expiredKeys), expiredKeys)
	}
	foundAlpha, foundBeta := false, false
	for _, k := range expiredKeys {
		if k == "key:alpha" {
			foundAlpha = true
		}
		if k == "key:beta" {
			foundBeta = true
		}
	}
	if !foundAlpha || !foundBeta {
		t.Errorf("missing expected keys in eviction pool: alpha=%t, beta=%t", foundAlpha, foundBeta)
	}
}

func TestChronometer_TTLRefresh(t *testing.T) {
	var (
		mu          sync.Mutex
		expiredKeys []string
	)
	keyGenerations := make(map[string]int)
	keyGenerations["session:user_1"] = 1
	mockDeletion := func(key string) {
		mu.Lock()
		defer mu.Unlock()
		if keyGenerations[key] == 1 {
			expiredKeys = append(expiredKeys, key)
		}
	}
	interval := 10 * time.Millisecond
	c := NewChronometer(mockDeletion, interval, 20, nil)
	c.Start()
	defer c.Stop()
	c.Register("session:user_1", 15*time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	mu.Lock()
	keyGenerations["session:user_1"] = 2 // Generation erhöht, alte Ticks werden ignoriert
	mu.Unlock()
	c.Register("session:user_1", 40*time.Millisecond)
	time.Sleep(15 * time.Millisecond)
	mu.Lock()
	if len(expiredKeys) > 0 {
		t.Errorf("key was evicted too early, expected generation check to filter old tick. Got: %v", expiredKeys)
	}
	mu.Unlock()
}

func TestChronometer_Concurrency_Race(t *testing.T) {
	mockDeletion := func(key string) {}

	interval := 2 * time.Millisecond
	c := NewChronometer(mockDeletion, interval, 50, nil)
	c.Start()
	defer c.Stop()

	var wg sync.WaitGroup
	goroutines := 50
	registrationsPerGoroutine := 100
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < registrationsPerGoroutine; i++ {
				// Wechselnde TTLs simulieren unregelmäßige Abstände
				ttl := time.Duration((i%5)+1) * time.Millisecond
				c.Register("concurrent_key", ttl)
			}
		}(g)
	}

	wg.Wait()
	time.Sleep(15 * time.Millisecond)
}

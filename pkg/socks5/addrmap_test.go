package socks5

import (
	"sync"
	"testing"
	"time"
)

func TestAddrMap_AgeBasedEviction(t *testing.T) {
	var m sync.Map

	now := time.Now()

	// Insert entries with different creation times.
	entries := []struct {
		key    string
		age    time.Duration
		expect bool // true = should survive cleanup
	}{
		{"fresh", 1 * time.Minute, true},
		{"old", 35 * time.Minute, false},
		{"mid", 20 * time.Minute, true},
		{"ancient", 60 * time.Minute, false},
	}

	for _, e := range entries {
		m.Store(e.key, &cachedHeader{
			header:    []byte(e.key),
			createdAt: now.Add(-e.age),
		})
	}

	// Simulate cleanup with a 30-minute max age.
	maxAge := 30 * time.Minute
	m.Range(func(k, v any) bool {
		entry := v.(*cachedHeader)
		if time.Since(entry.createdAt) > maxAge {
			m.Delete(k)
		}
		return true
	})

	for _, e := range entries {
		_, ok := m.Load(e.key)
		if ok != e.expect {
			t.Errorf("key %q: expected present=%v, got %v", e.key, e.expect, ok)
		}
	}
}

func TestAddrMap_ConcurrentStoreAndCleanup(t *testing.T) {
	var m sync.Map

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: continuously store entries.
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.Store(string(rune('a'+i%26)), &cachedHeader{
				header:    []byte{byte(i)},
				createdAt: time.Now(),
			})
		}
	}()

	// Goroutine 2: continuously Range + Delete (cleanup).
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			m.Range(func(k, v any) bool {
				entry := v.(*cachedHeader)
				if time.Since(entry.createdAt) > 1*time.Hour {
					m.Delete(k)
				}
				return true
			})
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Verify the map is still usable.
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count == 0 {
		t.Fatal("map is empty after concurrent store+cleanup")
	}
}

func TestAddrMap_StoreAndLoad(t *testing.T) {
	var m sync.Map

	m.Store("dest1", &cachedHeader{
		header:    []byte{0, 0, 0, 1, 2, 3},
		createdAt: time.Now(),
	})

	v, ok := m.Load("dest1")
	if !ok {
		t.Fatal("expected to load stored key")
	}
	entry := v.(*cachedHeader)
	if len(entry.header) != 6 {
		t.Fatalf("unexpected header length: %d", len(entry.header))
	}
	if entry.createdAt.IsZero() {
		t.Fatal("createdAt should not be zero")
	}
}

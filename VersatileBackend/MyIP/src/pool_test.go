package myipsvc

import (
	"testing"
	"time"
)

func TestPoolAcquireKeepsHostHashStable(t *testing.T) {
	pool, err := NewPool(Config{
		UpstreamURLs:   []string{"http://127.0.0.1:9001", "http://127.0.0.1:9002", "http://127.0.0.1:9003"},
		Strategy:       "host-hash",
		ConnectTimeout: 5 * time.Second,
		Cooldown:       5 * time.Second,
		MaxCooldown:    30 * time.Second,
	})
	if err != nil {
		t.Fatalf("new pool failed: %v", err)
	}
	first, err := pool.Acquire("example.com:443")
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	second, err := pool.Acquire("example.com:443")
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	if first.endpoint.id != second.endpoint.id {
		t.Fatalf("expected stable endpoint, got %d and %d", first.endpoint.id, second.endpoint.id)
	}
}

func TestPoolFailureMovesToAnotherEndpoint(t *testing.T) {
	pool, err := NewPool(Config{
		UpstreamURLs:   []string{"http://127.0.0.1:9101", "http://127.0.0.1:9102"},
		Strategy:       "round-robin",
		ConnectTimeout: 5 * time.Second,
		Cooldown:       30 * time.Second,
		MaxCooldown:    60 * time.Second,
	})
	if err != nil {
		t.Fatalf("new pool failed: %v", err)
	}
	first, err := pool.Acquire("")
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	pool.MarkFailure(first)
	second, err := pool.Acquire("")
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	if first.endpoint.id == second.endpoint.id {
		t.Fatalf("expected a different endpoint after failure")
	}
}

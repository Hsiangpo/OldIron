package myipsvc

import (
	"errors"
	"hash/fnv"
	"math"
	"sync"
	"time"
)

type Lease struct {
	endpoint *endpoint
}

type Pool struct {
	mu           sync.Mutex
	endpoints    []*endpoint
	strategy     string
	cooldown     time.Duration
	maxCooldown  time.Duration
	roundRobinAt uint64
}

func NewPool(cfg Config) (*Pool, error) {
	endpoints := make([]*endpoint, 0, len(cfg.UpstreamURLs))
	for idx, rawURL := range cfg.UpstreamURLs {
		item, err := newEndpoint(idx, rawURL, cfg.ConnectTimeout)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, item)
	}
	if len(endpoints) == 0 {
		return nil, errors.New("proxy pool has no endpoints")
	}
	return &Pool{
		endpoints:   endpoints,
		strategy:    cfg.Strategy,
		cooldown:    cfg.Cooldown,
		maxCooldown: cfg.MaxCooldown,
	}, nil
}

func (pool *Pool) Acquire(key string) (Lease, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	indexes := pool.availableIndexes(time.Now())
	if len(indexes) == 0 {
		return Lease{}, errors.New("proxy pool has no available endpoint")
	}
	chosen := pool.pickIndex(indexes, key)
	item := pool.endpoints[chosen]
	item.lastUsedAt = time.Now()
	return Lease{endpoint: item}, nil
}

func (pool *Pool) MarkSuccess(lease Lease) {
	if lease.endpoint == nil {
		return
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	item := pool.endpoints[lease.endpoint.id]
	item.failCount = 0
	item.cooldownUntil = time.Time{}
}

func (pool *Pool) MarkFailure(lease Lease) {
	if lease.endpoint == nil {
		return
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	item := pool.endpoints[lease.endpoint.id]
	item.failCount++
	cooldown := pool.cooldown * time.Duration(math.Pow(2, float64(item.failCount-1)))
	if cooldown > pool.maxCooldown {
		cooldown = pool.maxCooldown
	}
	item.cooldownUntil = time.Now().Add(cooldown)
}

func (pool *Pool) availableIndexes(now time.Time) []int {
	ready := make([]int, 0, len(pool.endpoints))
	for idx, item := range pool.endpoints {
		if item.cooldownUntil.IsZero() || !item.cooldownUntil.After(now) {
			ready = append(ready, idx)
		}
	}
	if len(ready) > 0 {
		return ready
	}
	fallback := make([]int, 0, len(pool.endpoints))
	for idx := range pool.endpoints {
		fallback = append(fallback, idx)
	}
	return fallback
}

func (pool *Pool) pickIndex(indexes []int, key string) int {
	if pool.strategy == "round-robin" || key == "" {
		index := indexes[int(pool.roundRobinAt%uint64(len(indexes)))]
		pool.roundRobinAt++
		return index
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return indexes[int(hasher.Sum32())%len(indexes)]
}

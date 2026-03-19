package firecrawlsvc

import (
	"errors"
	"os"
	"strings"
	"sync"
)

type keyState struct {
	value    string
	inFlight int
	disabled bool
}

type keyPool struct {
	keys     []*keyState
	limit    int
	next     int
	mutex    sync.Mutex
}

func newKeyPool(limit int) (*keyPool, error) {
	keys := loadKeys()
	if len(keys) == 0 {
		return nil, errors.New("missing firecrawl keys")
	}
	states := make([]*keyState, 0, len(keys))
	for _, key := range keys {
		states = append(states, &keyState{value: key})
	}
	if limit <= 0 {
		limit = 2
	}
	return &keyPool{keys: states, limit: limit}, nil
}

func loadKeys() []string {
	inline := splitKeys(os.Getenv("FIRECRAWL_KEYS"))
	if len(inline) > 0 {
		return inline
	}
	path := strings.TrimSpace(os.Getenv("FIRECRAWL_KEYS_FILE"))
	if path == "" {
		return nil
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return splitKeys(string(bytes))
}

func splitKeys(raw string) []string {
	values := []string{}
	normalized := strings.ReplaceAll(strings.ReplaceAll(raw, "\r", "\n"), ";", ",")
	for _, chunk := range strings.Split(normalized, "\n") {
		for _, part := range strings.Split(chunk, ",") {
			text := strings.TrimSpace(part)
			if text != "" && !strings.HasPrefix(text, "#") && !contains(values, text) {
				values = append(values, text)
			}
		}
	}
	return values
}

func (pool *keyPool) acquire() (*keyState, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	count := len(pool.keys)
	for offset := 0; offset < count; offset++ {
		index := (pool.next + offset) % count
		state := pool.keys[index]
		if state.disabled || state.inFlight >= pool.limit {
			continue
		}
		state.inFlight++
		pool.next = (index + 1) % count
		return state, nil
	}
	return nil, errors.New("no available firecrawl key")
}

func (pool *keyPool) release(state *keyState) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if state.inFlight > 0 {
		state.inFlight--
	}
}

func (pool *keyPool) disable(state *keyState) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	state.disabled = true
	state.inFlight = 0
}

func contains(values []string, target string) bool {
	for _, item := range values {
		if item == target {
			return true
		}
	}
	return false
}


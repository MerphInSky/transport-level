// Package senders remembers who initiated a request so the answer can be routed back.
package senders

import "sync"

// Registry maps request timestamps to sender names. It is safe for concurrent use.
type Registry struct {
	mu          sync.Mutex
	byTimestamp map[int64]string
}

func NewRegistry() *Registry {
	return &Registry{byTimestamp: make(map[int64]string)}
}

// Remember records the sender of a request identified by its timestamp.
func (r *Registry) Remember(timestamp int64, sender string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byTimestamp[timestamp] = sender
}

// Take returns the sender of a request and forgets it.
func (r *Registry) Take(timestamp int64) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	sender := r.byTimestamp[timestamp]
	delete(r.byTimestamp, timestamp)
	return sender
}
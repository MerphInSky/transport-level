// Package assembler buffers message segments and glues them back together.
package assembler

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dima040805/transport-level/internal/segment"
)

// Message is a reassembled document structure ready to be delivered.
type Message struct {
	Timestamp int64
	Payload   string
	Received  int
	Total     int
	// Incomplete is true when the message timed out before all segments arrived.
	Incomplete bool
}

type pending struct {
	total    int
	parts    map[int]string
	lastSeen time.Time
}

// Assembler collects segments per message timestamp. It is safe for concurrent use.
type Assembler struct {
	timeout time.Duration

	mu      sync.Mutex
	pending map[int64]*pending
}

// New returns an assembler that gives up on a message after timeout without new segments.
func New(timeout time.Duration) *Assembler {
	return &Assembler{timeout: timeout, pending: make(map[int64]*pending)}
}

// Add stores a segment. A repeated segment number replaces the earlier payload.
func (a *Assembler) Add(s segment.Segment, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	p, ok := a.pending[s.Timestamp]
	if !ok {
		p = &pending{total: s.TotalSegments, parts: make(map[int]string, s.TotalSegments)}
		a.pending[s.Timestamp] = p
	}
	p.parts[s.SegmentNumber] = s.DocumentStructureSegment
	p.lastSeen = now
}

// Collect removes and returns every message that is complete or has not
// received a segment for at least the timeout.
func (a *Assembler) Collect(now time.Time) []Message {
	a.mu.Lock()
	defer a.mu.Unlock()

	var ready []Message
	for ts, p := range a.pending {
		complete := len(p.parts) >= p.total
		if !complete && now.Sub(p.lastSeen) < a.timeout {
			continue
		}
		ready = append(ready, Message{
			Timestamp:  ts,
			Payload:    p.assemble(),
			Received:   len(p.parts),
			Total:      p.total,
			Incomplete: !complete,
		})
		delete(a.pending, ts)
	}
	return ready
}

// Pending reports how many messages are still being assembled.
func (a *Assembler) Pending() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending)
}

func (p *pending) assemble() string {
	numbers := make([]int, 0, len(p.parts))
	for n := range p.parts {
		numbers = append(numbers, n)
	}
	sort.Ints(numbers)

	var b strings.Builder
	for _, n := range numbers {
		b.WriteString(p.parts[n])
	}
	return b.String()
}
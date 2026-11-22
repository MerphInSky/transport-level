package collector

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dima040805/transport-level/internal/assembler"
	"github.com/dima040805/transport-level/internal/clients"
	"github.com/dima040805/transport-level/internal/segment"
	"github.com/dima040805/transport-level/internal/senders"
)

type fakeApp struct {
	mu  sync.Mutex
	got []clients.ReceiveRequest
}

func (f *fakeApp) Deliver(_ context.Context, req clients.ReceiveRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, req)
	return nil
}

func (f *fakeApp) received() []clients.ReceiveRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clients.ReceiveRequest(nil), f.got...)
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func strPtr(s string) *string { return &s }

func TestDeliverReady(t *testing.T) {
	asm := assembler.New(6 * time.Second)
	registry := senders.NewRegistry()
	app := &fakeApp{}
	c := New(asm, registry, app, time.Second, discard)

	now := time.Now()
	registry.Remember(100, "Ivan_Student")
	registry.Remember(200, "Anna_Student")
	asm.Add(segment.Segment{Timestamp: 100, TotalSegments: 1, SegmentNumber: 1, DocumentStructureSegment: "1. Введение"}, now)
	asm.Add(segment.Segment{Timestamp: 200, TotalSegments: 2, SegmentNumber: 1, DocumentStructureSegment: "partial"}, now.Add(-time.Minute))
	asm.Add(segment.Segment{Timestamp: 300, TotalSegments: 2, SegmentNumber: 1, DocumentStructureSegment: "waiting"}, now)

	if n := c.DeliverReady(context.Background()); n != 2 {
		t.Fatalf("delivered %d messages, want 2", n)
	}

	got := app.received()
	sort.Slice(got, func(i, j int) bool { return got[i].Timestamp < got[j].Timestamp })
	want := []clients.ReceiveRequest{
		{Sender: "Ivan_Student", Timestamp: 100, Error: false, Payload: strPtr("1. Введение")},
		{Sender: "Anna_Student", Timestamp: 200, Error: true, Payload: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if asm.Pending() != 1 {
		t.Fatalf("pending = %d, want 1", asm.Pending())
	}
}

func TestRunDeliversAndStops(t *testing.T) {
	asm := assembler.New(time.Minute)
	app := &fakeApp{}
	c := New(asm, senders.NewRegistry(), app, 10*time.Millisecond, discard)
	asm.Add(segment.Segment{Timestamp: 100, TotalSegments: 1, SegmentNumber: 1, DocumentStructureSegment: "hi"}, time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for len(app.received()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("message was not delivered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
package assembler

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dima040805/transport-level/internal/segment"
)

func seg(ts int64, number, total int, payload string) segment.Segment {
	return segment.Segment{
		Timestamp:                ts,
		SegmentNumber:            number,
		TotalSegments:            total,
		DocumentStructureSegment: payload,
	}
}

func TestCollectCompleteMessageOutOfOrder(t *testing.T) {
	a := New(time.Minute)
	now := time.Now()

	a.Add(seg(1001, 3, 3, "c"), now)
	a.Add(seg(1001, 1, 3, "a"), now)
	if got := a.Collect(now); len(got) != 0 {
		t.Fatalf("collected an incomplete message: %+v", got)
	}

	a.Add(seg(1001, 2, 3, "b"), now)
	got := a.Collect(now)
	want := []Message{{Timestamp: 1001, Payload: "abc", Received: 3, Total: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if a.Pending() != 0 {
		t.Fatalf("message was not removed from the buffer")
	}
}

func TestCollectExpiredMessageIsIncomplete(t *testing.T) {
	a := New(6 * time.Second)
	start := time.Now()
	a.Add(seg(1001, 1, 3, "a"), start)
	a.Add(seg(1001, 3, 3, "c"), start)

	if got := a.Collect(start.Add(5 * time.Second)); len(got) != 0 {
		t.Fatalf("collected before timeout: %+v", got)
	}

	got := a.Collect(start.Add(6 * time.Second))
	want := []Message{{Timestamp: 1001, Payload: "ac", Received: 2, Total: 3, Incomplete: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestNewSegmentExtendsTimeout(t *testing.T) {
	a := New(6 * time.Second)
	start := time.Now()
	a.Add(seg(1001, 1, 2, "a"), start)
	a.Add(seg(1001, 1, 2, "a"), start.Add(5*time.Second))

	if got := a.Collect(start.Add(8 * time.Second)); len(got) != 0 {
		t.Fatalf("timeout was not extended by the late segment: %+v", got)
	}
}

func TestDuplicateSegmentDoesNotCompleteMessage(t *testing.T) {
	a := New(time.Minute)
	now := time.Now()
	a.Add(seg(1001, 1, 2, "a"), now)
	a.Add(seg(1001, 1, 2, "a"), now)

	if got := a.Collect(now); len(got) != 0 {
		t.Fatalf("duplicate completed the message: %+v", got)
	}
}

func TestConcurrentAdd(t *testing.T) {
	a := New(time.Minute)
	now := time.Now()
	const total = 100

	var wg sync.WaitGroup
	for i := 1; i <= total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.Add(seg(1001, i, total, "x"), now)
		}()
	}
	wg.Wait()

	got := a.Collect(now)
	if len(got) != 1 || len(got[0].Payload) != total || got[0].Incomplete {
		t.Fatalf("got %+v", got)
	}
}
package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dima040805/transport-level/internal/clients"
	"github.com/dima040805/transport-level/internal/senders"
)

type fakeAgent struct {
	resp   clients.GenerateResponse
	err    error
	called chan clients.GenerateRequest
}

func newFakeAgent(resp clients.GenerateResponse, err error) *fakeAgent {
	return &fakeAgent{
		resp:   resp,
		err:    err,
		called: make(chan clients.GenerateRequest, 1),
	}
}

func (f *fakeAgent) Generate(_ context.Context, req clients.GenerateRequest) (clients.GenerateResponse, error) {
	f.called <- req
	return f.resp, f.err
}

type fakePublisher struct {
	mu        sync.Mutex
	published []string
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, string(value))
	return nil
}

func newServer(agent Agent, pub Publisher, lossPercent, roll int) (http.Handler, *senders.Registry) {
	registry := senders.NewRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(agent, pub, registry, lossPercent, logger, WithRand(func(int) int { return roll }))
	return s.Handler(), registry
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

const validSegment = `{"timestamp":1715600000,"total_segments":2,"segment_number":1,"document_structure_segment":"1. Введение"}`

func TestTransferIsPublished(t *testing.T) {
	pub := &fakePublisher{}
	h, _ := newServer(newFakeAgent(clients.GenerateResponse{}, nil), pub, 6, 50)

	rec := do(h, http.MethodPost, "/api/v1/transfer", validSegment)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"success":true`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
	if len(pub.published) != 1 || pub.published[0] != validSegment {
		t.Fatalf("published %q", pub.published)
	}
}

func TestTransferLossIsSimulated(t *testing.T) {
	pub := &fakePublisher{}
	h, _ := newServer(newFakeAgent(clients.GenerateResponse{}, nil), pub, 6, 3)

	rec := do(h, http.MethodPost, "/api/v1/transfer", validSegment)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"success":true`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
	if len(pub.published) != 0 {
		t.Fatal("lost segment reached Kafka")
	}
}

func TestTransferRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"not json":           "segment",
		"missing timestamp":  `{"total_segments":1,"segment_number":1,"document_structure_segment":"a"}`,
		"zero segment":       `{"timestamp":100,"total_segments":2,"segment_number":0,"document_structure_segment":"a"}`,
		"number above total": `{"timestamp":100,"total_segments":2,"segment_number":5,"document_structure_segment":"a"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			pub := &fakePublisher{}
			h, _ := newServer(newFakeAgent(clients.GenerateResponse{}, nil), pub, 0, 0)
			if rec := do(h, http.MethodPost, "/api/v1/transfer", body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
			if len(pub.published) != 0 {
				t.Fatal("invalid segment reached Kafka")
			}
		})
	}
}

func TestTransferPublishFailure(t *testing.T) {
	h, _ := newServer(newFakeAgent(clients.GenerateResponse{}, nil), &fakePublisher{err: errors.New("broker down")}, 0, 0)
	if rec := do(h, http.MethodPost, "/api/v1/transfer", validSegment); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestMethodAndPreflight(t *testing.T) {
	h, _ := newServer(newFakeAgent(clients.GenerateResponse{}, nil), &fakePublisher{}, 0, 0)

	if rec := do(h, http.MethodGet, "/api/v1/transfer", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/v1/transfer status = %d, want 405", rec.Code)
	}
	rec := do(h, http.MethodOptions, "/api/v1/send", "")
	if rec.Code != http.StatusOK || rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("preflight: %d %v", rec.Code, rec.Header())
	}
}

func TestSendForwardsTopicAndRemembersSender(t *testing.T) {
	agent := newFakeAgent(clients.GenerateResponse{Success: true, Message: "done"}, nil)
	h, registry := newServer(agent, &fakePublisher{}, 0, 0)

	body := `{"sender":"Ivan_Student","timestamp":1715600000,"document_topic":"Распределенные системы"}`
	rec := do(h, http.MethodPost, "/api/v1/send", body)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"success":true`) {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}

	select {
	case got := <-agent.called:
		want := clients.GenerateRequest{
			Sender:        "Ivan_Student",
			Timestamp:     1715600000,
			DocumentTopic: "Распределенные системы",
		}
		if got != want {
			t.Fatalf("agent got %+v, want %+v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("agent.Generate was not called")
	}

	if got := registry.Take(1715600000); got != "Ivan_Student" {
		t.Fatalf("sender = %q, want Ivan_Student", got)
	}
}

func TestSendRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"not json":          "bad",
		"empty sender":      `{"sender":"","timestamp":1715600000,"document_topic":"Kafka"}`,
		"missing timestamp": `{"sender":"Ivan","document_topic":"Kafka"}`,
		"empty topic":       `{"sender":"Ivan","timestamp":1715600000,"document_topic":""}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			h, _ := newServer(newFakeAgent(clients.GenerateResponse{}, nil), &fakePublisher{}, 0, 0)
			if rec := do(h, http.MethodPost, "/api/v1/send", body); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}
}
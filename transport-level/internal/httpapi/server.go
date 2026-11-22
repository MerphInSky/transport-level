// Package httpapi exposes the /api/v1/send and /api/v1/transfer endpoints of the transport level.
package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/dima040805/transport-level/internal/clients"
	"github.com/dima040805/transport-level/internal/segment"
	"github.com/dima040805/transport-level/internal/senders"
)

const (
	maxBodyBytes   = 10 << 20
	publishTimeout = 5 * time.Second
)

// Agent forwards document generation requests to the agent level.
type Agent interface {
	Generate(ctx context.Context, req clients.GenerateRequest) (clients.GenerateResponse, error)
}

// Publisher puts a raw segment into the message broker.
type Publisher interface {
	Publish(ctx context.Context, value []byte) error
}

type Server struct {
	agent       Agent
	publisher   Publisher
	senders     *senders.Registry
	lossPercent int
	randIntn    func(n int) int
	logger      *slog.Logger
}

type Option func(*Server)

// WithRand replaces the random source used to simulate segment loss.
func WithRand(intn func(n int) int) Option {
	return func(s *Server) { s.randIntn = intn }
}

func New(agent Agent, publisher Publisher, registry *senders.Registry, lossPercent int, logger *slog.Logger, opts ...Option) *Server {
	s := &Server{
		agent:       agent,
		publisher:   publisher,
		senders:     registry,
		lossPercent: lossPercent,
		randIntn:    rand.IntN,
		logger:      logger,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/send", s.handleSend)
	mux.HandleFunc("POST /send", s.handleSend)
	mux.HandleFunc("POST /api/v1/transfer", s.handleTransfer)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, operationResponse{Success: true, Message: "ok"})
	})
	return withCORS(mux)
}

type sendRequest struct {
	Sender        string `json:"sender"`
	Timestamp     int64  `json:"timestamp"`
	DocumentTopic string `json:"document_topic"`
}

type operationResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, operationResponse{Success: false, Message: "Failed to read body"})
		return
	}
	var req sendRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, operationResponse{Success: false, Message: "Invalid JSON"})
		return
	}
	if req.Sender == "" || req.Timestamp <= 0 || req.DocumentTopic == "" {
		writeJSON(w, http.StatusBadRequest, operationResponse{
			Success: false,
			Message: "sender, positive timestamp and document_topic are required",
		})
		return
	}

	s.senders.Remember(req.Timestamp, req.Sender)
	s.logger.Info("send: forwarding topic to agent",
		"sender", req.Sender,
		"timestamp", req.Timestamp,
		"topic", req.DocumentTopic,
	)

	// Agent executes LLM generation synchronously and posts segments to /api/v1/transfer
	// before returning 200 OK, so we dispatch the call asynchronously.
	go func(genReq clients.GenerateRequest) {
		resp, err := s.agent.Generate(context.Background(), genReq)
		if err != nil {
			s.logger.Error("send: agent generation failed",
				"timestamp", genReq.Timestamp,
				"sender", genReq.Sender,
				"err", err,
			)
			return
		}
		s.logger.Info("send: agent finished",
			"timestamp", genReq.Timestamp,
			"success", resp.Success,
			"message", resp.Message,
		)
	}(clients.GenerateRequest{
		Sender:        req.Sender,
		Timestamp:     req.Timestamp,
		DocumentTopic: req.DocumentTopic,
	})

	writeJSON(w, http.StatusOK, operationResponse{
		Success: true,
		Message: "Сообщение успешно принято и передано агенту",
	})
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, operationResponse{Success: false, Message: "Failed to read body"})
		return
	}
	var seg segment.Segment
	if err := json.Unmarshal(body, &seg); err != nil {
		writeJSON(w, http.StatusBadRequest, operationResponse{Success: false, Message: "Invalid JSON"})
		return
	}
	if err := seg.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, operationResponse{Success: false, Message: err.Error()})
		return
	}

	if s.randIntn(100) < s.lossPercent {
		s.logger.Info("transfer: simulated loss",
			"timestamp", seg.Timestamp,
			"segment", seg.SegmentNumber,
			"total", seg.TotalSegments,
		)
		writeJSON(w, http.StatusOK, operationResponse{
			Success: true,
			Message: "Сегмент отброшен (имитация потери сети)",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), publishTimeout)
	defer cancel()
	if err := s.publisher.Publish(ctx, body); err != nil {
		s.logger.Error("transfer: publish to kafka", "timestamp", seg.Timestamp, "err", err)
		writeJSON(w, http.StatusInternalServerError, operationResponse{Success: false, Message: "Failed to send to Kafka"})
		return
	}
	writeJSON(w, http.StatusOK, operationResponse{
		Success: true,
		Message: "Сегмент успешно помещен в очередь",
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
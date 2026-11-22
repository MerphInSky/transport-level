// Package clients talks to the neighbouring levels over HTTP.
package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// GenerateRequest is sent to the agent level (POST /api/v1/generate).
type GenerateRequest struct {
	Sender        string `json:"sender"`
	Timestamp     int64  `json:"timestamp"`
	DocumentTopic string `json:"document_topic"`
}

// GenerateResponse is the agent's OperationResponse.
type GenerateResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// ReceiveRequest delivers a reassembled document structure to the application level (POST /api/receive).
// Payload is a pointer so it serializes as null when Error is true.
type ReceiveRequest struct {
	Sender    string  `json:"sender"`
	Timestamp int64   `json:"timestamp"`
	Error     bool    `json:"error"`
	Payload   *string `json:"payload"`
}

// StatusError is returned when a neighbour answers with an unexpected HTTP status.
type StatusError struct {
	Code int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected status %d", e.Code)
}

// Agent calls POST /api/v1/generate on the agent level.
type Agent struct {
	url    string
	client *http.Client
}

func NewAgent(url string, client *http.Client) *Agent {
	return &Agent{url: url, client: client}
}

// Generate forwards a document topic to the agent level.
// Accepts both 200 OK and 202 Accepted.
func (a *Agent) Generate(ctx context.Context, req GenerateRequest) (GenerateResponse, error) {
	body, err := postJSON(ctx, a.client, a.url, req, http.StatusOK, http.StatusAccepted)
	if err != nil {
		return GenerateResponse{}, err
	}
	var resp GenerateResponse
	if json.Unmarshal(body, &resp) != nil {
		return GenerateResponse{}, nil
	}
	return resp, nil
}

// App calls POST /api/receive on the application level.
type App struct {
	url    string
	client *http.Client
}

func NewApp(url string, client *http.Client) *App {
	return &App{url: url, client: client}
}

func (a *App) Deliver(ctx context.Context, req ReceiveRequest) error {
	_, err := postJSON(ctx, a.client, a.url, req, http.StatusOK)
	return err
}

func postJSON(ctx context.Context, client *http.Client, url string, v any, okStatuses ...int) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	for _, code := range okStatuses {
		if resp.StatusCode == code {
			return body, nil
		}
	}
	return body, &StatusError{Code: resp.StatusCode}
}
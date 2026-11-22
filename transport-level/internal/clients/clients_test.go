package clients

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestAgentGenerateOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GenerateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
			req.Sender != "Ivan" || req.Timestamp != 1715600000 || req.DocumentTopic != "Kafka" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"success":true,"message":"done"}`))
	}))
	defer srv.Close()

	resp, err := NewAgent(srv.URL, srv.Client()).Generate(context.Background(), GenerateRequest{
		Sender:        "Ivan",
		Timestamp:     1715600000,
		DocumentTopic: "Kafka",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if !resp.Success || resp.Message != "done" {
		t.Fatalf("got %+v", resp)
	}
}

func TestAgentGenerateAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"success":true,"message":"accepted"}`))
	}))
	defer srv.Close()

	resp, err := NewAgent(srv.URL, srv.Client()).Generate(context.Background(), GenerateRequest{
		Sender:        "Ivan",
		Timestamp:     1715600000,
		DocumentTopic: "Kafka",
	})
	if err != nil {
		t.Fatalf("Generate (202): %v", err)
	}
	if !resp.Success || resp.Message != "accepted" {
		t.Fatalf("got %+v", resp)
	}
}

func TestAgentGenerateNonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := NewAgent(srv.URL, srv.Client()).Generate(context.Background(), GenerateRequest{DocumentTopic: "Kafka"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want StatusError 503", err)
	}
}

func TestAgentGenerateIgnoresNonJSONReply(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("accepted"))
	}))
	defer srv.Close()

	resp, err := NewAgent(srv.URL, srv.Client()).Generate(context.Background(), GenerateRequest{DocumentTopic: "Kafka"})
	if err != nil || resp != (GenerateResponse{}) {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
}

func TestAppDeliver(t *testing.T) {
	var got ReceiveRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
	}))
	defer srv.Close()

	payload := "1. Введение"
	want := ReceiveRequest{Sender: "Ivan_Student", Timestamp: 1715600000, Error: false, Payload: &payload}
	if err := NewApp(srv.URL, srv.Client()).Deliver(context.Background(), want); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("app received %+v, want %+v", got, want)
	}
}

func TestAppDeliverErrorNullPayload(t *testing.T) {
	var raw map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&raw)
	}))
	defer srv.Close()

	req := ReceiveRequest{Sender: "Ivan_Student", Timestamp: 1715600000, Error: true, Payload: nil}
	if err := NewApp(srv.URL, srv.Client()).Deliver(context.Background(), req); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if raw["error"] != true || raw["payload"] != nil {
		t.Fatalf("unexpected json body: %+v", raw)
	}
}
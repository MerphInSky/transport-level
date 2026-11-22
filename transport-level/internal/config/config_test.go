package config

import (
	"testing"
	"time"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":8080" ||
		cfg.KafkaTopic != "segments" ||
		cfg.AgentURL != "http://127.0.0.1:8000/api/v1/generate" ||
		cfg.AppURL != "http://127.0.0.1:8081/api/receive" ||
		cfg.SegmentLossPercent != 6 ||
		cfg.BufferCheckInterval != 2*time.Second ||
		cfg.SegmentTimeout != 6*time.Second ||
		cfg.HTTPClientTimeout != 150*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestOverrides(t *testing.T) {
	cfg, err := load(env(map[string]string{
		"KAFKA_BROKER":          "kafka:29092",
		"SEGMENT_LOSS_PERCENT":  "9",
		"BUFFER_CHECK_INTERVAL": "500ms",
		"SEGMENT_TIMEOUT":       "3s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaBroker != "kafka:29092" || cfg.SegmentLossPercent != 9 ||
		cfg.BufferCheckInterval != 500*time.Millisecond || cfg.SegmentTimeout != 3*time.Second {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestInvalidValues(t *testing.T) {
	cases := []map[string]string{
		{"SEGMENT_LOSS_PERCENT": "150"},
		{"SEGMENT_LOSS_PERCENT": "many"},
		{"BUFFER_CHECK_INTERVAL": "soon"},
		{"SEGMENT_TIMEOUT": "-1s"},
	}
	for _, values := range cases {
		if _, err := load(env(values)); err == nil {
			t.Errorf("expected an error for %v", values)
		}
	}
}
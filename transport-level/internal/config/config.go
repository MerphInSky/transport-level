// Package config reads the transport level settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddr          string
	KafkaBroker         string
	KafkaTopic          string
	KafkaGroupID        string
	AgentURL            string
	AppURL              string
	SegmentLossPercent  int
	BufferCheckInterval time.Duration
	SegmentTimeout      time.Duration
	HTTPClientTimeout   time.Duration
}

// Load reads the configuration from environment variables, using defaults for unset ones.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	str := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}

	cfg := Config{
		ListenAddr:   str("LISTEN_ADDR", ":8080"),
		KafkaBroker:  str("KAFKA_BROKER", "localhost:9092"),
		KafkaTopic:   str("KAFKA_TOPIC", "segments"),
		KafkaGroupID: str("KAFKA_GROUP_ID", "transport-consumer-group"),
		AgentURL:     str("AGENT_URL", "http://127.0.0.1:8000/api/v1/generate"),
		AppURL:       str("APP_URL", "http://127.0.0.1:8081/api/receive"),
	}

	var err error
	if cfg.SegmentLossPercent, err = intValue(getenv, "SEGMENT_LOSS_PERCENT", 6); err != nil {
		return Config{}, err
	}
	if cfg.SegmentLossPercent < 0 || cfg.SegmentLossPercent > 100 {
		return Config{}, fmt.Errorf("SEGMENT_LOSS_PERCENT must be within [0, 100], got %d", cfg.SegmentLossPercent)
	}
	if cfg.BufferCheckInterval, err = durationValue(getenv, "BUFFER_CHECK_INTERVAL", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.SegmentTimeout, err = durationValue(getenv, "SEGMENT_TIMEOUT", 6*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.HTTPClientTimeout, err = durationValue(getenv, "HTTP_CLIENT_TIMEOUT", 150*time.Second); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func intValue(getenv func(string) string, key string, def int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func durationValue(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	raw := getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", key, raw)
	}
	return v, nil
}
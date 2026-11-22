// Package kafkaio wraps the Kafka writer and reader used by the transport level.
package kafkaio

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/dima040805/transport-level/internal/segment"
)

// Publisher writes raw segments to a topic through one long-lived writer.
type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(broker, topic string) *Publisher {
	return &Publisher{writer: &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}}
}

func (p *Publisher) Publish(ctx context.Context, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Value: value})
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Consume reads segments in a consumer group until ctx is cancelled
// and passes every valid segment to handle.
func Consume(ctx context.Context, broker, topic, groupID string, logger *slog.Logger, handle func(segment.Segment)) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	logger.Info("kafka consumer started", "topic", topic, "group", groupID)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("kafka consumer stopped")
				return
			}
			logger.Error("read from kafka", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		var s segment.Segment
		if err := json.Unmarshal(msg.Value, &s); err != nil {
			logger.Warn("skip malformed segment", "err", err)
			continue
		}
		if err := s.Validate(); err != nil {
			logger.Warn("skip invalid segment", "err", err)
			continue
		}
		handle(s)
	}
}

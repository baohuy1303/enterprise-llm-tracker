package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"enterprise-llm-tracker/internal/store"
)

type Producer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

func NewProducer(brokers []string, topic string, logger *slog.Logger) *Producer {
	if logger == nil {
		logger = slog.Default()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // partition by key (engineer email) → per-engineer order
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		BatchTimeout: 50 * time.Millisecond,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				logger.Error("kafka publish failed",
					slog.Int("batch_size", len(messages)),
					slog.String("err", err.Error()))
			}
		},
	}
	return &Producer{writer: w, logger: logger}
}

// Publish marshals the event and enqueues it for async delivery. Fire-and-forget:
// errors from the broker arrive via the Completion callback, not the return value.
// Never blocks the caller on broker availability.
func (p *Producer) Publish(ctx context.Context, e store.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.EngineerID),
		Value: body,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

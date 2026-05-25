package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"enterprise-llm-tracker/internal/store"
)

// Handler processes one event. Returning nil commits the offset; returning an
// error leaves the offset uncommitted so Kafka redelivers on the next poll.
type Handler func(ctx context.Context, e store.Event) error

type Consumer struct {
	reader *kafka.Reader
	name   string
	logger *slog.Logger
}

func NewConsumer(brokers []string, topic, groupID string, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})
	return &Consumer{reader: r, name: groupID, logger: logger}
}

// Run blocks until ctx is cancelled. For each message: unmarshal, call handler,
// commit on success. Handler errors are logged and the message is left
// uncommitted so it will be redelivered.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	c.logger.Info("consumer started", slog.String("group", c.name))
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				c.logger.Info("consumer shutting down", slog.String("group", c.name))
				return nil
			}
			c.logger.Error("consumer fetch failed",
				slog.String("group", c.name), slog.String("err", err.Error()))
			time.Sleep(1 * time.Second)
			continue
		}

		var ev store.Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			// Bad payload — commit anyway so we don't loop forever on a poison message.
			c.logger.Error("consumer unmarshal failed",
				slog.String("group", c.name),
				slog.String("err", err.Error()),
				slog.Int("offset", int(msg.Offset)))
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := handler(ctx, ev); err != nil {
			c.logger.Error("consumer handler failed",
				slog.String("group", c.name),
				slog.String("engineer", ev.EngineerID),
				slog.String("metric", ev.MetricName),
				slog.String("err", err.Error()))
			// Do not commit — Kafka will redeliver.
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("consumer commit failed",
				slog.String("group", c.name), slog.String("err", err.Error()))
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

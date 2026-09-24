package storage

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

var TopicCallsLabeled = "calls.labeled"

type Consumer struct {
	reader     *kafka.Reader
	logger     *slog.Logger
	repository Repository
}

func NewConsumer(brokers []string, groupID string, logger *slog.Logger, repo Repository) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   TopicCallsLabeled,
		GroupID: groupID,
	})
	return &Consumer{
		reader:     reader,
		logger:     logger,
		repository: repo,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		c.process(ctx, msg)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit offset", "error", err)
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) {
	var event LabeledEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("failed to unmarshal event", "error", err)
		return
	}
	if err := c.repository.Save(ctx, event); err != nil {
		c.logger.Error("failet to save labeled event to DB", "error", err)
		return
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

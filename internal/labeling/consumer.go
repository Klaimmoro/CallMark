package labeling

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TopicCallsRaw = "calls.raw"

type Consumer struct {
	reader      *kafka.Reader
	rulesClient RulesClient
	publisher   Publisher
	logger      *slog.Logger

	maxAttempts  int
	initialDelay time.Duration
}

func NewConsumer(brokers []string, groupID string, rulesClient RulesClient, publisher Publisher, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   TopicCallsRaw,
		GroupID: groupID,
	})
	return &Consumer{
		reader:      reader,
		rulesClient: rulesClient,
		publisher:   publisher,
		logger:      logger,

		maxAttempts:  3,
		initialDelay: time.Second,
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
	var raw RawCall
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		c.logger.Error("failet to unmarshal event, sending to DLQ", "error", err)
		c.publisher.PublishDLQ(ctx, msg.Value, "publish labeled failed"+err.Error())
		return
	}
	result, err := c.checkWithRetry(ctx, raw.CallerINN)
	if err != nil {
		c.logger.Error("rules check failed after retries", "call_id", raw.CallID, "error", err)
		c.sendToDLQ(ctx, msg.Value, "rules check failed: "+err.Error())
		return
	}
	label, reason := Label(result)
	event := LabeledEvent{
		CallID:      raw.CallID,
		CallerINN:   raw.CallerINN,
		Label:       label,
		Reason:      reason,
		ProcessedAt: time.Now().UTC(),
	}
	if err := c.publisher.PublishLabeled(ctx, event); err != nil {
		c.logger.Error("failet to publish labeled event", "call_id", raw.CallID, "error", err)
		c.sendToDLQ(ctx, msg.Value, "publish labeled failed: "+err.Error())
		return
	}
	c.logger.Info("call labeled", "call_id", raw.CallID, "label", label)
}

func (c *Consumer) checkWithRetry(ctx context.Context, inn string) (RuleResult, error) {
	var lastErr error
	delay := c.initialDelay

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		result, err := c.rulesClient.CheckEntity(ctx, inn)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if !isRetryable(err) {
			return RuleResult{}, err
		}

		if attempt < c.maxAttempts {
			current_delay := time.Duration(rand.N(int64(delay)))
			select {
			case <-time.After(current_delay):
			case <-ctx.Done():
				return RuleResult{}, ctx.Err()
			}
			delay *= 2
		}
	}

	return RuleResult{}, lastErr
}

func isRetryable(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

func (c *Consumer) sendToDLQ(ctx context.Context, raw []byte, reason string) {
	if err := c.publisher.PublishDLQ(ctx, raw, reason); err != nil {
		c.logger.Error("failed to publsh to DLQ - event lost", "error", err)
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

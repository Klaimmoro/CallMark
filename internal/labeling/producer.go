package labeling

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

const (
	TopicCallsLabeled = "calls.labeled"
	TopicCallsDLQ     = "calls.dlq"
)

type Publisher interface {
	PublishLabeled(ctx context.Context, event LabeledEvent) error
	PublishDLQ(ctx context.Context, raw []byte, reason string) error
}

type Producer struct {
	labeledWriter *kafka.Writer
	dlqWriter     *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{
		labeledWriter: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicCallsLabeled,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		},
		dlqWriter: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicCallsDLQ,
			Balancer:               &kafka.RoundRobin{},
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Producer) PublishLabeled(ctx context.Context, event LabeledEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.labeledWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.CallerINN),
		Value: payload,
	})
}

type dlqEnvelope struct {
	Reason  string          `json:"reason"`
	RawJSON json.RawMessage `json:"raw_event"`
}

func (p *Producer) PublishDLQ(ctx context.Context, raw []byte, reason string) error {
	envelope := dlqEnvelope{
		Reason:  reason,
		RawJSON: raw,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return p.dlqWriter.WriteMessages(ctx, kafka.Message{
		Value: payload,
	})
}

func (p *Producer) Close() error {
	if err := p.labeledWriter.Close(); err != nil {
		return err
	}
	return p.dlqWriter.Close()
}

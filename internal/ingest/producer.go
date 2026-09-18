package ingest

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

// `CallsRawTopic` - name of topic, where raw calls events are published
const CallsRawTopic = "calls.raw"

// Producer, that post calls events to Kafka
type Producer struct {
	writer *kafka.Writer
}

// Constructor for `Producer`
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  CallsRawTopic,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Public message to kafka topic `CallsRawTopic`
func (p *Producer) Publish(ctx context.Context, event CallEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(event.CallerINN),
			Value: payload,
		},
	)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

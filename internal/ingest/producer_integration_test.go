//go:build integration

// Запускается отдельно от обычных unit-тестов, потому что требует Docker:
//
//	go test -tags=integration ./internal/ingest/... -run TestProducer_Integration -v
package ingest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Тестирование настоящего пути от микросервиса до Kafka
func TestProducer_Integration(t *testing.T) {
	ctx := context.Background()

	brokers := startKafkaContainer(t, ctx)

	producer := NewProducer(brokers)
	defer producer.Close()

	event := CallEvent{
		CallID:       "integration-call-1",
		CallerINN:    "7707083893",
		CalleeNumber: "+79261234567",
		CallTime:     time.Now().UTC(),
		DurationSec:  30,
		ReceivedAt:   time.Now().UTC(),
	}

	if err := producer.Publish(ctx, event); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	got := readOneMessage(t, ctx, brokers, CallsRawTopic)

	var gotEvent CallEvent
	if err := json.Unmarshal(got.Value, &gotEvent); err != nil {
		t.Fatalf("failed to unmarshal message from Kafka: %v", err)
	}
	if gotEvent.CallID != event.CallID {
		t.Errorf("expected call_id %q, got %q", event.CallID, gotEvent.CallID)
	}
	if string(got.Key) != event.CallerINN {
		t.Errorf("expected message key to be caller_inn %q, got %q", event.CallerINN, string(got.Key))
	}
}

// Тестирование на соответствие партиций к ИНН
// ИНН одного юр лица должны кластся в одну и туже партицию Kafka
func TestProducer_Integration_SameKeyGoesToSamePartition(t *testing.T) {
	ctx := context.Background()

	brokers := startKafkaContainer(t, ctx)

	producer := NewProducer(brokers)
	defer producer.Close()

	inn := "7707083893"
	for i := 0; i < 2; i++ {
		event := CallEvent{
			CallID:      "call-" + string(rune('a'+i)),
			CallerINN:   inn,
			CallTime:    time.Now().UTC(),
			DurationSec: 10,
			ReceivedAt:  time.Now().UTC(),
		}
		if err := producer.Publish(ctx, event); err != nil {
			t.Fatalf("Publish failed on message %d: %v", i, err)
		}
	}

	msg1 := readOneMessage(t, ctx, brokers, CallsRawTopic)
	msg2 := readOneMessage(t, ctx, brokers, CallsRawTopic)

	if msg1.Partition != msg2.Partition {
		t.Errorf("expected both events for the same caller_inn to land in the same partition, got %d and %d",
			msg1.Partition, msg2.Partition)
	}
}

// Подъём одноброкерного Kafka через официальный
// testcontainers-go модуль и возвращает адреса брокеров, доступные с хоста.
func startKafkaContainer(t *testing.T, ctx context.Context) []string {
	t.Helper()

	kafkaContainer, err := tckafka.Run(
		ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("callmark-test-cluster"),
	)
	t.Cleanup(
		func() {
			if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
				t.Logf("failed to terminate Kafka container: %v", err)
			}
		},
	)
	if err != nil {
		t.Fatalf("failed to start Kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get broker addresses: %v", err)
	}

	ensureTopic(t, ctx, brokers[0], CallsRawTopic)

	return brokers
}

// Явное создание топика
func ensureTopic(t *testing.T, ctx context.Context, broker, topic string) {
	t.Helper()

	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("failed to dial Kafka broker to create topic: %v", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("failed to create topic %q: %v", topic, err)
	}
}

// readOneMessage читает ровно одно сообщение из топика — простой способ
// проверить, что Producer реально записал данные, а не просто не вернул ошибку.
func readOneMessage(t *testing.T, ctx context.Context, brokers []string, topic string) kafkago.Message {
	t.Helper()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "producer-integration-test",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("failed to read message from Kafka: %v", err)
	}
	return msg
}

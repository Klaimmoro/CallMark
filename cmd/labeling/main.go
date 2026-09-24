package main

import (
	"callmark/internal/labeling"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/lpernett/godotenv"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := godotenv.Load(); err != nil {
		logger.Warn("`env` config will be set to default")
	}
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	rulesAddr := envOrDefault("RULES_ADDR", "localhost:50051")
	groupID := envOrDefault("KAFKA_GROUP_ID", "labeling-service")

	rulesClient, err := labeling.NewGRPCRulesClient(rulesAddr)
	if err != nil {
		logger.Error("failed to create rules clinet", "addr", rulesAddr, "error", err)
		os.Exit(1)
	}
	defer rulesClient.Close()

	producer := labeling.NewProducer([]string{kafkaBrokers})
	defer producer.Close()

	consumer := labeling.NewConsumer([]string{kafkaBrokers}, groupID, rulesClient, producer, logger)
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		logger.Info("labeling service starting", "kafka_brokers", kafkaBrokers, "rules_addr", rulesAddr)
		runErr <- consumer.Run(ctx)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stop:
		logger.Info("shutting down")
		cancel()
		<-runErr
	case err := <-runErr:
		if err != nil {
			logger.Error("consumer loo exited unexpectedly", "error", err)
			os.Exit(1)
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

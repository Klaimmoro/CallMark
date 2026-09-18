package main

import (
	"callmark/internal/ingest"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lpernett/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := godotenv.Load(); err != nil {
		logger.Warn("`env` config will be set to default")
	}
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	dedup := ingest.NewDeduplicator(rdb, time.Hour*24)

	producer := ingest.NewProducer([]string{kafkaBrokers})
	defer producer.Close()

	handler := ingest.NewHandler(dedup, producer, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /calls", handler.PostCall)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go func() {
		logger.Info("ingest service starting", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}

}

// Return value by key in `env` file or return default
func envOrDefault(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}

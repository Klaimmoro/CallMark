package main

import (
	"callmark/internal/storage"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lpernett/godotenv"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := godotenv.Load(); err != nil {
		logger.Warn("`env` config will be set to default")
	}
	kafkaBrokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	groupID := envOrDefault("KAFKA_GROUP_ID", "storage-service")
	httpAddr := envOrDefault("HTTP_ADDR", ":8081")
	postgresDSN := envOrDefault("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/storage")

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, postgresDSN)
	if err != nil {
		logger.Error("failed to create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}

	repo := storage.NewPostgresRepository(pool)

	consumer := storage.NewConsumer([]string{kafkaBrokers}, groupID, logger, repo)
	defer consumer.Close()

	handler := storage.NewHandler(repo, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{id}", handler.GetCall)
	mux.HandleFunc("GET /stats", handler.GetStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: httpAddr, Handler: mux}

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()

	httpErr := make(chan error, 1)
	go func() {
		logger.Info("storage HTTP starting", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			httpErr <- err
			return
		}
		httpErr <- nil
	}()

	consumerErr := make(chan error, 1)
	go func() {
		logger.Info("storage consumer starting", "kafka_brokers", kafkaBrokers)
		consumerErr <- consumer.Run(consumerCtx)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var reason string
	select {
	case <-stop:
		reason = "signal"
	case err := <-httpErr:
		reason = "http"
		if err != nil {
			logger.Error("http server failed", "error", err)
		}
	case err := <-consumerErr:
		reason = "consumer"
		if err != nil {
			logger.Error("http server failed", "error", err)
		}
	}

	logger.Info("shutting down", "reason", reason)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http graceful shutdown failed", "error", err)
	}
	cancelConsumer()

	if reason != "http" {
		<-httpErr
	}
	if reason != "consumer" {
		<-consumerErr
	}

}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

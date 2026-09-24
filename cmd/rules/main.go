package main

import (
	"callmark/internal/rules"
	pb "callmark/proto/rules"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/lpernett/godotenv"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := godotenv.Load(); err != nil {
		logger.Warn("`env` config will be set to default")
	}
	grpcAddr := envOrDefault("GRPC_ADDR", ":50051")

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to listen", "addr", grpcAddr, "error", err)
	}

	checker := rules.NewMockChecker()
	server := rules.NewServer(checker)

	grpcServer := grpc.NewServer()
	pb.RegisterRulesServiceServer(grpcServer, server)

	go func() {
		logger.Info("rules service started", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	grpcServer.GracefulStop()

}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

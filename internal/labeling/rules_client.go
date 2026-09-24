package labeling

import (
	"context"
	"time"

	pb "callmark/proto/rules"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RuleResult struct {
	Status   string
	Category string
}

type RulesClient interface {
	CheckEntity(ctx context.Context, inn string) (RuleResult, error)
}

type grpcRulesClient struct {
	conn    *grpc.ClientConn
	client  pb.RulesServiceClient
	timeout time.Duration
}

func NewGRPCRulesClient(addr string) (*grpcRulesClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err

	}
	return &grpcRulesClient{
		conn:    conn,
		client:  pb.NewRulesServiceClient(conn),
		timeout: 3 * time.Second,
	}, nil
}

func (c *grpcRulesClient) CheckEntity(ctx context.Context, inn string) (RuleResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := c.client.CheckEntity(ctx, &pb.CheckEntityRequest{Inn: inn})
	if err != nil {
		return RuleResult{}, err
	}

	return RuleResult{
		Status:   resp.GetStatus().String(),
		Category: resp.GetCategory(),
	}, nil
}

func (c *grpcRulesClient) Close() error {
	return c.conn.Close()
}

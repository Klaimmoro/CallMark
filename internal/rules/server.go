package rules

import (
	pb "callmark/proto/rules"
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedRulesServiceServer
	checker Checker
}

func NewServer(checker Checker) *Server {
	return &Server{
		checker: checker,
	}
}

func (s *Server) CheckEntity(ctx context.Context, req *pb.CheckEntityRequest) (*pb.CheckEntityResponse, error) {
	result, err := s.checker.Check(ctx, req.GetInn())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CheckEntityResponse{
		Status:   toProtoStatus(result.Status),
		Category: result.Category,
	}, nil
}

func toProtoStatus(s string) pb.Status {
	switch s {
	case StatusOK:
		return pb.Status_OK
	case StatusBlacklisted:
		return pb.Status_BLACKLISTED
	default:
		return pb.Status_UNKNOWN
	}
}

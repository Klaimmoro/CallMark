package rules

import (
	"context"
	"errors"
	"testing"

	pb "callmark/proto/rules"
)

type fakeChecker struct {
	result Result
	err    error
}

func (f *fakeChecker) Check(ctx context.Context, inn string) (Result, error) {
	return f.result, f.err
}

func TestServer_CheckEntity_MapsStatusCorrectly(t *testing.T) {
	tests := []struct {
		name       string
		result     Result
		wantStatus pb.Status
	}{
		{"OK maps to Status_OK", Result{Status: StatusOK}, pb.Status_OK},
		{"BLACKLISTED maps to Status_BLACKLISTED", Result{Status: StatusBlacklisted}, pb.Status_BLACKLISTED},
		{"unknown status defaults to Status_UNKNOWN", Result{Status: "GARBAGE"}, pb.Status_UNKNOWN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := &fakeChecker{result: tt.result}
			server := NewServer(checker)

			resp, err := server.CheckEntity(context.Background(), &pb.CheckEntityRequest{Inn: "123"})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.GetStatus() != tt.wantStatus {
				t.Errorf("got status %v, want %v", resp.GetStatus(), tt.wantStatus)
			}
		})
	}
}

func TestServer_CheckEntity_CheckerErrorBecomesGRPCError(t *testing.T) {
	checker := &fakeChecker{err: errors.New("db is down")}
	server := NewServer(checker)

	_, err := server.CheckEntity(context.Background(), &pb.CheckEntityRequest{Inn: "123"})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

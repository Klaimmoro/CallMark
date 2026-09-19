package rules

import "context"

type Result struct {
	Status   string
	Category string
}

const (
	StatusOK          = "OK"
	StatusBlacklisted = "BLACKLISTED"
	StatusUnknown     = "UNKNOWN"
)

type Checker interface {
	Check(ctx context.Context, inn string) (Result, error)
}

type MockChecker struct {
	blacklist map[string]bool
}

func NewMockChecker() *MockChecker {
	return &MockChecker{
		blacklist: map[string]bool{
			"0000000000": true,
			"1111111111": true,
		},
	}
}

func (c *MockChecker) Check(ctx context.Context, inn string) (Result, error) {
	if c.blacklist[inn] {
		return Result{
				Status:   StatusBlacklisted,
				Category: "known__spam",
			},
			nil
	}
	return Result{Status: StatusOK, Category: "unverified"}, nil
}

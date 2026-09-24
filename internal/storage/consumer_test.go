package storage

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"
)

func newTestConsumer(repo Repository) *Consumer {
	return &Consumer{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		repository: repo,
	}
}

func labeledEventJSON(t *testing.T, callID string) []byte {
	t.Helper()
	b, err := json.Marshal(LabeledEvent{
		CallID:      callID,
		CallerINN:   "7707083893",
		Label:       "LEGIT",
		Reason:      "caller verified",
		ProcessedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failet to matshal test LabeledEvent: %v", err)
	}
	return b
}

func TestProcess_ValidEvent_SavesToRepository(t *testing.T) {
}

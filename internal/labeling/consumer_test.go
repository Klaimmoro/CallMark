package labeling

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRulesClient struct {
	calls   int
	results []RuleResult
	errs    []error
}

func (f *fakeRulesClient) CheckEntity(ctx context.Context, inn string) (RuleResult, error) {
	i := f.calls
	f.calls++
	if i < len(f.errs) && f.errs[i] != nil {
		return RuleResult{}, f.errs[i]
	}
	if i < len(f.results) {
		return f.results[i], nil
	}
	return RuleResult{}, errors.New("fakeRulesClient: no more scripted responses")
}

type fakePublisher struct {
	labeled []LabeledEvent
	dlq     []string
}

func (f *fakePublisher) PublishLabeled(ctx context.Context, event LabeledEvent) error {
	f.labeled = append(f.labeled, event)
	return nil
}

func (f *fakePublisher) PublishDLQ(ctx context.Context, raw []byte, reason string) error {
	f.dlq = append(f.dlq, reason)
	return nil
}

func newTestConsumer(rc RulesClient, publish Publisher) *Consumer {
	return &Consumer{
		rulesClient:  rc,
		publisher:    publish,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxAttempts:  3,
		initialDelay: time.Millisecond,
	}
}

func rawCallJSON(t *testing.T, callid, inn string) []byte {
	t.Helper()
	b, err := json.Marshal(RawCall{
		CallID:       callid,
		CallerINN:    inn,
		CalleeNumber: "+79182021719",
		CallTime:     time.Now().UTC(),
		DurationSec:  10,
		ReceivedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to marshal test RawCall: %v", err)
	}
	return b
}

func TestProcess_HappyPath_PublishesLabeledEvent(t *testing.T) {
	rc := &fakeRulesClient{results: []RuleResult{{Status: "OK", Category: "verified"}}}
	pub := &fakePublisher{}
	c := newTestConsumer(rc, pub)

	msg := kafka.Message{Value: rawCallJSON(t, "call-1", "7707083893")}
	c.process(context.Background(), msg)

	if len(pub.labeled) != 1 {
		t.Fatalf("expected 1 labeled event, got %d", len(pub.labeled))
	}

	if pub.labeled[0].Label != LabelLegit {
		t.Errorf("expected label %q, got %q", LabelLegit, pub.labeled[0].Label)
	}
	if len(pub.dlq) != 0 {
		t.Errorf("expected no DLQ events on happy path, got %d", len(pub.dlq))
	}
	if rc.calls != 1 {
		t.Errorf("expected exactly 1 call to RulesClient, got %d", rc.calls)
	}
}

func TestProcess_InvalidJSON_GoesStraightToDLQ_NoRulesCall(t *testing.T) {
	rc := &fakeRulesClient{results: []RuleResult{{Status: "OK", Category: "verified"}}}
	pub := &fakePublisher{}
	c := newTestConsumer(rc, pub)
	msg := kafka.Message{Value: []byte("{not valid json")}
	c.process(context.Background(), msg)

	if len(pub.dlq) != 1 {
		t.Fatalf("expected 1 DLQ event, got %d", len(pub.dlq))
	}

	if len(pub.labeled) != 0 {
		t.Errorf("expected no labeled events, got %d", len(pub.labeled))
	}

	if rc.calls != 0 {
		t.Errorf("expected 0 calls to RulesClient for invalid JSON, got %d", rc.calls)
	}
}

func TestProcess_NonRetryableError_GoesToDLQ_AfterExactlyOneAttempt(t *testing.T) {
	rc := &fakeRulesClient{
		errs: []error{status.Error(codes.InvalidArgument, "bad inn format")},
	}
	pub := &fakePublisher{}
	c := newTestConsumer(rc, pub)

	msg := kafka.Message{Value: rawCallJSON(t, "call-1", "bad-inn")}
	c.process(context.Background(), msg)
	if len(pub.dlq) != 1 {
		t.Fatalf("expected 1 DLQ event, got %d", len(pub.dlq))
	}
	if rc.calls != 1 {
		t.Errorf("expected exactly 1 attempt for a non-retryable error, got %d", rc.calls)
	}
}

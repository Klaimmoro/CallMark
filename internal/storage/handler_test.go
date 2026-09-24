package storage

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeRepo struct {
	records map[string]CallRecord
	stats   []StatsBucket
	saved   []LabeledEvent
	err     error
}

func (f *fakeRepo) Save(ctx context.Context, event LabeledEvent) error {
	if f.err != nil {
		return f.err
	}
	f.saved = append(f.saved, event)
	return nil
}

func (f *fakeRepo) GetByID(ctx context.Context, callID string) (CallRecord, error) {
	if f.err != nil {
		return CallRecord{}, f.err
	}
	rec, ok := f.records[callID]
	if !ok {
		return CallRecord{}, ErrNotFound
	}
	return rec, nil
}

func (f *fakeRepo) GetStats(ctx context.Context, from, to time.Time) ([]StatsBucket, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.stats, nil
}

func newTestHandler(repo Repository) *Handler {
	return NewHandler(repo, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func TestGetCall_Found_Returns200(t *testing.T) {
	repo := &fakeRepo{records: map[string]CallRecord{
		"call-1": {CallID: "call-1", Label: "LEGIT"},
	}}
	h := newTestHandler(repo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{id}", h.GetCall)

	req := httptest.NewRequest(http.MethodGet, "/calls/call-1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var got CallRecord
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if got.Label != "LEGIT" {
		t.Errorf("expected label LEGIT, got %q", got.Label)
	}
}

func TestGetCall_NotFound_Returns404(t *testing.T) {
	repo := &fakeRepo{records: map[string]CallRecord{}}
	h := newTestHandler(repo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{id}", h.GetCall)

	req := httptest.NewRequest(http.MethodGet, "/calls/nope", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGetCall_RepoError_Returns500(t *testing.T) {
	repo := &fakeRepo{err: context.DeadlineExceeded}
	h := newTestHandler(repo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /calls/{id}", h.GetCall)

	req := httptest.NewRequest(http.MethodGet, "/calls/nope", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestGetStats_DefaultRange_Returns200(t *testing.T) {
	repo := &fakeRepo{stats: []StatsBucket{{Label: "LEGIT", Count: 5}}}
	h := newTestHandler(repo)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", h.GetStats)

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()
	h.GetStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body=%s", rec.Code, rec.Body.String())
	}

	var got []StatsBucket
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if len(got) != 1 || got[0].Count != 5 {
		t.Errorf("unexpected stats: %v", got)
	}

}

func TestGetStats_InvalidForm_Returns400(t *testing.T) {
	repo := &fakeRepo{stats: []StatsBucket{{Label: "LEGIT", Count: 5}}}
	h := newTestHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/stats?from=not-a-date", nil)
	rec := httptest.NewRecorder()
	h.GetStats(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetStats_ToBeforeFrom_Returns400(t *testing.T) {
	repo := &fakeRepo{}
	h := newTestHandler(repo)

	req := httptest.NewRequest(http.MethodGet, "/stats?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z", nil)
	rec := httptest.NewRecorder()
	h.GetStats(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Handler struct {
	repository Repository
	logger     *slog.Logger
}

func NewHandler(repo Repository, logger *slog.Logger) *Handler {
	return &Handler{repository: repo, logger: logger}
}

// GetCall - GET /calls/{id}
func (h *Handler) GetCall(w http.ResponseWriter, r *http.Request) {
	callID := r.PathValue("id")
	if callID == "" {
		respondError(w, http.StatusBadRequest, "call ID is required")
		return
	}

	record, err := h.repository.GetByID(r.Context(), callID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			respondError(w, http.StatusNotFound, "call not found")
			return
		}
		h.logger.Error("failed to get call", "call_ID", callID, "error", err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, record)
}

func respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// GetStats - GET /stats?from=RFC3339&to=RFC3339
func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	from, to, err := parseStatsRange(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	buckets, err := h.repository.GetStats(r.Context(), from, to)
	if err != nil {
		h.logger.Error("failed to get stats", "error", err)
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, buckets)
}

func parseStatsRange(r *http.Request) (from, to time.Time, err error) {
	q := r.URL.Query()

	now := time.Now().UTC()
	from, to = now.Add(-24*time.Hour), now

	if v := q.Get("from"); v != "" {
		from, err = time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid 'from' parameter: %w", err)
		}
	}

	if v := q.Get("to"); v != "" {
		to, err = time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid 'to' parameter: %w", err)
		}
	}

	if !to.After(from) {
		return time.Time{}, time.Time{}, fmt.Errorf("'to' must be after 'from': %w", err)
	}

	return from, to, nil

}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}

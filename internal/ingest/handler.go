package ingest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// HTTP requests handler
type Handler struct {
	dedup    *Deduplicator
	producer *Producer
	logger   *slog.Logger
}

// Constructor for `Handler`
func NewHandler(dedup *Deduplicator, producer *Producer, logger *slog.Logger) *Handler {
	return &Handler{dedup: dedup, producer: producer, logger: logger}
}

// PostCall - response to POST /calls: validate and post to Kafka
func (h *Handler) PostCall(w http.ResponseWriter, r *http.Request) {
	var req CallRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	if err := Validate(req); err != nil {
		respondError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	ctx := r.Context()

	seen, err := h.dedup.SeenBefore(ctx, req.CallID)
	if err != nil {
		h.logger.Error("dedup check failed", "call_id", req.CallID, "error", err)
		respondError(w, http.StatusInternalServerError, "internal server")
		return
	}
	if seen {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	event := CallEvent{
		CallID:       req.CallID,
		CallerINN:    req.CallerINN,
		CalleeNumber: req.CalleeNumber,
		CallTime:     req.CallTime,
		DurationSec:  req.DurationSec,
		ReceivedAt:   time.Now().UTC(),
	}
	if err := h.producer.Publish(ctx, event); err != nil {
		h.logger.Error("kafka publish failed", "call_id", req.CallID, "error", err)
		respondError(w, http.StatusServiceUnavailable, "failed to accept event")
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func respondError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

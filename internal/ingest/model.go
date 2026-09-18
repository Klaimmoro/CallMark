package ingest

import "time"

// CallRequest — request from external system (POST /calls).
type CallRequest struct {
	CallID       string    `json:"call_id"`
	CallerINN    string    `json:"caller_inn"`
	CalleeNumber string    `json:"callee_number"`
	CallTime     time.Time `json:"call_time"`
	DurationSec  int       `json:"duration_sec"`
}

// CallEvent — event, that posts to Kafka
type CallEvent struct {
	CallID       string    `json:"call_id"`
	CallerINN    string    `json:"caller_inn"`
	CalleeNumber string    `json:"callee_number"`
	CallTime     time.Time `json:"call_time"`
	DurationSec  int       `json:"duration_sec"`
	ReceivedAt   time.Time `json:"received_at"`
}

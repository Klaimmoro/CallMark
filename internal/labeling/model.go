package labeling

import "time"

type LabeledEvent struct {
	CallID      string    `json:"call_id"`
	CallerINN   string    `json:"caller_inn"`
	Label       string    `json:"label"`
	Reason      string    `json:"reason"`
	ProcessedAt time.Time `json:"processed_at"`
}

type RawCall struct {
	CallID       string    `json:"call_id"`
	CallerINN    string    `json:"caller_inn"`
	CalleeNumber string    `json:"callee_number"`
	CallTime     time.Time `json:"call_time"`
	DurationSec  int       `json:"duration_sec"`
	ReceivedAt   time.Time `json:"received_at"`
}

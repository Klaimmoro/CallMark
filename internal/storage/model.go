package storage

import "time"

// LabeledEvent - то, что Storage Service читает из топика Kafka: calls.labeled
type LabeledEvent struct {
	CallID      string    `json:"call_id"`
	CallerINN   string    `json:"caller_inn"`
	Label       string    `json:"label"`
	Reason      string    `json:"reason"`
	ProcessedAt time.Time `json:"processed_at"`
}

// CallRecord - то, что хранится в PostgreSQL и отдаётся через HTTP API
type CallRecord struct {
	CallID      string    `json:"call_id"`
	CallerINN   string    `json:"caller_inn"`
	Label       string    `json:"label"`
	Reason      string    `json:"reason"`
	ProcessedAt time.Time `json:"processed_at"`
	CreatedAt   time.Time `json:"created_at"`
}

// StatcBucket - request-структура для GET /stats: сколько звонков получило конкретную метку за период
type StatsBucket struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

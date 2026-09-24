CREATE TABLE IF NOT EXISTS calls (
    call_id TEXT PRIMARY KEY,
    caller_inn VARCHAR(12) NOT NULL,
    label TEXT NOT NULL,
    reason TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_calls_caller_inn_processed_at
    ON calls (caller_inn, processed_at);

CREATE INDEX IF NOT EXISTS idx_calls_processed_at
    ON calls (processed_at);
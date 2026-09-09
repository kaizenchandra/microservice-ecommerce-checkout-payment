CREATE TABLE processed_saga_event (
    event_id UUID PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE projection_generation
(
    id            BIGSERIAL PRIMARY KEY,
    state         VARCHAR(20) NOT NULL CHECK (state IN ('BUILDING', 'ACTIVE', 'RETIRED', 'FAILED')),
    last_sequence BIGINT      NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    last_error    VARCHAR(150)
);
INSERT INTO projection_generation(state)
VALUES ('ACTIVE');
CREATE TABLE projection_control
(
    singleton           BOOLEAN PRIMARY KEY CHECK (singleton),
    active_generation   BIGINT NOT NULL REFERENCES projection_generation (id),
    building_generation BIGINT REFERENCES projection_generation (id)
);
INSERT INTO projection_control
VALUES (true, 1, NULL);
CREATE TABLE projection_event
(
    sequence          BIGSERIAL PRIMARY KEY,
    event_id          UUID        NOT NULL UNIQUE,
    order_id          UUID        NOT NULL,
    aggregate_type    VARCHAR(50) NOT NULL,
    aggregate_version BIGINT      NOT NULL CHECK (aggregate_version > 0),
    content_hash      VARCHAR(64) NOT NULL,
    envelope          JSONB       NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_type, order_id, aggregate_version)
);
CREATE INDEX projection_event_order_idx ON projection_event (order_id, sequence);
CREATE TABLE projection_inbox
(
    event_id       UUID PRIMARY KEY,
    content_hash   VARCHAR(64) NOT NULL,
    event_sequence BIGINT      NOT NULL REFERENCES projection_event (sequence)
);
CREATE TABLE order_projection
(
    generation    BIGINT      NOT NULL REFERENCES projection_generation (id),
    order_id      UUID        NOT NULL,
    customer_id   UUID        NOT NULL,
    status        VARCHAR(20) NOT NULL CHECK (status IN ('PENDING', 'COMPENSATING', 'COMPLETED', 'CANCELLED')),
    order_version BIGINT      NOT NULL CHECK (order_version > 0),
    created_at    TIMESTAMPTZ NOT NULL,
    view          JSONB       NOT NULL,
    PRIMARY KEY (generation, order_id)
);
CREATE INDEX order_projection_customer_idx ON order_projection (generation, customer_id, created_at DESC, order_id);
CREATE INDEX order_projection_status_idx ON order_projection (generation, status, created_at DESC, order_id);
CREATE FUNCTION protect_projection_history() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN RAISE
EXCEPTION 'Projection input history is append-only';
END;
$$;
CREATE TRIGGER projection_history_immutable
    BEFORE UPDATE OR
DELETE
OR TRUNCATE ON projection_event
    FOR EACH STATEMENT EXECUTE FUNCTION protect_projection_history();
CREATE TRIGGER projection_inbox_immutable
    BEFORE UPDATE OR
DELETE
OR TRUNCATE ON projection_inbox
    FOR EACH STATEMENT EXECUTE FUNCTION protect_projection_history();

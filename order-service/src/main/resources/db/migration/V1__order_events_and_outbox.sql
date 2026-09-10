-- Technical stream cursor only. There is no mutable order status/price row.
CREATE TABLE order_stream
(
    aggregate_id    UUID PRIMARY KEY,
    current_version BIGINT NOT NULL CHECK (current_version >= 0)
);
CREATE TABLE domain_event
(
    event_id          UUID PRIMARY KEY,
    aggregate_id      UUID         NOT NULL REFERENCES order_stream (aggregate_id),
    aggregate_type    VARCHAR(100) NOT NULL CHECK (aggregate_type = 'Order'),
    aggregate_version BIGINT       NOT NULL CHECK (aggregate_version > 0),
    event_type        VARCHAR(150) NOT NULL,
    schema_version    INTEGER      NOT NULL CHECK (schema_version > 0),
    payload           JSONB        NOT NULL,
    occurred_at       TIMESTAMPTZ  NOT NULL,
    UNIQUE (aggregate_id, aggregate_version)
);
CREATE TABLE order_command
(
    customer_id     UUID        NOT NULL,
    idempotency_key UUID        NOT NULL,
    request_hash    VARCHAR(64) NOT NULL,
    order_id        UUID        NOT NULL UNIQUE,
    response        JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, idempotency_key)
);
CREATE TABLE outbox_event
(
    id                UUID PRIMARY KEY REFERENCES domain_event (event_id),
    aggregate_type    VARCHAR(100) NOT NULL CHECK (aggregate_type = 'Order'),
    aggregate_id      UUID         NOT NULL,
    aggregate_version BIGINT       NOT NULL,
    event_type        VARCHAR(150) NOT NULL,
    topic             VARCHAR(100) NOT NULL CHECK (topic = 'order.events'),
    payload           JSONB        NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL,
    published_at      TIMESTAMPTZ,
    status            VARCHAR(30)  NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED')),
    attempts          INTEGER      NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_error        VARCHAR(150),
    UNIQUE (aggregate_id, aggregate_version),
    CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL))
);
CREATE INDEX outbox_pending_due_idx ON outbox_event (next_attempt_at, created_at) WHERE status = 'PENDING';
CREATE INDEX outbox_pending_stream_idx ON outbox_event (aggregate_id, aggregate_version) WHERE status = 'PENDING';

CREATE FUNCTION reject_history_change() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE
EXCEPTION 'Append-only history cannot be changed';
END;
$$;
CREATE TRIGGER domain_event_append_only
    BEFORE UPDATE OR
DELETE
OR TRUNCATE ON domain_event
    FOR EACH STATEMENT EXECUTE FUNCTION reject_history_change();
CREATE TRIGGER order_command_append_only
    BEFORE UPDATE OR
DELETE
OR TRUNCATE ON order_command
    FOR EACH STATEMENT EXECUTE FUNCTION reject_history_change();

CREATE FUNCTION protect_outbox_identity() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF
ROW(NEW.id, NEW.aggregate_type, NEW.aggregate_id, NEW.aggregate_version, NEW.event_type, NEW.topic, NEW.payload, NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id, OLD.aggregate_type, OLD.aggregate_id, OLD.aggregate_version, OLD.event_type, OLD.topic, OLD.payload, OLD.created_at) THEN
        RAISE EXCEPTION 'Outbox event identity and payload are immutable';
END IF;
RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_identity_immutable
    BEFORE UPDATE
    ON outbox_event
    FOR EACH ROW EXECUTE FUNCTION protect_outbox_identity();

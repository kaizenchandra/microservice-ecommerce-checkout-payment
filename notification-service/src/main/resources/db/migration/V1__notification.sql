CREATE TABLE notification (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (status IN ('COMPLETED', 'CANCELLED')),
    reference VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE processed_event (
    event_id UUID PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES notification(order_id),
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    event_type VARCHAR(100) NOT NULL,
    topic VARCHAR(100) NOT NULL CHECK (topic = 'notification.events'),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error VARCHAR(150),
    UNIQUE (aggregate_id, aggregate_version),
    CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL))
);
CREATE INDEX outbox_pending_due_idx ON outbox_event(next_attempt_at, created_at) WHERE status = 'PENDING';
CREATE INDEX outbox_pending_stream_idx ON outbox_event(aggregate_id, aggregate_version) WHERE status = 'PENDING';

CREATE FUNCTION protect_outbox_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id, NEW.aggregate_id, NEW.aggregate_version, NEW.event_type, NEW.topic, NEW.payload, NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id, OLD.aggregate_id, OLD.aggregate_version, OLD.event_type, OLD.topic, OLD.payload, OLD.created_at) THEN
        RAISE EXCEPTION 'Outbox event identity and payload are immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER outbox_identity_immutable BEFORE UPDATE ON outbox_event
    FOR EACH ROW EXECUTE FUNCTION protect_outbox_identity();

CREATE TABLE payment (
    payment_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    input_event JSONB NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'UNKNOWN', 'COMPLETED', 'FAILED')),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error VARCHAR(150),
    provider_reference UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    CHECK ((status IN ('COMPLETED', 'FAILED')) = (version = 1)),
    CHECK (status <> 'COMPLETED' OR provider_reference IS NOT NULL),
    CHECK (status = 'UNKNOWN' OR lease_token IS NULL)
);
CREATE INDEX payment_recovery_idx ON payment(next_attempt_at, created_at) WHERE status IN ('PENDING', 'UNKNOWN');
CREATE TABLE processed_event (
    event_id UUID PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Independent simulator ledger: deliberately no FK to payment; models provider-owned durability.
CREATE TABLE provider_charge (
    payment_id UUID PRIMARY KEY,
    request_hash VARCHAR(64) NOT NULL,
    outcome VARCHAR(30) CHECK (outcome IN ('COMPLETED', 'FAILED')),
    provider_reference UUID,
    requests INTEGER NOT NULL DEFAULT 0 CHECK (requests >= 0),
    charge_count INTEGER NOT NULL DEFAULT 0 CHECK (charge_count BETWEEN 0 AND 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((outcome IS NOT DISTINCT FROM 'COMPLETED') = (charge_count = 1 AND provider_reference IS NOT NULL))
);
CREATE TABLE outbox_event (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES payment(payment_id),
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    event_type VARCHAR(100) NOT NULL,
    topic VARCHAR(100) NOT NULL CHECK (topic = 'payment.events'),
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

CREATE FUNCTION protect_payment_input() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.payment_id, NEW.customer_id, NEW.request_hash, NEW.input_event, NEW.created_at)
       IS DISTINCT FROM ROW(OLD.payment_id, OLD.customer_id, OLD.request_hash, OLD.input_event, OLD.created_at) THEN
        RAISE EXCEPTION 'Payment identity and input are immutable';
    END IF;
    IF OLD.status IN ('COMPLETED', 'FAILED') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'Terminal charge outcome cannot be changed';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER payment_input_immutable BEFORE UPDATE ON payment
    FOR EACH ROW EXECUTE FUNCTION protect_payment_input();

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

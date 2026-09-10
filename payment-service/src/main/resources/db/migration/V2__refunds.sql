-- Refund intents can arrive before the local charge is visible and are retained until ready.
CREATE TABLE refund
(
    order_id           UUID PRIMARY KEY,
    event_id           UUID        NOT NULL UNIQUE,
    customer_id        UUID        NOT NULL,
    correlation_id     UUID        NOT NULL,
    traceparent        VARCHAR(55),
    status             VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'UNKNOWN', 'REFUNDED')),
    attempts           INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token        UUID,
    lease_until        TIMESTAMPTZ,
    next_attempt_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    provider_reference UUID,
    last_error         VARCHAR(150),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    CHECK ((status = 'REFUNDED') = (provider_reference IS NOT NULL)),
    CHECK (status = 'UNKNOWN' OR lease_token IS NULL)
);
CREATE TABLE provider_refund
(
    payment_id         UUID PRIMARY KEY REFERENCES provider_charge (payment_id),
    provider_reference UUID,
    requests           INTEGER NOT NULL DEFAULT 0 CHECK (requests >= 0),
    refund_count       INTEGER NOT NULL DEFAULT 0 CHECK (refund_count BETWEEN 0 AND 1),
    CHECK ((refund_count = 1) = (provider_reference IS NOT NULL))
);

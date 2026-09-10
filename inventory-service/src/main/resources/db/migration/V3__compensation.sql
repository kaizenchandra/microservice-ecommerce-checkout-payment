CREATE TABLE compensation
(
    order_id       UUID PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    customer_id    UUID        NOT NULL,
    event_type     VARCHAR(30) NOT NULL CHECK (event_type IN ('PaymentFailed', 'PaymentRefunded')),
    correlation_id UUID        NOT NULL,
    traceparent    VARCHAR(55),
    status         VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DONE')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

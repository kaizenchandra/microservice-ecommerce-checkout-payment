CREATE TABLE product
(
    id          UUID PRIMARY KEY,
    sku         VARCHAR(40)    NOT NULL UNIQUE,
    name        VARCHAR(160)   NOT NULL,
    description VARCHAR(2000)  NOT NULL,
    amount      NUMERIC(14, 2) NOT NULL CHECK (amount > 0),
    currency    VARCHAR(3)     NOT NULL CHECK (currency = 'USD'),
    active      BOOLEAN        NOT NULL,
    version     BIGINT         NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at  TIMESTAMPTZ    NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL
);
CREATE INDEX product_active_name_idx ON product (name, id) WHERE active;

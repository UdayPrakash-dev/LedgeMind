-- Sagas: one row per transfer. Unlike events, this row is updated as the transfer
-- moves through its steps. history keeps a log of every step attempt.
CREATE TABLE IF NOT EXISTS sagas (
    saga_id          UUID        PRIMARY KEY,
    idempotency_key  TEXT        NOT NULL UNIQUE,          -- same key = same transfer
    from_wallet      TEXT        NOT NULL,
    to_wallet        TEXT        NOT NULL,
    amount           BIGINT      NOT NULL CHECK (amount > 0), -- paise
    current_step     TEXT        NOT NULL,                 -- DEBIT_SENDER, CALL_RAIL, CREDIT_RECEIVER, REFUND_SENDER, DONE
    status           TEXT        NOT NULL,                 -- RUNNING, COMPLETED, COMPENSATED, FAILED, STUCK
    history          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_wallet <> to_wallet)
);

CREATE INDEX IF NOT EXISTS sagas_status_idx ON sagas (status);

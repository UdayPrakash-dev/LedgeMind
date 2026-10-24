-- Day 5: each saga remembers which recovery policy it ran with (refund, retry, escalate).
ALTER TABLE sagas ADD COLUMN IF NOT EXISTS policy TEXT NOT NULL DEFAULT 'refund';
CREATE INDEX IF NOT EXISTS sagas_created_idx ON sagas (created_at);

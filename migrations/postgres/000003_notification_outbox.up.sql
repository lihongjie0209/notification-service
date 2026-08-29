CREATE TABLE notification_outbox_events (
 id TEXT PRIMARY KEY, subject TEXT NOT NULL, envelope BYTEA NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 available_at TIMESTAMPTZ NOT NULL, published_at TIMESTAMPTZ, last_error TEXT NOT NULL DEFAULT '', version BIGINT NOT NULL DEFAULT 1,
 created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL
);
CREATE INDEX idx_notification_outbox_pending ON notification_outbox_events (available_at,created_at) WHERE published_at IS NULL;

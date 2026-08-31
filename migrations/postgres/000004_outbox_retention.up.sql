CREATE INDEX notification_outbox_retention_idx ON notification_outbox_events (published_at, id) WHERE published_at IS NOT NULL;

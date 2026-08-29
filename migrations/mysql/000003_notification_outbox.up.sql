CREATE TABLE notification_outbox_events (
 id VARCHAR(191) PRIMARY KEY, subject VARCHAR(255) NOT NULL, envelope LONGBLOB NOT NULL, attempts INT NOT NULL DEFAULT 0,
 available_at DATETIME(6) NOT NULL, published_at DATETIME(6), last_error TEXT NOT NULL, version BIGINT NOT NULL DEFAULT 1,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL, created_by TEXT NOT NULL, updated_by TEXT NOT NULL,
 INDEX idx_notification_outbox_pending (published_at,available_at,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

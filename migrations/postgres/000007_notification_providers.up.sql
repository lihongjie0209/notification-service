CREATE TABLE notification_providers (
 id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 application_id TEXT NOT NULL,
 code TEXT NOT NULL,
 channel TEXT NOT NULL,
 upstream TEXT NOT NULL,
 path TEXT NOT NULL,
 priority INTEGER NOT NULL DEFAULT 100,
 status TEXT NOT NULL,
 version BIGINT NOT NULL DEFAULT 1,
 created_at TIMESTAMPTZ NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL,
 created_by TEXT NOT NULL,
 updated_by TEXT NOT NULL,
 CONSTRAINT notification_provider_scope_code_uq UNIQUE(tenant_id,application_id,code),
 CONSTRAINT chk_notification_provider_priority CHECK(priority >= 0),
 CONSTRAINT chk_notification_provider_status CHECK(status IN ('active','disabled'))
);
CREATE INDEX notification_providers_scope_list_idx ON notification_providers(tenant_id,application_id,status,channel,priority,code);

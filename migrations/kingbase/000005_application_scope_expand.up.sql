ALTER TABLE notification_templates ADD COLUMN application_id TEXT NULL;
ALTER TABLE notification_deliveries ADD COLUMN application_id TEXT NULL;
CREATE INDEX notification_templates_application_idx ON notification_templates(tenant_id,application_id,updated_at DESC,id);
CREATE INDEX notification_deliveries_application_idx ON notification_deliveries(tenant_id,application_id,created_at DESC,id);

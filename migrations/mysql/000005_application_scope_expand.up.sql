ALTER TABLE notification_templates ADD COLUMN application_id VARCHAR(191) NULL AFTER tenant_id, ADD INDEX notification_templates_application_idx(tenant_id,application_id,updated_at DESC,id);
ALTER TABLE notification_deliveries ADD COLUMN application_id VARCHAR(191) NULL AFTER tenant_id, ADD INDEX notification_deliveries_application_idx(tenant_id,application_id,created_at DESC,id);

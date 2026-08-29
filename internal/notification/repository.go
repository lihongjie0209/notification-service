package notification

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

var ErrNotFound = errors.New("notification resource not found")
var ErrStaleVersion = errors.New("stale notification version")

type Repository interface {
	GetTemplate(context.Context, string, string, string, string) (Template, error)
	InsertTemplate(context.Context, sqlx.ExtContext, Template) error
	UpdateTemplate(context.Context, sqlx.ExtContext, Template, int64) error
	GetDelivery(context.Context, string, string) (Delivery, error)
	GetDeliveryByKey(context.Context, string, string) (Delivery, error)
	InsertDelivery(context.Context, sqlx.ExtContext, Delivery) error
	ListDeliveries(context.Context, string, string, int, int) ([]Delivery, int64, error)
	ClaimDue(context.Context, *sqlx.Tx, time.Time, int) ([]Delivery, error)
	Finish(context.Context, sqlx.ExtContext, Delivery, int64) error
	AddOutbox(context.Context, sqlx.ExtContext, OutboxEvent) error
}

func (r *SQLRepository) AddOutbox(ctx context.Context, executor sqlx.ExtContext, event OutboxEvent) error {
	_, err := executor.ExecContext(ctx, r.db.Rebind(`INSERT INTO notification_outbox_events (id,subject,envelope,attempts,available_at,published_at,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,?,NULL,'',1,?,?,?,?)`), event.ID, event.Subject, event.Envelope, event.AvailableAt, event.CreatedAt, event.UpdatedAt, event.CreatedBy, event.UpdatedBy)
	return err
}

func (r *SQLRepository) ClaimDue(ctx context.Context, tx *sqlx.Tx, now time.Time, limit int) ([]Delivery, error) {
	var values []Delivery
	query := r.db.Rebind(`SELECT ` + deliveryColumns + ` FROM notification_deliveries WHERE status IN ('pending','retrying') AND next_attempt_at<=? ORDER BY next_attempt_at,id LIMIT ? FOR UPDATE SKIP LOCKED`)
	if err := tx.SelectContext(ctx, &values, query, now, limit); err != nil {
		return nil, err
	}
	for index := range values {
		result, err := tx.ExecContext(ctx, r.db.Rebind(`UPDATE notification_deliveries SET status='processing',attempts=attempts+1,version=version+1,updated_at=? WHERE id=? AND version=?`), now, values[index].ID, values[index].Version)
		if err != nil {
			return nil, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return nil, ErrStaleVersion
		}
		values[index].Status = "processing"
		values[index].Attempts++
		values[index].Version++
		values[index].UpdatedAt = now
	}
	return values, nil
}
func (r *SQLRepository) Finish(ctx context.Context, e sqlx.ExtContext, v Delivery, expected int64) error {
	result, err := e.ExecContext(ctx, r.db.Rebind(`UPDATE notification_deliveries SET status=?,provider=?,provider_message_id=?,failure_reason=?,next_attempt_at=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=?`), v.Status, v.Provider, v.ProviderMessageID, v.FailureReason, v.NextAttemptAt, v.UpdatedAt, v.UpdatedBy, v.ID, expected)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err == nil && changed != 1 {
		return ErrStaleVersion
	}
	return err
}

type SQLRepository struct{ db *sqlx.DB }

func NewRepository(db *sqlx.DB) Repository { return &SQLRepository{db: db} }

const templateColumns = `id,tenant_id,code,channel,locale,subject,content,status,version,created_at,updated_at,created_by,updated_by`
const deliveryColumns = `id,tenant_id,template_code,channel,locale,recipient,variables,idempotency_key,status,provider,provider_message_id,failure_reason,attempts,next_attempt_at,version,created_at,updated_at,created_by,updated_by`

func (r *SQLRepository) GetTemplate(ctx context.Context, tenant, code, channel, locale string) (Template, error) {
	var v Template
	err := r.db.GetContext(ctx, &v, r.db.Rebind(`SELECT `+templateColumns+` FROM notification_templates WHERE tenant_id=? AND code=? AND channel=? AND locale=?`), tenant, code, channel, locale)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return v, err
}
func (r *SQLRepository) InsertTemplate(ctx context.Context, e sqlx.ExtContext, v Template) error {
	_, err := e.ExecContext(ctx, r.db.Rebind(`INSERT INTO notification_templates (`+templateColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`), v.ID, v.TenantID, v.Code, v.Channel, v.Locale, v.Subject, v.Content, v.Status, v.Version, v.CreatedAt, v.UpdatedAt, v.CreatedBy, v.UpdatedBy)
	return err
}
func (r *SQLRepository) UpdateTemplate(ctx context.Context, e sqlx.ExtContext, v Template, expected int64) error {
	result, err := e.ExecContext(ctx, r.db.Rebind(`UPDATE notification_templates SET subject=?,content=?,status=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=?`), v.Subject, v.Content, v.Status, v.UpdatedAt, v.UpdatedBy, v.ID, expected)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return ErrStaleVersion
	}
	return err
}
func (r *SQLRepository) GetDelivery(ctx context.Context, id, tenant string) (Delivery, error) {
	var v Delivery
	err := r.db.GetContext(ctx, &v, r.db.Rebind(`SELECT `+deliveryColumns+` FROM notification_deliveries WHERE id=? AND tenant_id=?`), id, tenant)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return v, err
}
func (r *SQLRepository) GetDeliveryByKey(ctx context.Context, tenant, key string) (Delivery, error) {
	var v Delivery
	err := r.db.GetContext(ctx, &v, r.db.Rebind(`SELECT `+deliveryColumns+` FROM notification_deliveries WHERE tenant_id=? AND idempotency_key=?`), tenant, key)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return v, err
}
func (r *SQLRepository) InsertDelivery(ctx context.Context, e sqlx.ExtContext, v Delivery) error {
	_, err := e.ExecContext(ctx, r.db.Rebind(`INSERT INTO notification_deliveries (`+deliveryColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), v.ID, v.TenantID, v.TemplateCode, v.Channel, v.Locale, v.Recipient, v.Variables, v.IdempotencyKey, v.Status, v.Provider, v.ProviderMessageID, v.FailureReason, v.Attempts, v.NextAttemptAt, v.Version, v.CreatedAt, v.UpdatedAt, v.CreatedBy, v.UpdatedBy)
	return err
}
func (r *SQLRepository) ListDeliveries(ctx context.Context, tenant, status string, limit, offset int) ([]Delivery, int64, error) {
	where := ` WHERE tenant_id=? AND (?='' OR status=?)`
	args := []any{tenant, status, status}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT count(*) FROM notification_deliveries`+where), args...); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, offset)
	var values []Delivery
	err := r.db.SelectContext(ctx, &values, r.db.Rebind(`SELECT `+deliveryColumns+` FROM notification_deliveries`+where+` ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?`), args...)
	return values, total, err
}

package notification

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/notification-service/internal/database"
	"github.com/lihongjie0209/notification-service/internal/principal"
)

type fakeRepo struct {
	template Template
	delivery Delivery
	outbox   []OutboxEvent
}

func (f *fakeRepo) AddOutbox(_ context.Context, _ sqlx.ExtContext, event OutboxEvent) error {
	f.outbox = append(f.outbox, event)
	return nil
}

func (f *fakeRepo) GetTemplate(context.Context, string, string, string, string) (Template, error) {
	if f.template.ID == "" {
		return Template{}, ErrNotFound
	}
	return f.template, nil
}
func (f *fakeRepo) InsertTemplate(_ context.Context, _ sqlx.ExtContext, v Template) error {
	f.template = v
	return nil
}
func (f *fakeRepo) UpdateTemplate(context.Context, sqlx.ExtContext, Template, int64) error {
	return nil
}
func (f *fakeRepo) GetDelivery(context.Context, string, string) (Delivery, error) {
	return f.delivery, nil
}
func (f *fakeRepo) GetDeliveryByKey(context.Context, string, string) (Delivery, error) {
	if f.delivery.ID == "" {
		return Delivery{}, ErrNotFound
	}
	return f.delivery, nil
}
func (f *fakeRepo) InsertDelivery(_ context.Context, _ sqlx.ExtContext, v Delivery) error {
	f.delivery = v
	return nil
}
func (f *fakeRepo) ListDeliveries(context.Context, string, string, int, int) ([]Delivery, int64, error) {
	return []Delivery{f.delivery}, 1, nil
}
func (f *fakeRepo) ClaimDue(context.Context, *sqlx.Tx, time.Time, int) ([]Delivery, error) {
	return nil, nil
}
func (f *fakeRepo) Finish(context.Context, sqlx.ExtContext, Delivery, int64) error { return nil }
func TestSendIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repo := &fakeRepo{template: Template{ID: "tpl-1", Status: "active"}}
	service := NewService(repo, database.NewTransactor(sqlx.NewDb(db, "sqlmock")))
	service.now = func() time.Time { return time.Date(2026, 8, 30, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)) }
	ctx := principal.WithContext(t.Context(), principal.Principal{Subject: "user-1"})
	mock.ExpectBegin()
	mock.ExpectCommit()
	first, err := service.Send(ctx, "tenant-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", map[string]string{"name": "A"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Send(ctx, "tenant-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", map[string]string{"name": "A"})
	if err != nil || second.ID != first.ID {
		t.Fatalf("replay=%+v err=%v", second, err)
	}
	if len(repo.outbox) != 1 || repo.outbox[0].Subject != "platform.notification.requested.v1" {
		t.Fatalf("outbox=%+v", repo.outbox)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestPutTemplateRejectsInvalidSyntax(t *testing.T) {
	service := NewService(&fakeRepo{}, &database.Transactor{})
	ctx := principal.WithContext(t.Context(), principal.Principal{Subject: "admin"})
	if _, err := service.PutTemplate(ctx, Template{TenantID: "t", Code: "welcome", Channel: "email", Locale: "zh-cn", Content: "{{"}, 0); err == nil {
		t.Fatal("invalid template accepted")
	}
}
func TestRenderRejectsMissingVariableAndBackoffIsBounded(t *testing.T) {
	if _, err := render("message", "Hello {{.name}}", map[string]string{}); err == nil {
		t.Fatal("render accepted missing variable")
	}
	if got := retryBackoff(time.Second, 1); got != time.Second {
		t.Fatalf("first backoff=%s", got)
	}
	if got := retryBackoff(time.Second, 100); got != 1024*time.Second {
		t.Fatalf("capped backoff=%s", got)
	}
}

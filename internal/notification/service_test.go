package notification

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/notification-service/internal/apperror"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/database"
	"github.com/lihongjie0209/notification-service/internal/ratelimit"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

type rejectingApplicationVerifier struct{ err error }

func (v rejectingApplicationVerifier) Verify(context.Context, string, string) error { return v.err }

type fakeRepo struct {
	template Template
	delivery Delivery
	outbox   []OutboxEvent
}

func (f *fakeRepo) AddOutbox(_ context.Context, _ sqlx.ExtContext, event OutboxEvent) error {
	f.outbox = append(f.outbox, event)
	return nil
}

func (f *fakeRepo) GetTemplate(context.Context, string, string, string, string, string) (Template, error) {
	if f.template.ID == "" {
		return Template{}, ErrNotFound
	}
	return f.template, nil
}
func (f *fakeRepo) ListTemplates(context.Context, string, string, string, string, string, int, int) ([]Template, int64, error) {
	return []Template{f.template}, 1, nil
}
func (f *fakeRepo) InsertTemplate(_ context.Context, _ sqlx.ExtContext, v Template) error {
	f.template = v
	return nil
}
func (f *fakeRepo) UpdateTemplate(context.Context, sqlx.ExtContext, Template, int64) error {
	return nil
}
func (f *fakeRepo) GetDelivery(context.Context, string, string, string) (Delivery, error) {
	return f.delivery, nil
}
func (f *fakeRepo) GetDeliveryByKey(context.Context, string, string, string) (Delivery, error) {
	if f.delivery.ID == "" {
		return Delivery{}, ErrNotFound
	}
	return f.delivery, nil
}
func (f *fakeRepo) GetDeliveryByProviderMessage(context.Context, string, string, string, string) (Delivery, error) {
	if f.delivery.ID == "" {
		return Delivery{}, ErrNotFound
	}
	return f.delivery, nil
}
func (f *fakeRepo) InsertDelivery(_ context.Context, _ sqlx.ExtContext, v Delivery) error {
	f.delivery = v
	return nil
}
func (f *fakeRepo) ListDeliveries(context.Context, string, string, string, int, int) ([]Delivery, int64, error) {
	return []Delivery{f.delivery}, 1, nil
}
func (f *fakeRepo) ClaimDue(context.Context, *sqlx.Tx, time.Time, int) ([]Delivery, error) {
	return nil, nil
}
func (f *fakeRepo) Finish(_ context.Context, _ sqlx.ExtContext, value Delivery, expected int64) error {
	if f.delivery.Version != expected {
		return ErrStaleVersion
	}
	value.Version = expected + 1
	f.delivery = value
	return nil
}
func (f *fakeRepo) DeleteTerminalDeliveriesBefore(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}
func TestSendIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repo := &fakeRepo{template: Template{ID: "tpl-1", ApplicationID: "app-1", Status: "active"}}
	service := NewService(repo, database.NewTransactor(sqlx.NewDb(db, "sqlmock")), nil, config.Config{})
	service.now = func() time.Time { return time.Date(2026, 8, 30, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)) }
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	mock.ExpectBegin()
	mock.ExpectCommit()
	first, err := service.Send(ctx, "tenant-1", "app-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", map[string]string{"name": "A"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Send(ctx, "tenant-1", "app-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", map[string]string{"name": "A"})
	if err != nil || second.ID != first.ID {
		t.Fatalf("replay=%+v err=%v", second, err)
	}
	if len(repo.outbox) != 1 || repo.outbox[0].Subject != "platform.notification.requested.v1" {
		t.Fatalf("outbox=%+v", repo.outbox)
	}
	envelope := &commonv1.EventEnvelope{}
	if err := proto.Unmarshal(repo.outbox[0].Envelope, envelope); err != nil {
		t.Fatal(err)
	}
	if first.ApplicationID != "app-1" || envelope.GetApplicationId() != "app-1" {
		t.Fatalf("delivery=%+v envelope application=%q", first, envelope.GetApplicationId())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSendRejectsApplicationWithoutTenantGrant(t *testing.T) {
	service, err := NewRuntimeService(&fakeRepo{}, &database.Transactor{}, nil, config.Config{}, rejectingApplicationVerifier{err: appaccess.ErrNotGranted})
	if err != nil {
		t.Fatal(err)
	}
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	_, err = service.Send(ctx, "tenant-1", "app-denied", "welcome", "email", "zh-cn", "a@example.com", "send-1", nil)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeForbidden {
		t.Fatalf("Send() error = %v, want forbidden", err)
	}
}
func TestPutTemplateRejectsInvalidSyntax(t *testing.T) {
	service := NewService(&fakeRepo{}, &database.Transactor{}, nil, config.Config{})
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "admin", Type: platformprincipal.TypeUser, TenantID: "t"})
	if _, err := service.PutTemplate(ctx, Template{TenantID: "t", ApplicationID: "app-1", Code: "welcome", Channel: "email", Locale: "zh-cn", Content: "{{"}, 0); err == nil {
		t.Fatal("invalid template accepted")
	}
}
func TestListTemplatesRejectsTenantOutsideJWTContext(t *testing.T) {
	service := NewService(&fakeRepo{}, &database.Transactor{}, nil, config.Config{})
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "admin", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	_, err := service.ListTemplates(ctx, "tenant-2", "app-1", "", "", "", 1, 20)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeForbidden {
		t.Fatalf("ListTemplates() error = %v, want forbidden", err)
	}
}
func TestSendEnforcesRecipientFrequencyLimit(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cfg := config.Config{RateLimit: config.RateLimit{Enabled: true}, Notification: config.Notification{
		TenantLimit:    config.RateLimitRule{Rate: 100, Burst: 100, Period: time.Hour},
		TemplateLimit:  config.RateLimitRule{Rate: 100, Burst: 100, Period: time.Hour},
		RecipientLimit: config.RateLimitRule{Rate: 1, Burst: 1, Period: time.Hour},
	}}
	repo := &fakeRepo{template: Template{ID: "tpl-1", ApplicationID: "app-1", Status: "active"}}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectCommit()
	service := NewService(repo, database.NewTransactor(sqlx.NewDb(db, "sqlmock")), ratelimit.New(client, cfg), cfg)
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	if _, err := service.Send(ctx, "tenant-1", "app-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", nil); err != nil {
		t.Fatal(err)
	}
	repo.delivery = Delivery{}
	if _, err := service.Send(ctx, "tenant-1", "app-1", "welcome", "email", "zh-cn", "a@example.com", "send-2", nil); err == nil || !strings.Contains(err.Error(), "too many requests") {
		t.Fatalf("second request error=%v", err)
	}
}
func TestRecordReceiptIsTransactionalAndIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	repo := &fakeRepo{delivery: Delivery{ID: "delivery-1", TenantID: "tenant-1", ApplicationID: "app-1", Provider: "email", ProviderMessageID: "provider-1", Status: "sent", Version: 2, CreatedAt: now, UpdatedAt: now}}
	service := NewService(repo, database.NewTransactor(sqlx.NewDb(db, "sqlmock")), nil, config.Config{})
	service.now = func() time.Time { return now }
	ctx := platformprincipal.SystemContext(t.Context(), "provider:email")
	mock.ExpectBegin()
	mock.ExpectCommit()
	delivered, err := service.RecordReceipt(ctx, "tenant-1", "app-1", "email", "provider-1", "delivered", "")
	if err != nil {
		t.Fatal(err)
	}
	if delivered.Status != "delivered" || delivered.Version != 3 || len(repo.outbox) != 1 {
		t.Fatalf("delivered=%+v outbox=%+v", delivered, repo.outbox)
	}
	replayed, err := service.RecordReceipt(ctx, "tenant-1", "app-1", "email", "provider-1", "delivered", "")
	if err != nil || replayed.Version != delivered.Version || len(repo.outbox) != 1 {
		t.Fatalf("replayed=%+v err=%v outbox=%d", replayed, err, len(repo.outbox))
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

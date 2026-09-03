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
	provider Provider
	template Template
	delivery Delivery
	outbox   []OutboxEvent
}

func (f *fakeRepo) GetProvider(context.Context, string, string, string) (Provider, error) {
	if f.provider.ID == "" {
		return Provider{}, ErrNotFound
	}
	return f.provider, nil
}
func (f *fakeRepo) ListProviders(context.Context, string, string, string, string, string, int, int) ([]Provider, int64, error) {
	return []Provider{f.provider}, 1, nil
}
func (f *fakeRepo) InsertProvider(_ context.Context, _ sqlx.ExtContext, value Provider) error {
	f.provider = value
	return nil
}
func (f *fakeRepo) UpdateProvider(_ context.Context, _ sqlx.ExtContext, value Provider, expected int64) error {
	if f.provider.Version != expected {
		return ErrStaleVersion
	}
	value.Version = expected + 1
	f.provider = value
	return nil
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

func TestPutProviderCreatesAndUpdatesWithOptimisticLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := &fakeRepo{}
	service := NewService(repository, database.NewTransactor(sqlx.NewDb(db, "sqlmock")), nil, config.Config{Notification: config.Notification{ProviderUpstreams: []string{"email-primary", "email-secondary"}}, Outbound: config.Outbound{HTTP: map[string]config.HTTPUpstream{"email-primary": {}, "email-secondary": {}}}})
	service.now = func() time.Time { return time.Date(2026, 9, 3, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)) }
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "admin", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	mock.ExpectBegin()
	mock.ExpectCommit()
	created, err := service.PutProvider(ctx, Provider{TenantID: "tenant-1", ApplicationID: "app-1", Code: " primary ", Channel: " EMAIL ", Upstream: "email-primary", Path: "/send", Priority: 10}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if created.Code != "primary" || created.Channel != "email" || created.Status != "active" || created.Version != 1 || created.CreatedBy != "admin" {
		t.Fatalf("created provider=%+v", created)
	}
	mock.ExpectBegin()
	mock.ExpectCommit()
	updated, err := service.PutProvider(ctx, Provider{TenantID: "tenant-1", ApplicationID: "app-1", Code: "primary", Channel: "email", Upstream: "email-secondary", Path: "/v2/send", Priority: 20, Status: "disabled"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Upstream != "email-secondary" || updated.Status != "disabled" {
		t.Fatalf("updated provider=%+v", updated)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPutProviderRejectsUnsafeRouteAndStaleCreate(t *testing.T) {
	service := NewService(&fakeRepo{}, &database.Transactor{}, nil, config.Config{Notification: config.Notification{ProviderUpstreams: []string{"email"}}, Outbound: config.Outbound{HTTP: map[string]config.HTTPUpstream{"email": {}}}})
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "admin", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	for _, value := range []Provider{
		{TenantID: "tenant-1", ApplicationID: "app-1", Code: "primary", Channel: "email", Upstream: "email", Path: "https://evil.example/send"},
		{TenantID: "tenant-1", ApplicationID: "app-1", Code: "primary", Channel: "email", Upstream: "email", Path: "//evil.example/send"},
		{TenantID: "tenant-1", ApplicationID: "app-1", Code: "primary", Channel: "unknown", Upstream: "email", Path: "/send"},
	} {
		if _, err := service.PutProvider(ctx, value, 0); err == nil {
			t.Fatalf("PutProvider(%+v) accepted invalid provider", value)
		}
	}
	if _, err := service.PutProvider(ctx, Provider{TenantID: "tenant-1", ApplicationID: "app-1", Code: "primary", Channel: "email", Upstream: "email", Path: "/send"}, 2); err == nil {
		t.Fatal("PutProvider accepted a nonzero version for create")
	}
	disallowed := NewService(&fakeRepo{}, &database.Transactor{}, nil, config.Config{Outbound: config.Outbound{HTTP: map[string]config.HTTPUpstream{"internal-api": {}}}})
	if _, err := disallowed.PutProvider(ctx, Provider{TenantID: "tenant-1", ApplicationID: "app-1", Code: "primary", Channel: "email", Upstream: "internal-api", Path: "/send"}, 0); err == nil {
		t.Fatal("PutProvider accepted an outbound client outside the provider allowlist")
	}
}

func TestListProvidersBoundsPageSizeAndTenantScope(t *testing.T) {
	service := NewService(&fakeRepo{}, &database.Transactor{}, nil, config.Config{})
	ctx := platformprincipal.WithContext(t.Context(), platformprincipal.Principal{ID: "admin", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	if _, err := service.ListProviders(ctx, "tenant-1", "app-1", "", "", "", 1, 101); err == nil {
		t.Fatal("ListProviders accepted page_size above 100")
	}
	if _, err := service.ListProviders(ctx, "tenant-2", "app-1", "", "", "", 1, 20); err == nil {
		t.Fatal("ListProviders accepted another tenant")
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

package notification

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	"github.com/lihongjie0209/notification-service/internal/apperror"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/database"
	"github.com/lihongjie0209/notification-service/internal/principal"
	"github.com/lihongjie0209/notification-service/internal/ratelimit"
	notificationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/notification/v1"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	repository Repository
	transactor *database.Transactor
	limiter    *ratelimit.Limiter
	config     config.Notification
	now        func() time.Time
}

func NewService(r Repository, t *database.Transactor, limiter *ratelimit.Limiter, cfg config.Config) *Service {
	return &Service{repository: r, transactor: t, limiter: limiter, config: cfg.Notification, now: time.Now}
}
func (s *Service) PutTemplate(ctx context.Context, v Template, expected int64) (Template, error) {
	v.TenantID = strings.TrimSpace(v.TenantID)
	v.Code = strings.ToLower(strings.TrimSpace(v.Code))
	v.Channel = strings.ToLower(strings.TrimSpace(v.Channel))
	v.Locale = strings.ToLower(strings.TrimSpace(v.Locale))
	if v.TenantID == "" || v.Code == "" || !validChannel(v.Channel) || v.Locale == "" || v.Content == "" {
		return Template{}, apperror.Invalid("invalid notification template", nil)
	}
	if _, err := template.New(v.Code).Parse(v.Subject + v.Content); err != nil {
		return Template{}, apperror.Invalid("invalid template syntax", err)
	}
	caller, ok := principal.FromContext(ctx)
	if !ok {
		return Template{}, apperror.Unauthorized("authenticated actor is required")
	}
	now := s.now()
	current, err := s.repository.GetTemplate(ctx, v.TenantID, v.Code, v.Channel, v.Locale)
	if errors.Is(err, ErrNotFound) {
		v.ID = uuid.NewString()
		v.Status = "active"
		v.Version = 1
		v.CreatedAt = now
		v.UpdatedAt = now
		v.CreatedBy = caller.Subject
		v.UpdatedBy = caller.Subject
		err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.InsertTemplate(ctx, tx, v) })
		return v, translate(err)
	}
	if err != nil {
		return Template{}, translate(err)
	}
	current.Subject, current.Content, current.Status, current.UpdatedAt, current.UpdatedBy = v.Subject, v.Content, v.Status, now, caller.Subject
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.UpdateTemplate(ctx, tx, current, expected) })
	current.Version = expected + 1
	return current, translate(err)
}
func (s *Service) Send(ctx context.Context, tenant, code, channel, locale, recipient, key string, variables map[string]string) (Delivery, error) {
	tenant = strings.TrimSpace(tenant)
	code = strings.ToLower(strings.TrimSpace(code))
	channel = strings.ToLower(strings.TrimSpace(channel))
	locale = strings.ToLower(strings.TrimSpace(locale))
	recipient = strings.TrimSpace(recipient)
	key = strings.TrimSpace(key)
	if tenant == "" || code == "" || !validChannel(channel) || locale == "" || recipient == "" || key == "" {
		return Delivery{}, apperror.Invalid("invalid notification request", nil)
	}
	if existing, err := s.repository.GetDeliveryByKey(ctx, tenant, key); err == nil {
		return existing, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Delivery{}, translate(err)
	}
	if err := s.checkFrequency(ctx, tenant, code, channel, recipient); err != nil {
		return Delivery{}, err
	}
	tpl, err := s.repository.GetTemplate(ctx, tenant, code, channel, locale)
	if err != nil {
		return Delivery{}, translate(err)
	}
	if tpl.Status != "active" {
		return Delivery{}, apperror.Conflict("notification template is disabled", nil)
	}
	encoded, err := json.Marshal(variables)
	if err != nil {
		return Delivery{}, apperror.Invalid("invalid variables", err)
	}
	caller, ok := principal.FromContext(ctx)
	if !ok {
		return Delivery{}, apperror.Unauthorized("authenticated actor is required")
	}
	now := s.now()
	v := Delivery{ID: uuid.NewString(), TenantID: tenant, TemplateCode: code, Channel: channel, Locale: locale, Recipient: recipient, Variables: encoded, IdempotencyKey: key, Status: "pending", NextAttemptAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: caller.Subject, UpdatedBy: caller.Subject}
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		if err := s.repository.InsertDelivery(ctx, tx, v); err != nil {
			return err
		}
		return s.addDeliveryEvent(ctx, tx, v, "platform.notification.v1.NotificationRequested", "platform.notification.requested.v1")
	})
	return v, translate(err)
}
func (s *Service) checkFrequency(ctx context.Context, tenant, code, channel, recipient string) error {
	if s.limiter == nil || !s.limiter.Enabled() {
		return nil
	}
	checks := []struct {
		key  string
		rule config.RateLimitRule
	}{
		{"notification:tenant:" + tenant, s.config.TenantLimit},
		{"notification:template:" + tenant + ":" + channel + ":" + code, s.config.TemplateLimit},
		{"notification:recipient:" + tenant + ":" + channel + ":" + recipient, s.config.RecipientLimit},
	}
	for _, check := range checks {
		result, err := s.limiter.Allow(ctx, check.key, check.rule)
		if err != nil {
			if s.limiter.FailOpen() {
				continue
			}
			return apperror.Unavailable("notification frequency limiter unavailable", err)
		}
		if !result.Allowed {
			return apperror.TooManyRequests()
		}
	}
	return nil
}
func (s *Service) GetDelivery(ctx context.Context, id, tenant string) (Delivery, error) {
	if _, ok := principal.FromContext(ctx); !ok {
		return Delivery{}, apperror.Unauthorized("authenticated actor is required")
	}
	v, err := s.repository.GetDelivery(ctx, id, tenant)
	return v, translate(err)
}
func (s *Service) RecordReceipt(ctx context.Context, tenant, provider, messageID, statusValue, reason string) (Delivery, error) {
	tenant, provider, messageID = strings.TrimSpace(tenant), strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(messageID)
	statusValue, reason = strings.ToLower(strings.TrimSpace(statusValue)), strings.TrimSpace(reason)
	if tenant == "" || provider == "" || messageID == "" || !validReceiptStatus(statusValue) {
		return Delivery{}, apperror.Invalid("invalid provider receipt", nil)
	}
	caller, ok := principal.FromContext(ctx)
	if !ok {
		return Delivery{}, apperror.Unauthorized("authenticated actor is required")
	}
	delivery, err := s.repository.GetDeliveryByProviderMessage(ctx, tenant, provider, messageID)
	if err != nil {
		return Delivery{}, translate(err)
	}
	if delivery.Status == statusValue {
		return delivery, nil
	}
	if terminalDeliveryStatus(delivery.Status) {
		return Delivery{}, apperror.Conflict("delivery already has a terminal status", nil)
	}
	now := s.now()
	delivery.Status, delivery.FailureReason = statusValue, reason
	delivery.UpdatedAt, delivery.UpdatedBy = now, caller.Subject
	expected := delivery.Version
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		if err := s.repository.Finish(ctx, tx, delivery, expected); err != nil {
			return err
		}
		delivery.Version = expected + 1
		return s.addDeliveryEvent(ctx, tx, delivery, "platform.notification.v1.NotificationStatusChanged", "platform.notification.status.changed.v1")
	})
	return delivery, translate(err)
}
func (s *Service) ListDeliveries(ctx context.Context, tenant, status string, page, pageSize int) (Page, error) {
	if _, ok := principal.FromContext(ctx); !ok {
		return Page{}, apperror.Unauthorized("authenticated actor is required")
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		return Page{}, apperror.Invalid("page_size must not exceed 100", nil)
	}
	v, total, err := s.repository.ListDeliveries(ctx, tenant, status, pageSize, (page-1)*pageSize)
	return Page{Deliveries: v, Total: total, Page: page, PageSize: pageSize}, translate(err)
}
func (s *Service) addDeliveryEvent(ctx context.Context, tx sqlx.ExtContext, delivery Delivery, eventType, subject string) error {
	payload := &notificationv1.NotificationRequestedEvent{Delivery: toProtoDelivery(delivery)}
	if strings.Contains(eventType, "StatusChanged") {
		payload = nil
	}
	var message proto.Message = payload
	if payload == nil {
		message = &notificationv1.NotificationStatusChangedEvent{Delivery: toProtoDelivery(delivery)}
	}
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{EventID: uuid.NewString(), EventType: eventType, AggregateID: delivery.ID, AggregateType: "notification_delivery", TenantID: delivery.TenantID, SchemaVersion: 1, ActorID: delivery.UpdatedBy, OccurredAt: delivery.UpdatedAt}, message)
	if err != nil {
		return err
	}
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	return s.repository.AddOutbox(ctx, tx, OutboxEvent{ID: envelope.GetEventId(), Subject: subject, Envelope: encoded, AvailableAt: delivery.UpdatedAt, CreatedAt: delivery.UpdatedAt, UpdatedAt: delivery.UpdatedAt, CreatedBy: delivery.UpdatedBy, UpdatedBy: delivery.UpdatedBy})
}
func toProtoDelivery(value Delivery) *notificationv1.Delivery {
	variables := map[string]string{}
	_ = json.Unmarshal(value.Variables, &variables)
	return &notificationv1.Delivery{Id: value.ID, TenantId: value.TenantID, TemplateCode: value.TemplateCode, Channel: value.Channel, Recipient: value.Recipient, Variables: variables, Status: value.Status, Provider: value.Provider, ProviderMessageId: value.ProviderMessageID, FailureReason: value.FailureReason, Attempts: value.Attempts, NextAttemptAt: timestamppb.New(value.NextAttemptAt), Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func validChannel(v string) bool {
	return v == "email" || v == "sms" || v == "webhook" || v == "in_app"
}
func validReceiptStatus(value string) bool {
	return value == "delivered" || value == "bounced" || value == "failed"
}
func terminalDeliveryStatus(value string) bool {
	return value == "delivered" || value == "bounced" || value == "failed" || value == "dead_letter"
}
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return apperror.NotFound("notification resource not found")
	}
	if errors.Is(err, ErrStaleVersion) {
		return apperror.StaleVersion(err)
	}
	return apperror.Internal(err)
}

var Module = fx.Module("notification", fx.Provide(NewRepository, NewService))

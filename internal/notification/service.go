package notification

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/notification-service/internal/apperror"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/database"
	"github.com/lihongjie0209/notification-service/internal/ratelimit"
	notificationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/notification/v1"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	repository   Repository
	transactor   *database.Transactor
	limiter      *ratelimit.Limiter
	config       config.Notification
	outboundHTTP map[string]config.HTTPUpstream
	applications appaccess.Verifier
	now          func() time.Time
}

type allowAllApplications struct{}

func (allowAllApplications) Verify(context.Context, string, string) error { return nil }

func NewService(r Repository, t *database.Transactor, limiter *ratelimit.Limiter, cfg config.Config) *Service {
	service, _ := NewRuntimeService(r, t, limiter, cfg, allowAllApplications{})
	return service
}

func NewRuntimeService(r Repository, t *database.Transactor, limiter *ratelimit.Limiter, cfg config.Config, applications appaccess.Verifier) (*Service, error) {
	if applications == nil {
		return nil, errors.New("application verifier is required")
	}
	return &Service{repository: r, transactor: t, limiter: limiter, config: cfg.Notification, outboundHTTP: cfg.Outbound.HTTP, applications: applications, now: time.Now}, nil
}

func (s *Service) PutProvider(ctx context.Context, value Provider, expected int64) (Provider, error) {
	value.TenantID = strings.TrimSpace(value.TenantID)
	value.ApplicationID = strings.TrimSpace(value.ApplicationID)
	value.Code = strings.ToLower(strings.TrimSpace(value.Code))
	value.Channel = strings.ToLower(strings.TrimSpace(value.Channel))
	value.Upstream = strings.TrimSpace(value.Upstream)
	value.Path = strings.TrimSpace(value.Path)
	value.Status = strings.ToLower(strings.TrimSpace(value.Status))
	if value.Status == "" {
		value.Status = "active"
	}
	if value.TenantID == "" || value.ApplicationID == "" || value.Code == "" || !validChannel(value.Channel) || value.Upstream == "" || !validProviderPath(value.Path) || value.Priority < 0 || (value.Status != "active" && value.Status != "disabled") {
		return Provider{}, apperror.Invalid("invalid notification provider", nil)
	}
	if _, configured := s.outboundHTTP[value.Upstream]; !configured {
		return Provider{}, apperror.Invalid("notification provider upstream is not configured", nil)
	}
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return Provider{}, apperror.Unauthorized("authenticated actor is required")
	}
	if err := enforceTenant(caller, value.TenantID); err != nil {
		return Provider{}, err
	}
	if err := s.verifyApplication(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Provider{}, err
	}
	now := s.now()
	current, err := s.repository.GetProvider(ctx, value.TenantID, value.ApplicationID, value.Code)
	if errors.Is(err, ErrNotFound) {
		if expected != 0 {
			return Provider{}, apperror.Conflict("notification provider version conflict", ErrStaleVersion)
		}
		value.ID = uuid.NewString()
		value.Version = 1
		value.CreatedAt, value.UpdatedAt = now, now
		value.CreatedBy, value.UpdatedBy = caller.ID, caller.ID
		err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.InsertProvider(ctx, tx, value) })
		return value, translate(err)
	}
	if err != nil {
		return Provider{}, translate(err)
	}
	if expected <= 0 || expected != current.Version {
		return Provider{}, apperror.Conflict("notification provider version conflict", ErrStaleVersion)
	}
	current.Channel, current.Upstream, current.Path = value.Channel, value.Upstream, value.Path
	current.Priority, current.Status, current.UpdatedAt, current.UpdatedBy = value.Priority, value.Status, now, caller.ID
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.UpdateProvider(ctx, tx, current, expected) })
	current.Version = expected + 1
	return current, translate(err)
}

func validProviderPath(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && strings.HasPrefix(parsed.Path, "/") && !strings.HasPrefix(parsed.Path, "//") && parsed.Scheme == "" && parsed.Host == "" && parsed.User == nil && parsed.Fragment == ""
}

func (s *Service) ListProviders(ctx context.Context, tenant, application, keyword, channel, status string, page, pageSize int) (ProviderPage, error) {
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return ProviderPage{}, apperror.Unauthorized("authenticated actor is required")
	}
	tenant, application, keyword = strings.TrimSpace(tenant), strings.TrimSpace(application), strings.TrimSpace(keyword)
	channel, status = strings.ToLower(strings.TrimSpace(channel)), strings.ToLower(strings.TrimSpace(status))
	if err := enforceTenant(caller, tenant); err != nil {
		return ProviderPage{}, err
	}
	if err := s.verifyApplication(ctx, tenant, application); err != nil {
		return ProviderPage{}, err
	}
	if channel != "" && !validChannel(channel) {
		return ProviderPage{}, apperror.Invalid("invalid notification channel", nil)
	}
	if status != "" && status != "active" && status != "disabled" {
		return ProviderPage{}, apperror.Invalid("invalid notification provider status", nil)
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		return ProviderPage{}, apperror.Invalid("page_size must not exceed 100", nil)
	}
	values, total, err := s.repository.ListProviders(ctx, tenant, application, keyword, channel, status, pageSize, (page-1)*pageSize)
	return ProviderPage{Providers: values, Total: total, Page: page, PageSize: pageSize}, translate(err)
}

func (s *Service) PutTemplate(ctx context.Context, v Template, expected int64) (Template, error) {
	v.TenantID = strings.TrimSpace(v.TenantID)
	v.ApplicationID = strings.TrimSpace(v.ApplicationID)
	v.Code = strings.ToLower(strings.TrimSpace(v.Code))
	v.Channel = strings.ToLower(strings.TrimSpace(v.Channel))
	v.Locale = strings.ToLower(strings.TrimSpace(v.Locale))
	if v.TenantID == "" || v.ApplicationID == "" || v.Code == "" || !validChannel(v.Channel) || v.Locale == "" || v.Content == "" {
		return Template{}, apperror.Invalid("invalid notification template", nil)
	}
	if _, err := template.New(v.Code).Parse(v.Subject + v.Content); err != nil {
		return Template{}, apperror.Invalid("invalid template syntax", err)
	}
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return Template{}, apperror.Unauthorized("authenticated actor is required")
	}
	if err := enforceTenant(caller, v.TenantID); err != nil {
		return Template{}, err
	}
	if err := s.verifyApplication(ctx, v.TenantID, v.ApplicationID); err != nil {
		return Template{}, err
	}
	now := s.now()
	current, err := s.repository.GetTemplate(ctx, v.TenantID, v.ApplicationID, v.Code, v.Channel, v.Locale)
	if errors.Is(err, ErrNotFound) {
		v.ID = uuid.NewString()
		v.Status = "active"
		v.Version = 1
		v.CreatedAt = now
		v.UpdatedAt = now
		v.CreatedBy = caller.ID
		v.UpdatedBy = caller.ID
		err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.InsertTemplate(ctx, tx, v) })
		return v, translate(err)
	}
	if err != nil {
		return Template{}, translate(err)
	}
	current.Subject, current.Content, current.Status, current.UpdatedAt, current.UpdatedBy = v.Subject, v.Content, v.Status, now, caller.ID
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.UpdateTemplate(ctx, tx, current, expected) })
	current.Version = expected + 1
	return current, translate(err)
}
func (s *Service) Send(ctx context.Context, tenant, application, code, channel, locale, recipient, key string, variables map[string]string) (Delivery, error) {
	tenant = strings.TrimSpace(tenant)
	application = strings.TrimSpace(application)
	code = strings.ToLower(strings.TrimSpace(code))
	channel = strings.ToLower(strings.TrimSpace(channel))
	locale = strings.ToLower(strings.TrimSpace(locale))
	recipient = strings.TrimSpace(recipient)
	key = strings.TrimSpace(key)
	if tenant == "" || application == "" || code == "" || !validChannel(channel) || locale == "" || recipient == "" || key == "" {
		return Delivery{}, apperror.Invalid("invalid notification request", nil)
	}
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return Delivery{}, apperror.Unauthorized("authenticated actor is required")
	}
	if err := enforceTenant(caller, tenant); err != nil {
		return Delivery{}, err
	}
	if err := s.verifyApplication(ctx, tenant, application); err != nil {
		return Delivery{}, err
	}
	if existing, err := s.repository.GetDeliveryByKey(ctx, tenant, application, key); err == nil {
		return existing, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Delivery{}, translate(err)
	}
	if err := s.checkFrequency(ctx, tenant, application, code, channel, recipient); err != nil {
		return Delivery{}, err
	}
	tpl, err := s.repository.GetTemplate(ctx, tenant, application, code, channel, locale)
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
	now := s.now()
	v := Delivery{ID: uuid.NewString(), TenantID: tenant, ApplicationID: application, TemplateCode: code, Channel: channel, Locale: locale, Recipient: recipient, Variables: encoded, IdempotencyKey: key, Status: "pending", NextAttemptAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: caller.ID, UpdatedBy: caller.ID}
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		if err := s.repository.InsertDelivery(ctx, tx, v); err != nil {
			return err
		}
		return s.addDeliveryEvent(ctx, tx, v, "platform.notification.v1.NotificationRequested", "platform.notification.requested.v1")
	})
	return v, translate(err)
}
func (s *Service) checkFrequency(ctx context.Context, tenant, application, code, channel, recipient string) error {
	if s.limiter == nil || !s.limiter.Enabled() {
		return nil
	}
	checks := []struct {
		key  string
		rule config.RateLimitRule
	}{
		{"notification:tenant:" + tenant + ":application:" + application, s.config.TenantLimit},
		{"notification:template:" + tenant + ":" + application + ":" + channel + ":" + code, s.config.TemplateLimit},
		{"notification:recipient:" + tenant + ":" + application + ":" + channel + ":" + recipient, s.config.RecipientLimit},
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
func (s *Service) GetDelivery(ctx context.Context, id, tenant, application string) (Delivery, error) {
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return Delivery{}, apperror.Unauthorized("authenticated actor is required")
	}
	if err := enforceTenant(caller, tenant); err != nil {
		return Delivery{}, err
	}
	if err := s.verifyApplication(ctx, tenant, application); err != nil {
		return Delivery{}, err
	}
	v, err := s.repository.GetDelivery(ctx, id, tenant, application)
	return v, translate(err)
}
func (s *Service) RecordReceipt(ctx context.Context, tenant, application, provider, messageID, statusValue, reason string) (Delivery, error) {
	tenant, provider, messageID = strings.TrimSpace(tenant), strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(messageID)
	application = strings.TrimSpace(application)
	statusValue, reason = strings.ToLower(strings.TrimSpace(statusValue)), strings.TrimSpace(reason)
	if tenant == "" || application == "" || provider == "" || messageID == "" || !validReceiptStatus(statusValue) {
		return Delivery{}, apperror.Invalid("invalid provider receipt", nil)
	}
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return Delivery{}, apperror.Unauthorized("authenticated actor is required")
	}
	if err := enforceTenant(caller, tenant); err != nil {
		return Delivery{}, err
	}
	if err := s.verifyApplication(ctx, tenant, application); err != nil {
		return Delivery{}, err
	}
	delivery, err := s.repository.GetDeliveryByProviderMessage(ctx, tenant, application, provider, messageID)
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
	delivery.UpdatedAt, delivery.UpdatedBy = now, caller.ID
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
func (s *Service) ListDeliveries(ctx context.Context, tenant, application, status string, page, pageSize int) (Page, error) {
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return Page{}, apperror.Unauthorized("authenticated actor is required")
	}
	if err := enforceTenant(caller, tenant); err != nil {
		return Page{}, err
	}
	if err := s.verifyApplication(ctx, tenant, application); err != nil {
		return Page{}, err
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
	v, total, err := s.repository.ListDeliveries(ctx, tenant, application, status, pageSize, (page-1)*pageSize)
	return Page{Deliveries: v, Total: total, Page: page, PageSize: pageSize}, translate(err)
}
func (s *Service) ListTemplates(ctx context.Context, tenant, application, keyword, channel, status string, page, pageSize int) (TemplatePage, error) {
	caller, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return TemplatePage{}, apperror.Unauthorized("authenticated actor is required")
	}
	tenant, keyword = strings.TrimSpace(tenant), strings.TrimSpace(keyword)
	application = strings.TrimSpace(application)
	channel, status = strings.ToLower(strings.TrimSpace(channel)), strings.ToLower(strings.TrimSpace(status))
	if err := enforceTenant(caller, tenant); err != nil {
		return TemplatePage{}, err
	}
	if err := s.verifyApplication(ctx, tenant, application); err != nil {
		return TemplatePage{}, err
	}
	if channel != "" && !validChannel(channel) {
		return TemplatePage{}, apperror.Invalid("invalid notification channel", nil)
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		return TemplatePage{}, apperror.Invalid("page_size must not exceed 100", nil)
	}
	values, total, err := s.repository.ListTemplates(ctx, tenant, application, keyword, channel, status, pageSize, (page-1)*pageSize)
	return TemplatePage{Templates: values, Total: total, Page: page, PageSize: pageSize}, translate(err)
}

func enforceTenant(caller platformprincipal.Principal, requestedTenantID string) error {
	if caller.Type == platformprincipal.TypeUser && (strings.TrimSpace(caller.TenantID) == "" || caller.TenantID != strings.TrimSpace(requestedTenantID)) {
		return apperror.Forbidden("tenant access denied")
	}
	return nil
}

func (s *Service) verifyApplication(ctx context.Context, tenantID, applicationID string) error {
	tenantID, applicationID = strings.TrimSpace(tenantID), strings.TrimSpace(applicationID)
	if tenantID == "" || applicationID == "" {
		return apperror.Invalid("tenant_id and application_id are required", nil)
	}
	if err := s.applications.Verify(ctx, tenantID, applicationID); err != nil {
		if errors.Is(err, appaccess.ErrNotGranted) {
			return apperror.Forbidden("application access denied")
		}
		return apperror.Unavailable("application access verification unavailable", err)
	}
	return nil
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
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{EventID: uuid.NewString(), EventType: eventType, AggregateID: delivery.ID, AggregateType: "notification_delivery", TenantID: delivery.TenantID, ApplicationID: delivery.ApplicationID, SchemaVersion: 1, ActorID: delivery.UpdatedBy, OccurredAt: delivery.UpdatedAt}, message)
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
	return &notificationv1.Delivery{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, TemplateCode: value.TemplateCode, Channel: value.Channel, Recipient: value.Recipient, Variables: variables, Status: value.Status, Provider: value.Provider, ProviderMessageId: value.ProviderMessageID, FailureReason: value.FailureReason, Attempts: value.Attempts, NextAttemptAt: timestamppb.New(value.NextAttemptAt), Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
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

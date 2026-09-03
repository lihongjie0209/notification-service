package httptransport

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lihongjie0209/notification-service/internal/apperror"
	"github.com/lihongjie0209/notification-service/internal/buildinfo"
	"github.com/lihongjie0209/notification-service/internal/health"
	notificationdomain "github.com/lihongjie0209/notification-service/internal/notification"
)

type Handler struct {
	logger *slog.Logger
	health *health.Service

	notifications *notificationdomain.Service
}

func NewHandler(healthService *health.Service, notificationService *notificationdomain.Service, logger *slog.Logger) *Handler {
	return &Handler{health: healthService, notifications: notificationService, logger: logger}
}

type PutTemplateRequest struct {
	TenantID        string `json:"tenant_id" binding:"required"`
	ApplicationID   string `json:"application_id" binding:"required"`
	Code            string `json:"code" binding:"required"`
	Channel         string `json:"channel" binding:"required"`
	Locale          string `json:"locale" binding:"required"`
	Subject         string `json:"subject"`
	Content         string `json:"content" binding:"required"`
	Status          string `json:"status"`
	ExpectedVersion int64  `json:"expected_version"`
}
type PutProviderRequest struct {
	TenantID        string `json:"tenant_id" binding:"required"`
	ApplicationID   string `json:"application_id" binding:"required"`
	Code            string `json:"code" binding:"required"`
	Channel         string `json:"channel" binding:"required"`
	Upstream        string `json:"upstream" binding:"required"`
	Path            string `json:"path" binding:"required"`
	Priority        int    `json:"priority"`
	Status          string `json:"status"`
	ExpectedVersion int64  `json:"expected_version"`
}
type ListProvidersRequest struct {
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
	Keyword       string `json:"keyword"`
	Channel       string `json:"channel"`
	Status        string `json:"status"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}
type SendNotificationRequest struct {
	TenantID       string            `json:"tenant_id" binding:"required"`
	ApplicationID  string            `json:"application_id" binding:"required"`
	TemplateCode   string            `json:"template_code" binding:"required"`
	Channel        string            `json:"channel" binding:"required"`
	Locale         string            `json:"locale" binding:"required"`
	Recipient      string            `json:"recipient" binding:"required"`
	Variables      map[string]string `json:"variables"`
	IdempotencyKey string            `json:"idempotency_key" binding:"required"`
}
type ListTemplatesRequest struct {
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
	Keyword       string `json:"keyword"`
	Channel       string `json:"channel"`
	Status        string `json:"status"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}
type GetDeliveryRequest struct {
	ID            string `json:"id" binding:"required"`
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
}
type ListDeliveriesRequest struct {
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
	Status        string `json:"status"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}
type ProviderReceiptRequest struct {
	TenantID          string `json:"tenant_id" binding:"required"`
	ApplicationID     string `json:"application_id" binding:"required"`
	Provider          string `json:"provider" binding:"required"`
	ProviderMessageID string `json:"provider_message_id" binding:"required"`
	Status            string `json:"status" binding:"required"`
	FailureReason     string `json:"failure_reason"`
}

type TemplatePageResponseBody struct {
	Templates []TemplateResponseBody `json:"templates"`
	Total     int64                  `json:"total"`
	Page      int                    `json:"page"`
	PageSize  int                    `json:"page_size"`
}
type ProviderPageResponseBody struct {
	Providers []ProviderResponseBody `json:"providers"`
	Total     int64                  `json:"total"`
	Page      int                    `json:"page"`
	PageSize  int                    `json:"page_size"`
}
type ProviderResponseBody struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	ApplicationID string    `json:"application_id"`
	Code          string    `json:"code"`
	Channel       string    `json:"channel"`
	Upstream      string    `json:"upstream"`
	Path          string    `json:"path"`
	Priority      int       `json:"priority"`
	Status        string    `json:"status"`
	Version       int64     `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	CreatedBy     string    `json:"created_by"`
	UpdatedBy     string    `json:"updated_by"`
}

type TemplateResponseBody struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	ApplicationID string    `json:"application_id"`
	Code          string    `json:"code"`
	Channel       string    `json:"channel"`
	Locale        string    `json:"locale"`
	Subject       string    `json:"subject"`
	Content       string    `json:"content"`
	Status        string    `json:"status"`
	Version       int64     `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	CreatedBy     string    `json:"created_by"`
	UpdatedBy     string    `json:"updated_by"`
}

type DeliveryResponseBody struct {
	ID                string    `json:"id"`
	TenantID          string    `json:"tenant_id"`
	ApplicationID     string    `json:"application_id"`
	TemplateCode      string    `json:"template_code"`
	Channel           string    `json:"channel"`
	Locale            string    `json:"locale"`
	Recipient         string    `json:"recipient"`
	Variables         any       `json:"variables"`
	IdempotencyKey    string    `json:"idempotency_key"`
	Status            string    `json:"status"`
	Provider          string    `json:"provider"`
	ProviderMessageID string    `json:"provider_message_id"`
	FailureReason     string    `json:"failure_reason"`
	Attempts          int32     `json:"attempts"`
	NextAttemptAt     time.Time `json:"next_attempt_at"`
	Version           int64     `json:"version"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	CreatedBy         string    `json:"created_by"`
	UpdatedBy         string    `json:"updated_by"`
}

type DeliveryPageResponseBody struct {
	Deliveries []DeliveryResponseBody `json:"deliveries"`
	Total      int64                  `json:"total"`
	Page       int                    `json:"page"`
	PageSize   int                    `json:"page_size"`
}

type MeResponseBody struct {
	Subject string `json:"subject"`
}

// Login godoc
// @Summary Issue a JWT access token
// @Tags authentication
// @Accept json
// @Produce json
// @Param request body LoginRequest true "Client credentials"
// @Success 200 {object} Response{body=LoginResponseBody}
// @Failure 400 {object} Response "Code 10001: invalid request"
// @Failure 401 {object} Response "Code 20001: invalid credentials"
// @Failure 429 {object} Response "Code 10029: rate limited"

// Live godoc
// @Summary Check process liveness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Router /live [post]
func (h *Handler) Live(c *gin.Context) { OK(c, h.health.Live()) }

// Ready godoc
// @Summary Check database and Redis readiness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Failure 503 {object} Response{body=health.Status} "Code 50003: dependency unavailable"
// @Router /ready [post]
func (h *Handler) Ready(c *gin.Context) {
	status, ready := h.health.Ready(c.Request.Context())
	if !ready {
		c.JSON(503, Response{Code: apperror.CodeDependencyUnavailable, Message: "service is not ready", Body: status, RequestID: requestID(c)})
		return
	}
	OK(c, status)
}

// Me godoc
// @Summary Return the authenticated subject
// @Tags authentication
// @Produce json
// @Security Bearer
// @Success 200 {object} Response{body=MeResponseBody}
// @Failure 401 {object} Response "Code 20001: unauthorized"
// @Router /api/v1/me [post]
func (h *Handler) Me(c *gin.Context) {
	subject, _ := c.Get("subject")
	OK(c, gin.H{"subject": subject})
}

// PutNotificationProvider godoc
// @Summary Create or update a notification provider route
// @Tags notifications
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body PutProviderRequest true "Provider route"
// @Success 200 {object} Response{body=ProviderResponseBody}
// @Router /api/v1/notifications/providers/put [post]
func (h *Handler) PutNotificationProvider(c *gin.Context) {
	var request PutProviderRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid request body", err))
		return
	}
	value, err := h.notifications.PutProvider(c.Request.Context(), notificationdomain.Provider{TenantID: request.TenantID, ApplicationID: request.ApplicationID, Code: request.Code, Channel: request.Channel, Upstream: request.Upstream, Path: request.Path, Priority: request.Priority, Status: request.Status}, request.ExpectedVersion)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, providerResponse(value))
}

// ListNotificationProviders godoc
// @Summary List notification provider routes
// @Tags notifications
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListProvidersRequest true "Provider filters and pagination"
// @Success 200 {object} Response{body=ProviderPageResponseBody}
// @Router /api/v1/notifications/providers/list [post]
func (h *Handler) ListNotificationProviders(c *gin.Context) {
	var request ListProvidersRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid request body", err))
		return
	}
	page, err := h.notifications.ListProviders(c.Request.Context(), request.TenantID, request.ApplicationID, request.Keyword, request.Channel, request.Status, request.Page, request.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	providers := make([]ProviderResponseBody, 0, len(page.Providers))
	for _, value := range page.Providers {
		providers = append(providers, providerResponse(value))
	}
	OK(c, ProviderPageResponseBody{Providers: providers, Total: page.Total, Page: page.Page, PageSize: page.PageSize})
}

func providerResponse(value notificationdomain.Provider) ProviderResponseBody {
	return ProviderResponseBody{ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, Code: value.Code, Channel: value.Channel, Upstream: value.Upstream, Path: value.Path, Priority: value.Priority, Status: value.Status, Version: value.Version, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}

// Version godoc
// @Summary Return build and runtime version information
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=buildinfo.Info}
// @Router /api/v1/version [post]
func (h *Handler) Version(c *gin.Context) { OK(c, buildinfo.Current()) }

// @Summary Create or update notification template
// @Tags notification
// @Security Bearer
// @Param request body PutTemplateRequest true "Template"
// @Success 200 {object} Response{body=TemplateResponseBody}
// @Router /api/v1/notifications/templates/put [post]
func (h *Handler) PutNotificationTemplate(c *gin.Context) {
	var r PutTemplateRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.PutTemplate(c.Request.Context(), notificationdomain.Template{TenantID: r.TenantID, ApplicationID: r.ApplicationID, Code: r.Code, Channel: r.Channel, Locale: r.Locale, Subject: r.Subject, Content: r.Content, Status: r.Status}, r.ExpectedVersion)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, templateResponse(v))
}

// ListNotificationTemplates godoc
// @Summary List notification templates
// @Tags notification
// @Security Bearer
// @Param request body ListTemplatesRequest true "Template filter"
// @Success 200 {object} Response{body=TemplatePageResponseBody}
// @Router /api/v1/notifications/templates/list [post]
func (h *Handler) ListNotificationTemplates(c *gin.Context) {
	var request ListTemplatesRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	page, err := h.notifications.ListTemplates(c.Request.Context(), request.TenantID, request.ApplicationID, request.Keyword, request.Channel, request.Status, request.Page, request.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	templates := make([]TemplateResponseBody, 0, len(page.Templates))
	for _, item := range page.Templates {
		templates = append(templates, templateResponse(item))
	}
	OK(c, TemplatePageResponseBody{Templates: templates, Total: page.Total, Page: page.Page, PageSize: page.PageSize})
}

// SendNotification godoc
// @Summary Queue a notification for asynchronous delivery
// @Tags notification
// @Security Bearer
// @Param request body SendNotificationRequest true "Notification"
// @Success 200 {object} Response{body=DeliveryResponseBody}
// @Router /api/v1/notifications/send [post]
func (h *Handler) SendNotification(c *gin.Context) {
	var r SendNotificationRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.Send(c.Request.Context(), r.TenantID, r.ApplicationID, r.TemplateCode, r.Channel, r.Locale, r.Recipient, r.IdempotencyKey, r.Variables)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, deliveryResponse(v))
}

// RecordProviderReceipt godoc
// @Summary Record an authenticated provider delivery receipt
// @Tags notification
// @Security PSK
// @Param request body ProviderReceiptRequest true "Provider receipt"
// @Success 200 {object} Response{body=DeliveryResponseBody}
// @Router /api/v1/notifications/providers/receipt [post]
func (h *Handler) RecordProviderReceipt(c *gin.Context) {
	var r ProviderReceiptRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.RecordReceipt(c.Request.Context(), r.TenantID, r.ApplicationID, r.Provider, r.ProviderMessageID, r.Status, r.FailureReason)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, deliveryResponse(v))
}

// GetNotificationDelivery godoc
// @Summary Get notification delivery status
// @Tags notification
// @Security Bearer
// @Param request body GetDeliveryRequest true "Delivery ID"
// @Success 200 {object} Response{body=DeliveryResponseBody}
// @Router /api/v1/notifications/deliveries/get [post]
func (h *Handler) GetNotificationDelivery(c *gin.Context) {
	var r GetDeliveryRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.GetDelivery(c.Request.Context(), r.ID, r.TenantID, r.ApplicationID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, deliveryResponse(v))
}

// ListNotificationDeliveries godoc
// @Summary List notification deliveries
// @Tags notification
// @Security Bearer
// @Param request body ListDeliveriesRequest true "Delivery filter"
// @Success 200 {object} Response{body=DeliveryPageResponseBody}
// @Router /api/v1/notifications/deliveries/list [post]
func (h *Handler) ListNotificationDeliveries(c *gin.Context) {
	var r ListDeliveriesRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.ListDeliveries(c.Request.Context(), r.TenantID, r.ApplicationID, r.Status, r.Page, r.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	responses := make([]DeliveryResponseBody, 0, len(v.Deliveries))
	for _, delivery := range v.Deliveries {
		responses = append(responses, deliveryResponse(delivery))
	}
	OK(c, DeliveryPageResponseBody{Deliveries: responses, Total: v.Total, Page: v.Page, PageSize: v.PageSize})
}

func deliveryResponse(value notificationdomain.Delivery) DeliveryResponseBody {
	var variables any
	if len(value.Variables) > 0 {
		_ = json.Unmarshal(value.Variables, &variables)
	}
	return DeliveryResponseBody{
		ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, TemplateCode: value.TemplateCode, Channel: value.Channel,
		Locale: value.Locale, Recipient: value.Recipient, Variables: variables, IdempotencyKey: value.IdempotencyKey,
		Status: value.Status, Provider: value.Provider, ProviderMessageID: value.ProviderMessageID,
		FailureReason: value.FailureReason, Attempts: value.Attempts, NextAttemptAt: value.NextAttemptAt,
		Version: value.Version, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
		CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func templateResponse(value notificationdomain.Template) TemplateResponseBody {
	return TemplateResponseBody{
		ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, Code: value.Code, Channel: value.Channel, Locale: value.Locale,
		Subject: value.Subject, Content: value.Content, Status: value.Status, Version: value.Version,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

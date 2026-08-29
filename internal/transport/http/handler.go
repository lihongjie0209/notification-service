package httptransport

import (
	"log/slog"

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
	Code            string `json:"code" binding:"required"`
	Channel         string `json:"channel" binding:"required"`
	Locale          string `json:"locale" binding:"required"`
	Subject         string `json:"subject"`
	Content         string `json:"content" binding:"required"`
	Status          string `json:"status"`
	ExpectedVersion int64  `json:"expected_version"`
}
type SendNotificationRequest struct {
	TenantID       string            `json:"tenant_id" binding:"required"`
	TemplateCode   string            `json:"template_code" binding:"required"`
	Channel        string            `json:"channel" binding:"required"`
	Locale         string            `json:"locale" binding:"required"`
	Recipient      string            `json:"recipient" binding:"required"`
	Variables      map[string]string `json:"variables"`
	IdempotencyKey string            `json:"idempotency_key" binding:"required"`
}
type GetDeliveryRequest struct {
	ID       string `json:"id" binding:"required"`
	TenantID string `json:"tenant_id" binding:"required"`
}
type ListDeliveriesRequest struct {
	TenantID string `json:"tenant_id" binding:"required"`
	Status   string `json:"status"`
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
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
// @Success 200 {object} Response{body=notification.Template}
// @Router /api/v1/notifications/templates/put [post]
func (h *Handler) PutNotificationTemplate(c *gin.Context) {
	var r PutTemplateRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.PutTemplate(c.Request.Context(), notificationdomain.Template{TenantID: r.TenantID, Code: r.Code, Channel: r.Channel, Locale: r.Locale, Subject: r.Subject, Content: r.Content, Status: r.Status}, r.ExpectedVersion)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, v)
}

// SendNotification godoc
// @Summary Queue a notification for asynchronous delivery
// @Tags notification
// @Security Bearer
// @Param request body SendNotificationRequest true "Notification"
// @Success 200 {object} Response{body=notification.Delivery}
// @Router /api/v1/notifications/send [post]
func (h *Handler) SendNotification(c *gin.Context) {
	var r SendNotificationRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.Send(c.Request.Context(), r.TenantID, r.TemplateCode, r.Channel, r.Locale, r.Recipient, r.IdempotencyKey, r.Variables)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, v)
}

// GetNotificationDelivery godoc
// @Summary Get notification delivery status
// @Tags notification
// @Security Bearer
// @Param request body GetDeliveryRequest true "Delivery ID"
// @Success 200 {object} Response{body=notification.Delivery}
// @Router /api/v1/notifications/deliveries/get [post]
func (h *Handler) GetNotificationDelivery(c *gin.Context) {
	var r GetDeliveryRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.GetDelivery(c.Request.Context(), r.ID, r.TenantID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, v)
}

// ListNotificationDeliveries godoc
// @Summary List notification deliveries
// @Tags notification
// @Security Bearer
// @Param request body ListDeliveriesRequest true "Delivery filter"
// @Success 200 {object} Response{body=notification.Page}
// @Router /api/v1/notifications/deliveries/list [post]
func (h *Handler) ListNotificationDeliveries(c *gin.Context) {
	var r ListDeliveriesRequest
	if err := c.ShouldBindJSON(&r); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return
	}
	v, err := h.notifications.ListDeliveries(c.Request.Context(), r.TenantID, r.Status, r.Page, r.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, v)
}

// CreateUser godoc
// @Summary Create a user
// @Tags users
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body CreateUserRequest true "User"
// @Success 200 {object} Response{body=user.User}
// @Failure 400 {object} Response "Code 10001: invalid request"
// @Failure 409 {object} Response "Code 30009: email already exists"

// GetUser godoc
// @Summary Get a user
// @Tags users
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body GetUserRequest true "User ID"
// @Success 200 {object} Response{body=user.User}
// @Failure 404 {object} Response "Code 10004: user not found"

// ListUsers godoc
// @Summary List users
// @Tags users
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListUsersRequest true "Pagination"
// @Success 200 {object} Response{body=user.Page}

// UpdateUser godoc
// @Summary Update a user using optimistic locking
// @Tags users
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body UpdateUserRequest true "User and current version"
// @Success 200 {object} Response{body=user.User}
// @Failure 409 {object} Response "Code 30009: version conflict"

// DeleteUser godoc
// @Summary Delete a user using optimistic locking
// @Tags users
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body DeleteUserRequest true "User ID and current version"
// @Success 200 {object} Response

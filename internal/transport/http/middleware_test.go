package httptransport

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	platformauthz "github.com/lihongjie0209/microservice-platform-go/authz"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/notification-service/internal/auth"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/idempotency"
)

type fakeIdempotencyManager struct {
	decision  idempotency.Decision
	beginKey  string
	completed *Response
}

func (*fakeIdempotencyManager) Enabled() bool { return true }
func (m *fakeIdempotencyManager) Begin(_ context.Context, key, _ string) (idempotency.Decision, error) {
	m.beginKey = key
	return m.decision, nil
}
func (m *fakeIdempotencyManager) Complete(_ context.Context, _, _ string, response any) error {
	value, ok := response.(Response)
	if ok {
		m.completed = &value
	}
	return nil
}
func (*fakeIdempotencyManager) Fail(context.Context, string, string, idempotency.Failure) error {
	return nil
}

func TestIdempotencyExecutionCompletesAndReplaysNotificationSend(t *testing.T) {
	t.Parallel()
	manager := &fakeIdempotencyManager{decision: idempotency.Decision{State: idempotency.StateAcquired, Owner: "owner-1"}}
	calls := 0
	router := gin.New()
	router.Use(RequestID(), func(c *gin.Context) {
		c.Set("subject", "user-1")
		c.Request = c.Request.WithContext(idempotency.WithContext(c.Request.Context(), "operation-1"))
		c.Next()
	}, IdempotencyExecution(manager, []string{"/api/v1/notifications/send"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	router.POST("/api/v1/notifications/send", func(c *gin.Context) { calls++; OK(c, gin.H{"delivery_id": "delivery-1"}) })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/send", strings.NewReader(`{"recipient":"a@example.test"}`)))
	if calls != 1 || manager.beginKey != "operation-1" || manager.completed == nil || manager.completed.RequestID != "" {
		t.Fatalf("calls=%d key=%q completed=%+v", calls, manager.beginKey, manager.completed)
	}
	stored, err := json.Marshal(*manager.completed)
	if err != nil {
		t.Fatal(err)
	}
	manager.decision = idempotency.Decision{State: idempotency.StateCompleted, Response: stored}
	replay := httptest.NewRequest(http.MethodPost, "/api/v1/notifications/send", strings.NewReader(`{"recipient":"a@example.test"}`))
	replay.Header.Set("X-Request-ID", "current-request")
	replayRecorder := httptest.NewRecorder()
	router.ServeHTTP(replayRecorder, replay)
	var response Response
	if err := json.Unmarshal(replayRecorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || response.RequestID != "current-request" {
		t.Fatalf("calls=%d response=%+v", calls, response)
	}
}

func TestIdempotencyExecutionBypassesNotificationQueries(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"/api/v1/notifications/templates/list", "/api/v1/notifications/deliveries/get", "/api/v1/notifications/deliveries/list"} {
		t.Run(route, func(t *testing.T) {
			manager := &fakeIdempotencyManager{decision: idempotency.Decision{State: idempotency.StateConflict}}
			calls := 0
			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Request = c.Request.WithContext(idempotency.WithContext(c.Request.Context(), "operation-1"))
				c.Next()
			}, IdempotencyExecution(manager, []string{"/api/v1/notifications/send"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
			router.POST(route, func(c *gin.Context) { calls++; OK(c, nil) })
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, route, nil))
			if calls != 1 || recorder.Code != http.StatusOK || manager.beginKey != "" {
				t.Fatalf("calls=%d status=%d key=%q", calls, recorder.Code, manager.beginKey)
			}
		})
	}
}

type authorizationStub struct{ err error }

func (a authorizationStub) Authorize(context.Context, platformprincipal.Principal, platformauthz.Requirement) error {
	return a.err
}

func TestNotificationHTTPRequirementCoversRoutesAndScopes(t *testing.T) {
	t.Parallel()
	routes := []string{"/api/v1/notifications/templates/put", "/api/v1/notifications/templates/list", "/api/v1/notifications/providers/put", "/api/v1/notifications/providers/list", "/api/v1/notifications/send", "/api/v1/notifications/providers/receipt", "/api/v1/notifications/deliveries/get", "/api/v1/notifications/deliveries/list"}
	for _, route := range routes {
		requirement, ok := notificationHTTPRequirement(route)
		if !ok || requirement.Resource == "" || requirement.Action == "" {
			t.Fatalf("route %q requirement = %+v, %v", route, requirement, ok)
		}
	}
	receipt, _ := notificationHTTPRequirement("/api/v1/notifications/providers/receipt")
	send, _ := notificationHTTPRequirement("/api/v1/notifications/send")
	if receipt.Scope != platformauthz.ScopePlatform || send.Scope != platformauthz.ScopePrincipal {
		t.Fatalf("unexpected scopes: receipt=%v send=%v", receipt.Scope, send.Scope)
	}
}

func TestAuthorizationFailsClosedAndClassifiesOutage(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{{"denied", platformauthz.ErrDenied, http.StatusForbidden}, {"unavailable", platformauthz.ErrDecisionUnavailable, http.StatusServiceUnavailable}} {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.Use(RequestID(), func(c *gin.Context) {
				c.Request = c.Request.WithContext(platformprincipal.WithContext(c.Request.Context(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1", MembershipID: "membership-1"}))
				c.Next()
			}, Authorization(true, authorizationStub{test.err}, slog.New(slog.NewTextHandler(io.Discard, nil))))
			router.POST("/api/v1/notifications/send", func(c *gin.Context) { OK(c, nil) })
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/send", nil))
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d", recorder.Code, test.status)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID())
	router.POST("/test", func(c *gin.Context) { OK(c, nil) })
	request := httptest.NewRequest(http.MethodPost, "/test", nil)
	request.Header.Set("X-Request-ID", "client-request-1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Request-ID"); got != "client-request-1" {
		t.Fatalf("X-Request-ID = %q", got)
	}
	var response Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.RequestID != "client-request-1" {
		t.Fatalf("request_id = %q", response.RequestID)
	}
}

func TestAuthentication_PSKPrecedesSkipAndJWT(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	const key = "01234567890123456789012345678901"
	service := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}})
	for _, test := range []struct {
		name   string
		header string
		status int
	}{
		{name: "valid PSK", header: "PSK " + key, status: http.StatusOK},
		{name: "PSK route does not become public", status: http.StatusUnauthorized},
		{name: "bearer cannot access PSK route", header: "Bearer invalid", status: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			router := gin.New()
			router.Use(RequestID(), Authentication(service, slog.New(slog.NewTextHandler(io.Discard, nil)), config.Auth{
				SkipHTTPPaths: []string{"/api/v1/external/*"},
				PSK:           config.PSK{Enabled: true, Key: key, HTTPPaths: []string{"/api/v1/external/*"}},
			}))
			router.POST("/api/v1/external/callback", func(c *gin.Context) {
				value, ok := platformprincipal.FromContext(c.Request.Context())
				if test.status == http.StatusOK && (!ok || value.ID != "notification-service:psk" || value.Type != platformprincipal.TypeServiceAccount) {
					c.AbortWithStatus(http.StatusInternalServerError)
					return
				}
				OK(c, nil)
			})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/external/callback", nil)
			request.Header.Set("Authorization", test.header)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d", recorder.Code, test.status)
			}
		})
	}
}

func TestRequireJSON(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID(), RequireJSON())
	router.POST("/test", func(c *gin.Context) { OK(c, nil) })
	request := httptest.NewRequest(http.MethodPost, "/test", io.NopCloser(&oneByteReader{}))
	request.ContentLength = 1
	request.Header.Set("Content-Type", "text/plain")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestTimeoutPropagatesCancellation(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := gin.New()
	router.Use(RequestID(), Timeout(time.Millisecond, logger))
	router.POST("/test", func(c *gin.Context) { <-c.Request.Context().Done() })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/test", nil))
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGatewayTimeout)
	}
}

type oneByteReader struct{}

func (*oneByteReader) Read(buffer []byte) (int, error) { buffer[0] = 'x'; return 1, io.EOF }

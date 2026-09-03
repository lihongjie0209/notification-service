package grpctransport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	platformauthz "github.com/lihongjie0209/microservice-platform-go/authz"
	platformidempotency "github.com/lihongjie0209/microservice-platform-go/idempotency"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/notification-service/internal/apperror"
	"github.com/lihongjie0209/notification-service/internal/auth"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/environment"
	apphealth "github.com/lihongjie0209/notification-service/internal/health"
	"github.com/lihongjie0209/notification-service/internal/idempotency"
	notificationdomain "github.com/lihongjie0209/notification-service/internal/notification"
	"github.com/lihongjie0209/notification-service/internal/observability"
	"github.com/lihongjie0209/notification-service/internal/requestid"

	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	notificationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/notification/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	server  *grpc.Server
	address string
	logger  *slog.Logger
}

func NewServer(lc fx.Lifecycle, cfg config.Config, authService *auth.Service, authorizer platformauthz.Authorizer, healthService *apphealth.Service, notificationService *notificationdomain.Service, idempotencyManager *idempotency.Manager, metrics *observability.Metrics, logger *slog.Logger) (*Server, error) {
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.GRPC.MaxReceiveBytes),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(environmentInterceptor(cfg.Runtime.ActiveProfile), requestIDInterceptor, idempotencyInterceptor, recoveryInterceptor(logger), authInterceptor(authService, cfg.Auth), platformauthz.UnaryServerInterceptor(authorizer, notificationGRPCRequirement(cfg.Authorization.Enabled)), platformidempotency.UnaryServerInterceptor(idempotencyManager, cfg.Idempotency.GRPCMethods, logger), metricsInterceptor(metrics, logger)),
		grpc.ChainStreamInterceptor(environmentStreamInterceptor(cfg.Runtime.ActiveProfile), requestIDStreamInterceptor, idempotencyStreamInterceptor, recoveryStreamInterceptor(logger), authStreamInterceptor(authService, cfg.Auth), metricsStreamInterceptor(metrics, logger)),
	}
	if cfg.GRPC.TLS.Enabled {
		creds, err := serverCredentials(cfg.GRPC.TLS)
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.Creds(creds))
	}
	grpcServer := grpc.NewServer(options...)
	notificationv1.RegisterNotificationServiceServer(grpcServer, &notificationServer{service: notificationService})
	grpc_health_v1.RegisterHealthServer(grpcServer, &healthServer{health: healthService})
	if cfg.GRPC.ReflectionEnabled {
		reflection.Register(grpcServer)
	}
	server := &Server{server: grpcServer, address: cfg.GRPC.Address, logger: logger}
	lc.Append(fx.Hook{OnStart: server.start(cfg.GRPC.Enabled), OnStop: server.stop})
	return server, nil
}

func notificationGRPCRequirement(enabled bool) platformauthz.GRPCResolver {
	return func(method string) (platformauthz.Requirement, bool) {
		if !enabled {
			return platformauthz.Requirement{}, false
		}
		requirements := map[string]platformauthz.Requirement{
			notificationv1.NotificationService_PutProvider_FullMethodName:           {Resource: "notification.provider", Action: "update", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_GetProvider_FullMethodName:           {Resource: "notification.provider", Action: "read", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_ListProviders_FullMethodName:         {Resource: "notification.provider", Action: "list", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_PutTemplate_FullMethodName:           {Resource: "notification.template", Action: "update", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_GetTemplate_FullMethodName:           {Resource: "notification.template", Action: "read", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_ListTemplates_FullMethodName:         {Resource: "notification.template", Action: "list", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_Send_FullMethodName:                  {Resource: "notification.delivery", Action: "send", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_RecordProviderReceipt_FullMethodName: {Resource: "notification.receipt", Action: "record", Scope: platformauthz.ScopePlatform},
			notificationv1.NotificationService_GetDelivery_FullMethodName:           {Resource: "notification.delivery", Action: "read", Scope: platformauthz.ScopePrincipal},
			notificationv1.NotificationService_ListDeliveries_FullMethodName:        {Resource: "notification.delivery", Action: "list", Scope: platformauthz.ScopePrincipal},
		}
		requirement, ok := requirements[method]
		return requirement, ok
	}
}

type notificationServer struct {
	notificationv1.UnimplementedNotificationServiceServer
	service *notificationdomain.Service
}

func (s *notificationServer) PutProvider(ctx context.Context, request *notificationv1.PutProviderRequest) (*notificationv1.PutProviderResponse, error) {
	if request.GetProvider() == nil {
		return nil, status.Error(codes.InvalidArgument, "provider is required")
	}
	value := request.GetProvider()
	result, err := s.service.PutProvider(ctx, notificationdomain.Provider{TenantID: value.GetTenantId(), ApplicationID: value.GetApplicationId(), Code: value.GetCode(), Channel: value.GetChannel(), Upstream: value.GetUpstream(), Path: value.GetPath(), Priority: int(value.GetPriority()), Status: value.GetStatus()}, request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.PutProviderResponse{Provider: toProtoProvider(result)}, nil
}

func (s *notificationServer) GetProvider(ctx context.Context, request *notificationv1.GetProviderRequest) (*notificationv1.GetProviderResponse, error) {
	result, err := s.service.GetProvider(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetCode())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.GetProviderResponse{Provider: toProtoProvider(result)}, nil
}

func (s *notificationServer) ListProviders(ctx context.Context, request *notificationv1.ListProvidersRequest) (*notificationv1.ListProvidersResponse, error) {
	page, pageSize := 0, 0
	if request.GetPage() != nil {
		page, pageSize = int(request.GetPage().GetPage()), int(request.GetPage().GetPageSize())
	}
	result, err := s.service.ListProviders(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetKeyword(), request.GetChannel(), request.GetStatus(), page, pageSize)
	if err != nil {
		return nil, grpcError(err)
	}
	response := &notificationv1.ListProvidersResponse{Page: &commonv1.PageResult{Total: uint64(result.Total), Page: uint32(result.Page), PageSize: uint32(result.PageSize)}}
	for _, value := range result.Providers {
		response.Providers = append(response.Providers, toProtoProvider(value))
	}
	return response, nil
}

func (s *notificationServer) PutTemplate(ctx context.Context, r *notificationv1.PutTemplateRequest) (*notificationv1.PutTemplateResponse, error) {
	if r.GetTemplate() == nil {
		return nil, status.Error(codes.InvalidArgument, "template is required")
	}
	v := r.GetTemplate()
	result, err := s.service.PutTemplate(ctx, notificationdomain.Template{TenantID: v.GetTenantId(), ApplicationID: v.GetApplicationId(), Code: v.GetCode(), Channel: v.GetChannel(), Locale: v.GetLocale(), Subject: v.GetSubject(), Content: v.GetContent(), Status: v.GetStatus()}, r.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.PutTemplateResponse{Template: toProtoTemplate(result)}, nil
}

func (s *notificationServer) GetTemplate(ctx context.Context, request *notificationv1.GetTemplateRequest) (*notificationv1.GetTemplateResponse, error) {
	result, err := s.service.GetTemplate(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetCode(), request.GetChannel(), request.GetLocale())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.GetTemplateResponse{Template: toProtoTemplate(result)}, nil
}
func (s *notificationServer) ListTemplates(ctx context.Context, r *notificationv1.ListTemplatesRequest) (*notificationv1.ListTemplatesResponse, error) {
	page, pageSize := 0, 0
	if r.GetPage() != nil {
		page, pageSize = int(r.GetPage().GetPage()), int(r.GetPage().GetPageSize())
	}
	result, err := s.service.ListTemplates(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetKeyword(), r.GetChannel(), r.GetStatus(), page, pageSize)
	if err != nil {
		return nil, grpcError(err)
	}
	response := &notificationv1.ListTemplatesResponse{Page: &commonv1.PageResult{Total: uint64(result.Total), Page: uint32(result.Page), PageSize: uint32(result.PageSize)}}
	for _, value := range result.Templates {
		response.Templates = append(response.Templates, toProtoTemplate(value))
	}
	return response, nil
}
func (s *notificationServer) Send(ctx context.Context, r *notificationv1.SendRequest) (*notificationv1.SendResponse, error) {
	v, err := s.service.Send(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetTemplateCode(), r.GetChannel(), r.GetLocale(), r.GetRecipient(), r.GetIdempotencyKey(), r.GetVariables())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.SendResponse{Delivery: toProtoDelivery(v)}, nil
}
func (s *notificationServer) RecordProviderReceipt(ctx context.Context, r *notificationv1.RecordProviderReceiptRequest) (*notificationv1.RecordProviderReceiptResponse, error) {
	v, err := s.service.RecordReceipt(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetProvider(), r.GetProviderMessageId(), r.GetStatus(), r.GetFailureReason())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.RecordProviderReceiptResponse{Delivery: toProtoDelivery(v)}, nil
}
func (s *notificationServer) GetDelivery(ctx context.Context, r *notificationv1.GetDeliveryRequest) (*notificationv1.GetDeliveryResponse, error) {
	v, err := s.service.GetDelivery(ctx, r.GetId(), r.GetTenantId(), r.GetApplicationId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &notificationv1.GetDeliveryResponse{Delivery: toProtoDelivery(v)}, nil
}
func (s *notificationServer) ListDeliveries(ctx context.Context, r *notificationv1.ListDeliveriesRequest) (*notificationv1.ListDeliveriesResponse, error) {
	page, pageSize := 0, 0
	if r.GetPage() != nil {
		page, pageSize = int(r.GetPage().GetPage()), int(r.GetPage().GetPageSize())
	}
	result, err := s.service.ListDeliveries(ctx, r.GetTenantId(), r.GetApplicationId(), r.GetStatus(), page, pageSize)
	if err != nil {
		return nil, grpcError(err)
	}
	response := &notificationv1.ListDeliveriesResponse{Page: &commonv1.PageResult{Total: uint64(result.Total), Page: uint32(result.Page), PageSize: uint32(result.PageSize)}}
	for _, v := range result.Deliveries {
		response.Deliveries = append(response.Deliveries, toProtoDelivery(v))
	}
	return response, nil
}
func toProtoProvider(value notificationdomain.Provider) *notificationv1.Provider {
	return &notificationv1.Provider{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, Code: value.Code, Channel: value.Channel, Upstream: value.Upstream, Path: value.Path, Priority: int32(value.Priority), Status: value.Status, Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func toProtoTemplate(v notificationdomain.Template) *notificationv1.Template {
	return &notificationv1.Template{Id: v.ID, TenantId: v.TenantID, ApplicationId: v.ApplicationID, Code: v.Code, Channel: v.Channel, Locale: v.Locale, Subject: v.Subject, Content: v.Content, Status: v.Status, Version: v.Version, CreatedAt: timestamppb.New(v.CreatedAt), UpdatedAt: timestamppb.New(v.UpdatedAt), CreatedBy: v.CreatedBy, UpdatedBy: v.UpdatedBy}
}
func toProtoDelivery(v notificationdomain.Delivery) *notificationv1.Delivery {
	variables := map[string]string{}
	_ = json.Unmarshal(v.Variables, &variables)
	return &notificationv1.Delivery{Id: v.ID, TenantId: v.TenantID, ApplicationId: v.ApplicationID, TemplateCode: v.TemplateCode, Channel: v.Channel, Recipient: v.Recipient, Variables: variables, Status: v.Status, Provider: v.Provider, ProviderMessageId: v.ProviderMessageID, FailureReason: v.FailureReason, Attempts: v.Attempts, NextAttemptAt: timestamppb.New(v.NextAttemptAt), Version: v.Version, CreatedAt: timestamppb.New(v.CreatedAt), UpdatedAt: timestamppb.New(v.UpdatedAt), CreatedBy: v.CreatedBy, UpdatedBy: v.UpdatedBy}
}

func (s *Server) start(enabled bool) func(context.Context) error {
	return func(context.Context) error {
		if !enabled {
			s.logger.Warn("grpc server is disabled")
			return nil
		}
		listener, err := net.Listen("tcp", s.address)
		if err != nil {
			return fmt.Errorf("listen grpc: %w", err)
		}
		go func() {
			if err := s.server.Serve(listener); err != nil {
				s.logger.Error("grpc server stopped unexpectedly", "error", err)
			}
		}()
		s.logger.Info("grpc server started", "address", s.address)
		return nil
	}
}
func (s *Server) stop(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() { s.server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		return ctx.Err()
	}
}

type healthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	health *apphealth.Service
}

func grpcError(err error) error {
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return status.Error(codes.Internal, "internal server error")
	}
	code := codes.Internal
	switch appErr.Code {
	case apperror.CodeInvalidArgument:
		code = codes.InvalidArgument
	case apperror.CodeUnauthorized:
		code = codes.Unauthenticated
	case apperror.CodeForbidden:
		code = codes.PermissionDenied
	case apperror.CodeNotFound:
		code = codes.NotFound
	case apperror.CodeConflict:
		code = codes.Aborted
	case apperror.CodeDependencyUnavailable:
		code = codes.Unavailable
	}
	return status.Error(code, appErr.Message)
}

func (s *healthServer) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	_, ready := s.health.Ready(ctx)
	serving := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if ready {
		serving = grpc_health_v1.HealthCheckResponse_SERVING
	}
	return &grpc_health_v1.HealthCheckResponse{Status: serving}, nil
}
func (s *healthServer) List(context.Context, *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
	return &grpc_health_v1.HealthListResponse{Statuses: map[string]*grpc_health_v1.HealthCheckResponse{"": {Status: grpc_health_v1.HealthCheckResponse_SERVING}}}, nil
}

func requestIDInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	id := ""
	if values := metadata.ValueFromIncomingContext(ctx, "x-request-id"); len(values) > 0 && requestid.Valid(values[0]) {
		id = values[0]
	}
	if id == "" {
		id = requestid.Generate()
	}
	header := metadata.Pairs("x-request-id", id)
	_ = grpc.SetHeader(ctx, header)
	return handler(requestid.WithContext(ctx, id), req)
}
func idempotencyInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	values := metadata.ValueFromIncomingContext(ctx, "idempotency-key")
	if len(values) == 0 {
		return handler(ctx, req)
	}
	if !idempotency.Valid(values[0]) {
		return nil, status.Error(codes.InvalidArgument, "invalid idempotency-key")
	}
	return handler(idempotency.WithContext(ctx, values[0]), req)
}
func environmentInterceptor(profile string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(environment.WithContext(ctx, profile), req)
	}
}
func authInterceptor(service *auth.Service, cfg config.Auth) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authCtx, err := authenticateGRPC(ctx, info.FullMethod, service, cfg)
		if err != nil {
			return nil, err
		}
		return handler(authCtx, req)
	}
}

func authenticateGRPC(ctx context.Context, method string, service *auth.Service, cfg config.Auth) (context.Context, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if cfg.PSK.Enabled && auth.MatchesAny(method, cfg.PSK.GRPCMethods) {
		if len(values) == 0 || !auth.VerifyPSK(values[0], cfg.PSK.Key) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid PSK")
		}
		return platformprincipal.WithContext(ctx, platformprincipal.Principal{ID: "notification-service:psk", Type: platformprincipal.TypeServiceAccount}), nil
	}
	if auth.MatchesAny(method, cfg.SkipGRPCMethods) {
		return ctx, nil
	}
	if len(values) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing bearer token")
	}
	scheme, raw, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	caller, err := service.Verify(ctx, raw)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
	}
	return platformprincipal.WithContext(ctx, caller), nil
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context { return s.ctx }

func environmentStreamInterceptor(profile string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: environment.WithContext(stream.Context(), profile)})
	}
}

func requestIDStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	id := ""
	if values := metadata.ValueFromIncomingContext(ctx, "x-request-id"); len(values) > 0 && requestid.Valid(values[0]) {
		id = values[0]
	}
	if id == "" {
		id = requestid.Generate()
	}
	if err := stream.SetHeader(metadata.Pairs("x-request-id", id)); err != nil {
		return status.Error(codes.Internal, "set request metadata")
	}
	return handler(srv, &contextServerStream{ServerStream: stream, ctx: requestid.WithContext(ctx, id)})
}

func idempotencyStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	values := metadata.ValueFromIncomingContext(stream.Context(), "idempotency-key")
	if len(values) == 0 {
		return handler(srv, stream)
	}
	if !idempotency.Valid(values[0]) {
		return status.Error(codes.InvalidArgument, "invalid idempotency-key")
	}
	return handler(srv, &contextServerStream{ServerStream: stream, ctx: idempotency.WithContext(stream.Context(), values[0])})
}

func authStreamInterceptor(service *auth.Service, cfg config.Auth) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticateGRPC(stream.Context(), info.FullMethod, service, cfg)
		if err != nil {
			return err
		}
		return handler(srv, &contextServerStream{ServerStream: stream, ctx: ctx})
	}
}

func recoveryStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(stream.Context(), "grpc stream panic recovered", "method", info.FullMethod, "panic", recovered)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, stream)
	}
}

func metricsStreamInterceptor(metrics *observability.Metrics, logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		err := handler(srv, stream)
		code := status.Code(err)
		if metrics.Enabled() {
			metrics.GRPCRequests.WithLabelValues(info.FullMethod, code.String()).Inc()
			metrics.GRPCDuration.WithLabelValues(info.FullMethod).Observe(time.Since(started).Seconds())
		}
		requestID, _ := requestid.FromContext(stream.Context())
		logger.InfoContext(stream.Context(), "grpc stream", "request_id", requestID, "method", info.FullMethod, "code", code.String(), "duration", time.Since(started))
		return err
	}
}

func recoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(ctx, "grpc panic recovered", "method", info.FullMethod, "panic", recovered)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}
func metricsInterceptor(metrics *observability.Metrics, logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, req)
		code := status.Code(err)
		if metrics.Enabled() {
			metrics.GRPCRequests.WithLabelValues(info.FullMethod, code.String()).Inc()
			metrics.GRPCDuration.WithLabelValues(info.FullMethod).Observe(time.Since(started).Seconds())
		}
		span := trace.SpanFromContext(ctx).SpanContext()
		requestID, _ := requestid.FromContext(ctx)
		logger.InfoContext(ctx, "grpc request", "request_id", requestID, "trace_id", span.TraceID().String(), "span_id", span.SpanID().String(), "method", info.FullMethod, "code", code.String(), "duration", time.Since(started))
		return response, err
	}
}

func serverCredentials(cfg config.GRPCTLS) (credentials.TransportCredentials, error) {
	certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load grpc certificate: %w", err)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
	if cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read grpc client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse grpc client CA")
		}
		tlsConfig.ClientCAs = pool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsConfig), nil
}

var Module = fx.Module("grpc", fx.Provide(NewServer), fx.Invoke(func(*Server) {}))

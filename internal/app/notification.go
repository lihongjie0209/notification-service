package app

import (
	"context"
	"errors"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/lihongjie0209/notification-service/internal/config"
	"github.com/lihongjie0209/notification-service/internal/notification"
	"github.com/lihongjie0209/notification-service/internal/outbound"
	applicationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/application/v1"
	"go.uber.org/fx"
)

type disabledApplicationVerifier struct{}

func (disabledApplicationVerifier) Verify(context.Context, string, string) error { return nil }

func newApplicationVerifier(cfg config.Config, registry *outbound.Registry) (appaccess.Verifier, error) {
	if !cfg.Database.Enabled {
		return disabledApplicationVerifier{}, nil
	}
	if registry == nil {
		return nil, errors.New("notification service requires outbound registry")
	}
	connection, ok := registry.GRPC("application")
	if !ok {
		return nil, errors.New("notification service requires outbound.grpc.application")
	}
	return appaccess.NewGRPCVerifier(applicationv1.NewApplicationServiceClient(connection), 2*time.Second), nil
}

var NotificationModule = fx.Module("notification", fx.Provide(notification.NewRepository, newApplicationVerifier, notification.NewRuntimeService))

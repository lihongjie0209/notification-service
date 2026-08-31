package notification

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/lihongjie0209/notification-service/internal/config"
	"go.uber.org/fx"
)

type RetentionCleaner struct {
	repository Repository
	logger     *slog.Logger
	retention  time.Duration
	interval   time.Duration
	batchSize  int
	enabled    bool
	now        func() time.Time
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewRetentionCleaner(lifecycle fx.Lifecycle, repository Repository, logger *slog.Logger, cfg config.Config) (*RetentionCleaner, error) {
	if repository == nil || logger == nil {
		return nil, errors.New("notification retention cleaner dependencies are required")
	}
	if cfg.Notification.Retention <= 0 {
		cfg.Notification.Retention = 30 * 24 * time.Hour
	}
	if cfg.Notification.CleanupInterval <= 0 {
		cfg.Notification.CleanupInterval = time.Hour
	}
	if cfg.Notification.CleanupBatchSize <= 0 {
		cfg.Notification.CleanupBatchSize = 500
	}
	cleaner := &RetentionCleaner{
		repository: repository,
		logger:     logger,
		retention:  cfg.Notification.Retention,
		interval:   cfg.Notification.CleanupInterval,
		batchSize:  cfg.Notification.CleanupBatchSize,
		enabled:    cfg.Database.Enabled,
		now:        time.Now,
	}
	lifecycle.Append(fx.Hook{OnStart: cleaner.start, OnStop: cleaner.stop})
	return cleaner, nil
}

func (c *RetentionCleaner) clean(ctx context.Context) error {
	for {
		deleted, err := c.repository.DeleteTerminalDeliveriesBefore(ctx, c.now().Add(-c.retention), c.batchSize)
		if err != nil {
			return err
		}
		if deleted > 0 {
			c.logger.InfoContext(ctx, "deleted expired notification deliveries", "count", deleted)
		}
		if deleted < int64(c.batchSize) {
			return nil
		}
	}
}

func (c *RetentionCleaner) start(context.Context) error {
	if !c.enabled {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			if err := c.clean(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.ErrorContext(ctx, "clean expired notification deliveries", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (c *RetentionCleaner) stop(context.Context) error {
	if c.cancel != nil {
		c.cancel()
		c.wg.Wait()
	}
	return nil
}

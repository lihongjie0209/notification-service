package notification

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lihongjie0209/notification-service/internal/config"
	"go.uber.org/fx/fxtest"
)

type retentionRepository struct {
	Repository
	counts []int64
	before []time.Time
}

func (r *retentionRepository) DeleteTerminalDeliveriesBefore(_ context.Context, before time.Time, _ int) (int64, error) {
	r.before = append(r.before, before)
	count := r.counts[0]
	r.counts = r.counts[1:]
	return count, nil
}

func TestRetentionCleanerDeletesInBoundedBatches(t *testing.T) {
	t.Parallel()

	repository := &retentionRepository{counts: []int64{2, 1}}
	cleaner, err := NewRetentionCleaner(
		fxtest.NewLifecycle(t),
		repository,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		config.Config{Database: config.Database{Enabled: true}, Notification: config.Notification{Retention: 24 * time.Hour, CleanupInterval: time.Hour, CleanupBatchSize: 2}},
	)
	if err != nil {
		t.Fatalf("NewRetentionCleaner() error = %v", err)
	}
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	cleaner.now = func() time.Time { return now }

	if err := cleaner.clean(context.Background()); err != nil {
		t.Fatalf("clean() error = %v", err)
	}
	if len(repository.before) != 2 {
		t.Fatalf("delete calls = %d, want 2", len(repository.before))
	}
	if want := now.Add(-24 * time.Hour); !repository.before[0].Equal(want) {
		t.Fatalf("cutoff = %v, want %v", repository.before[0], want)
	}
}

func TestNewRetentionCleanerAppliesSafeDefaults(t *testing.T) {
	t.Parallel()

	cleaner, err := NewRetentionCleaner(fxtest.NewLifecycle(t), &retentionRepository{}, slog.Default(), config.Config{})
	if err != nil {
		t.Fatalf("NewRetentionCleaner() error = %v", err)
	}
	if cleaner.retention != 30*24*time.Hour || cleaner.interval != time.Hour || cleaner.batchSize != 500 {
		t.Fatalf("unexpected defaults: retention=%v interval=%v batch=%d", cleaner.retention, cleaner.interval, cleaner.batchSize)
	}
}

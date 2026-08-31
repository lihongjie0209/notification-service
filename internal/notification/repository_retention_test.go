package notification

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestSQLRepositoryDeleteTerminalDeliveriesBefore(t *testing.T) {
	t.Parallel()

	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := NewRepository(sqlx.NewDb(database, "sqlmock"))
	before := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM notification_deliveries WHERE status IN ('sent','dead_letter') AND updated_at<? ORDER BY updated_at,id LIMIT ?")).
		WithArgs(before, 2).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("delivery-1").AddRow("delivery-2"))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM notification_deliveries WHERE id IN (?, ?) AND status IN ('sent','dead_letter') AND updated_at<?")).
		WithArgs("delivery-1", "delivery-2", before).
		WillReturnResult(sqlmock.NewResult(0, 2))

	deleted, err := repository.DeleteTerminalDeliveriesBefore(t.Context(), before, 2)
	if err != nil {
		t.Fatalf("DeleteTerminalDeliveriesBefore() error = %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

package notification

import (
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestSQLRepositoryListProvidersIsScopedAndBounded(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := NewRepository(sqlx.NewDb(database, "sqlmock"))
	where := " WHERE tenant_id=? AND application_id=? AND (?='' OR code LIKE ? OR upstream LIKE ?) AND (?='' OR channel=?) AND (?='' OR status=?)"
	args := []driver.Value{"tenant-1", "app-1", "mail", "%mail%", "%mail%", "email", "email", "active", "active"}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM notification_providers" + where)).WithArgs(args...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	now := time.Date(2026, 9, 3, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + providerColumns + " FROM notification_providers" + where + " ORDER BY priority,code LIMIT ? OFFSET ?")).WithArgs(append(args, 20, 20)...).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "application_id", "code", "channel", "upstream", "path", "priority", "status", "version", "created_at", "updated_at", "created_by", "updated_by"}).AddRow("provider-1", "tenant-1", "app-1", "mail-primary", "email", "mail", "/send", 10, "active", 1, now, now, "admin", "admin"))

	providers, total, err := repository.ListProviders(t.Context(), "tenant-1", "app-1", "mail", "email", "active", 20, 20)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(providers) != 1 || providers[0].Code != "mail-primary" {
		t.Fatalf("providers=%+v total=%d", providers, total)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

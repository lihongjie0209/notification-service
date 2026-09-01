package migration

import (
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMigrationVersionsAreUniquePerDialect(t *testing.T) {
	t.Parallel()
	for _, dialect := range []string{"postgres", "kingbase", "mysql"} {
		dialect := dialect
		t.Run(dialect, func(t *testing.T) {
			entries, err := os.ReadDir(filepath.Join("..", "..", "migrations", dialect))
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]map[string]string)
			for _, entry := range entries {
				parts := strings.Split(entry.Name(), ".")
				if entry.IsDir() || len(parts) != 3 || (parts[1] != "up" && parts[1] != "down") {
					continue
				}
				version, _, ok := strings.Cut(parts[0], "_")
				if !ok {
					t.Fatalf("migration %q has no version prefix", entry.Name())
				}
				if seen[version] == nil {
					seen[version] = make(map[string]string)
				}
				if previous := seen[version][parts[1]]; previous != "" {
					t.Fatalf("migration version %s has duplicate %s files: %s and %s", version, parts[1], previous, entry.Name())
				}
				seen[version][parts[1]] = entry.Name()
			}
			for version, directions := range seen {
				if directions["up"] == "" || directions["down"] == "" {
					t.Fatalf("migration version %s must have one up and one down file: %#v", version, directions)
				}
			}
		})
	}
}

func TestWithMigrationTable(t *testing.T) {
	t.Parallel()
	result, err := withMigrationOptions("postgres://user:pass@db/app?sslmode=disable", "orders_db", "orders_schema_migrations", "orders")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(result)
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Query().Get("x-migrations-table"); got != "orders_schema_migrations" {
		t.Fatalf("x-migrations-table = %q", got)
	}
	if got := parsed.Query().Get("sslmode"); got != "disable" {
		t.Fatalf("sslmode = %q", got)
	}
	if got := parsed.Query().Get("search_path"); got != "orders" {
		t.Fatalf("search_path = %q", got)
	}
	if parsed.Path != "/orders_db" {
		t.Fatalf("database path = %q", parsed.Path)
	}
}

func TestCreateSchemaSerializesConcurrentBootstrap(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	mock.ExpectExec(`SELECT pg_advisory_xact_lock\(hashtext\(\$1\)\)`).WithArgs("notification-service:schema:orders").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`CREATE SCHEMA IF NOT EXISTS "orders"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := createSchema((*sql.DB)(db), "orders"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWithMigrationTableMySQLDSN(t *testing.T) {
	t.Parallel()
	result, err := withMigrationOptions("mysql://app:app@tcp(mysql:3306)/app", "orders_db", "orders_schema_migrations", "ignored")
	if err != nil {
		t.Fatal(err)
	}
	const expected = "mysql://app:app@tcp(mysql:3306)/orders_db?x-migrations-table=orders_schema_migrations"
	if result != expected {
		t.Fatalf("result = %q, want %q", result, expected)
	}
}

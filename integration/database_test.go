//go:build integration

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lihongjie0209/notification-service/internal/config"
	appdb "github.com/lihongjie0209/notification-service/internal/database"
	"github.com/lihongjie0209/notification-service/internal/migration"
	notificationdomain "github.com/lihongjie0209/notification-service/internal/notification"
	"github.com/lihongjie0209/notification-service/internal/principal"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestRepositoryAndMigrations(t *testing.T) {
	for _, databaseType := range []string{"postgres", "mysql"} {
		t.Run(databaseType, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			dsn, migrationURL := startDatabase(t, ctx, databaseType)
			migrationPath, err := filepath.Abs(filepath.Join("..", "migrations", databaseType))
			if err != nil {
				t.Fatal(err)
			}
			schema := ""
			if databaseType == "postgres" {
				schema = "integration_postgres"
			}
			migrationCfg := config.Migration{Path: migrationPath, DatabaseURL: migrationURL, Table: "integration_" + databaseType + "_schema_migrations", Schema: schema, CreateSchema: schema != ""}
			migrationErrors := make(chan error, 3)
			var migrations sync.WaitGroup
			for range 3 {
				migrations.Add(1)
				go func() {
					defer migrations.Done()
					migrationErrors <- migration.Run(migrationCfg, "up", 0)
				}()
			}
			migrations.Wait()
			close(migrationErrors)
			for err := range migrationErrors {
				if err != nil {
					t.Fatalf("concurrent migration up: %v", err)
				}
			}

			db, err := appdb.Open(ctx, config.Database{Type: databaseType, DSN: dsn, Schema: schema, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			service := notificationdomain.NewService(notificationdomain.NewRepository(db), appdb.NewTransactor(db), nil, config.Config{})
			actorCtx := principal.WithContext(ctx, principal.Principal{Subject: "admin-1", Method: principal.AuthenticationJWT})
			template, err := service.PutTemplate(actorCtx, notificationdomain.Template{TenantID: "tenant-1", Code: "welcome", Channel: "email", Locale: "zh-cn", Subject: "Welcome", Content: "Hello {{.name}}"}, 0)
			if err != nil || template.Version != 1 {
				t.Fatalf("put template=%+v err=%v", template, err)
			}
			first, err := service.Send(actorCtx, "tenant-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", map[string]string{"name": "Alice"})
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			replay, err := service.Send(actorCtx, "tenant-1", "welcome", "email", "zh-cn", "a@example.com", "send-1", map[string]string{"name": "Alice"})
			if err != nil || replay.ID != first.ID {
				t.Fatalf("replay=%+v err=%v", replay, err)
			}
			if _, err := db.ExecContext(ctx, db.Rebind(`UPDATE notification_deliveries SET status='sent',provider='email',provider_message_id='provider-1',version=version+1 WHERE id=?`), first.ID); err != nil {
				t.Fatalf("simulate provider send: %v", err)
			}
			receiptCtx := principal.WithContext(ctx, principal.Principal{Subject: "provider:email", Method: principal.AuthenticationPSK})
			delivered, err := service.RecordReceipt(receiptCtx, "tenant-1", "email", "provider-1", "delivered", "")
			if err != nil || delivered.Status != "delivered" || delivered.Version != 3 {
				t.Fatalf("receipt=%+v err=%v", delivered, err)
			}
			var userTables int
			if databaseType == "postgres" {
				if err := db.GetContext(ctx, &userTables, `SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = 'users'`); err != nil {
					t.Fatal(err)
				}
				var timezone string
				if err := db.GetContext(ctx, &timezone, `SHOW TIMEZONE`); err != nil || timezone != "Asia/Shanghai" {
					t.Fatalf("timezone=%q err=%v", timezone, err)
				}
			} else if err := db.GetContext(ctx, &userTables, `SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'users'`); err != nil {
				t.Fatal(err)
			}
			if userTables != 0 {
				t.Fatal("generic template migration must not create a users table")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := migration.Run(migrationCfg, "down", 0); err != nil {
				t.Fatalf("migration down: %v", err)
			}
		})
	}
}

func startDatabase(t *testing.T, ctx context.Context, databaseType string) (string, string) {
	t.Helper()
	switch databaseType {
	case "postgres":
		container, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
		if err != nil {
			t.Fatal(err)
		}
		testcontainers.CleanupContainer(t, container)
		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		return dsn, dsn
	case "mysql":
		container, err := mysql.Run(ctx, "mysql:8.4", mysql.WithDatabase("app"), mysql.WithUsername("app"), mysql.WithPassword("app"))
		if err != nil {
			t.Fatal(err)
		}
		testcontainers.CleanupContainer(t, container)
		dsn, err := container.ConnectionString(ctx, "parseTime=true")
		if err != nil {
			t.Fatal(err)
		}
		migrationDSN, err := container.ConnectionString(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return dsn, "mysql://" + migrationDSN
	default:
		t.Fatal(fmt.Errorf("unsupported database %q", databaseType))
		return "", ""
	}
}

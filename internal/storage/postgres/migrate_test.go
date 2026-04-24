package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupMigratePostgres(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()

	pgC, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("pipeline"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgC.Terminate(ctx)
		t.Fatalf("conn string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		_ = pgC.Terminate(ctx)
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		_ = pgC.Terminate(ctx)
		t.Fatalf("ping: %v", err)
	}

	cleanup := func() {
		pool.Close()
		if err := pgC.Terminate(ctx); err != nil {
			t.Logf("warning: terminate: %v", err)
		}
	}
	return pool, cleanup
}

func TestMigrate_AppliesAllMigrations(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratePostgres(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)

	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count < 1 {
		t.Fatalf("expected >=1 migration applied, got %d", count)
	}

	// Spot-check key objects (tables or materialized views).
	objects := []string{
		"heroes", "items", "matches", "player_matches",
		"player_match_details", "teams", "public_matches",
		"mv_hero_winrate_patch",
	}
	for _, name := range objects {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables WHERE table_name = $1
			) OR EXISTS (
				SELECT 1 FROM pg_matviews WHERE matviewname = $1
			)`, name).Scan(&exists)
		if err != nil {
			t.Fatalf("check %s: %v", name, err)
		}
		if !exists {
			t.Errorf("object %q does not exist after migration", name)
		}
	}
}

func TestMigrate_Idempotent(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratePostgres(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)

	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate() error = %v", err)
	}

	var before int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM schema_migrations").Scan(&before); err != nil {
		t.Fatalf("count before: %v", err)
	}
	if before == 0 {
		t.Fatal("no migrations were applied on first run — check //go:embed")
	}

	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}

	var after int
	if err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM schema_migrations").Scan(&after); err != nil {
		t.Fatalf("count after: %v", err)
	}

	if after != before {
		t.Errorf("migration count changed on re-run: before=%d after=%d", before, after)
	}
}

func TestMigrate_PartitionsCreated(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratePostgres(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	if err := repo.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	var n int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pg_inherits i
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'matches'`).Scan(&n)
	if err != nil {
		t.Fatalf("partitions query: %v", err)
	}
	if n < 8 {
		t.Errorf("expected >=8 partitions for matches, got %d", n)
	}
}
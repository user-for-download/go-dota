package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const migrationAdvisoryLockID int64 = 727274

var MigrationsDir = "/app/migrations"

func init() {
	if v := os.Getenv("MIGRATIONS_DIR"); v != "" {
		MigrationsDir = v
	}
}

func (r *Repository) Migrate(ctx context.Context) error {
	log := slog.Default()
	log.Info("running migrations", "dir", MigrationsDir)

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SET lock_timeout = '60s'"); err != nil {
		return fmt.Errorf("set lock timeout: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SET lock_timeout = DEFAULT")
	}()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT PRIMARY KEY,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	entries, err := os.ReadDir(MigrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %q: %w", MigrationsDir, err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("no .sql files found in %q", MigrationsDir)
	}
	sort.Strings(files)
	log.Info("discovered migrations", "count", len(files))

	for _, name := range files {
		var exists bool
		_ = conn.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
			name,
		).Scan(&exists)
		if exists {
			continue
		}

		log.Info("applying migration", "file", name)
		path := filepath.Join(MigrationsDir, name)
		sqlText, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", path, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration tx: %w", err)
		}
		if _, err := tx.Exec(ctx, string(sqlText)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("exec migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)", name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}
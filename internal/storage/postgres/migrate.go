package postgres

import (
	"context"
	"embed"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationAdvisoryLockID int64 = 727274

func (r *Repository) Migrate(ctx context.Context) error {
	log := slog.Default()

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	// Ensure we can lock
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationAdvisoryLockID)
	}()

	// 1. Create migration table
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT PRIMARY KEY,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	// 2. Read embedded files
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations dir: %w", err)
	}
	log.Info("discovered migrations", "count", len(entries))

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}

	if len(files) == 0 {
		return fmt.Errorf("no .sql files found in embedded migrations directory")
	}
	sort.Strings(files)

	// 3. Apply
	for _, name := range files {
		var exists bool
		_ = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", name).Scan(&exists)

		if exists {
			continue
		}

		log.Info("applying migration", "file", name)
		sqlText, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration file %s: %w", name, err)
		}

		err = pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sqlText)); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
				return err
			}
			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", name, err)
		}
	}
	return nil
}

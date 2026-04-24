package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgresContainer(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	pgContainer, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("pipeline"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategyAndDeadline(
			120*time.Second,
			wait.ForExposedPort(),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("failed to ping postgres: %v", err)
	}

	cleanup := func() {
		pool.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("warning: failed to terminate container: %v", err)
		}
	}

	return pool, cleanup
}

func TestEnsureSchema(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'parsed_data'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("table existence check: %v", err)
	}
	if !exists {
		t.Error("parsed_data table not created")
	}
}

func TestUpsertParsedData(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	externalID := "test-ext-id-123"
	payload := json.RawMessage(`{"test":"data"}`)

	err := repo.UpsertParsedData(ctx, externalID, payload)
	if err != nil {
		t.Fatalf("UpsertParsedData() error = %v", err)
	}
}

func TestUpsertParsedDataConflict(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	externalID := "test-conflict-id"
	payload1 := json.RawMessage(`{"version":1}`)
	payload2 := json.RawMessage(`{"version":2}`)

	err := repo.UpsertParsedData(ctx, externalID, payload1)
	if err != nil {
		t.Fatalf("UpsertParsedData() first insert error = %v", err)
	}

	err = repo.UpsertParsedData(ctx, externalID, payload2)
	if err != nil {
		t.Fatalf("UpsertParsedData() second insert error = %v", err)
	}

	var result json.RawMessage
	err = pool.QueryRow(ctx,
		"SELECT payload FROM parsed_data WHERE external_id = $1",
		externalID).Scan(&result)
	if err != nil {
		t.Fatalf("query updated payload: %v", err)
	}

	var got struct{ Version int }
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Version != 2 {
		t.Errorf("payload not updated: got version=%d, want 2", got.Version)
	}
}

func TestFilterNewIDs(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	err := repo.UpsertParsedData(ctx, "123", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("UpsertParsedData() error = %v", err)
	}

	newIDs, err := repo.FilterNewIDs(ctx, []string{"123", "456", "789"})
	if err != nil {
		t.Fatalf("FilterNewIDs() error = %v", err)
	}

	want := map[string]bool{"456": true, "789": true}
	if len(newIDs) != len(want) {
		t.Fatalf("got %d new IDs, want %d", len(newIDs), len(want))
	}
	for _, id := range newIDs {
		if !want[id] {
			t.Errorf("unexpected ID in result: %s", id)
		}
	}
}

func TestFilterNewIDsEmpty(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	newIDs, err := repo.FilterNewIDs(ctx, []string{})
	if err != nil {
		t.Fatalf("FilterNewIDs() error = %v", err)
	}
	if len(newIDs) != 0 {
		t.Errorf("got %d new IDs, want 0", len(newIDs))
	}
}

func TestFilterNewIDsAllNew(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	newIDs, err := repo.FilterNewIDs(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("FilterNewIDs() error = %v", err)
	}
	if len(newIDs) != 3 {
		t.Errorf("got %d new IDs, want 3", len(newIDs))
	}
}

func TestFilterNewIDsAllExist(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewLegacyRepositoryFromPool(pool)

	if err := repo.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	for _, id := range []string{"x", "y", "z"} {
		if err := repo.UpsertParsedData(ctx, id, json.RawMessage(`{}`)); err != nil {
			t.Fatalf("UpsertParsedData() error = %v", err)
		}
	}

	newIDs, err := repo.FilterNewIDs(ctx, []string{"x", "y", "z"})
	if err != nil {
		t.Fatalf("FilterNewIDs() error = %v", err)
	}
	if len(newIDs) != 0 {
		t.Errorf("got %d new IDs, want 0", len(newIDs))
	}
}
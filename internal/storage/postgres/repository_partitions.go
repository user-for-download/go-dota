package postgres

import (
	"context"
	"fmt"
	"time"
)

type Partition struct {
	Name      string
	FromEpoch int64
	ToEpoch  int64
}

func (r *Repository) ListMatchPartitions(ctx context.Context) ([]Partition, error) {
	const q = `
		SELECT
			c.relname AS partition_name,
			pg_get_expr(c.relpartbound, c.oid) AS bound_def
		FROM pg_inherits i
		JOIN pg_class c  ON c.oid = i.inhrelid
		JOIN pg_class p  ON p.oid = i.inhparent
		WHERE p.relname = 'matches'
		ORDER BY c.relname`
	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	defer rows.Close()

	var out []Partition
	for rows.Next() {
		var p Partition
		var boundDef string
		if err := rows.Scan(&p.Name, &boundDef); err != nil {
			return nil, err
		}
		from, to, err := parsePartitionBounds(boundDef)
		if err != nil {
			continue
		}
		p.FromEpoch, p.ToEpoch = from, to
		out = append(out, p)
	}
	return out, rows.Err()
}

func parsePartitionBounds(expr string) (int64, int64, error) {
	var from, to int64
	_, err := fmt.Sscanf(expr, "FOR VALUES FROM ('%d') TO ('%d')", &from, &to)
	if err != nil {
		return 0, 0, err
	}
	return from, to, nil
}

func (r *Repository) CreateMatchPartition(ctx context.Context, name string, fromEpoch, toEpoch int64) error {
	q := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF matches
		FOR VALUES FROM ('%d') TO ('%d')
	`, name, fromEpoch, toEpoch)
	_, err := r.pool.Exec(ctx, q)
	return err
}

func (r *Repository) DetachMatchPartition(ctx context.Context, name string) error {
	q := fmt.Sprintf("ALTER TABLE matches DETACH PARTITION %s", name)
	_, err := r.pool.Exec(ctx, q)
	return err
}

func (r *Repository) DropMatchPartition(ctx context.Context, name string) error {
	q := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
	_, err := r.pool.Exec(ctx, q)
	return err
}

func MonthPartitionName(t time.Time) string {
	return fmt.Sprintf("matches_p_%04d_%02d", t.Year(), int(t.Month()))
}

func MonthBounds(t time.Time) (int64, int64) {
	t = t.UTC()
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	return start.Unix(), end.Unix()
}
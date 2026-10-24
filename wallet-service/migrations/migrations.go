// Package migrations creates the database tables. Every .sql file here is
// safe to run many times; they run in name order on every service start.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

// lockID is an arbitrary number; the advisory lock stops two processes
// (e.g. parallel test packages) from running migrations at the same time.
const lockID = 7_420_001

func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrations: acquire: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("migrations: lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID)

	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migrations: %s: %w", name, err)
		}
	}
	return nil
}

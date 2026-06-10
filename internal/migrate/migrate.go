package migrate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

// migrateLockKey is an arbitrary but stable key for the Postgres session-level
// advisory lock that serializes migrations across replicas. It is the only
// advisory lock the app takes, so any fixed value is safe. Without it, multiple
// sentinel-api replicas starting together would race to apply the same files.
const migrateLockKey int64 = 20240601

func Apply(ctx context.Context, db *pgxpool.Pool, dir string) ([]string, error) {
	// Serialize migrations across replicas: hold a session-level advisory lock
	// on a dedicated pooled connection for the duration of Apply. Concurrent
	// callers block on pg_advisory_lock until we release. Returning a connection
	// to the pool does NOT drop a session lock, so we must unlock explicitly.
	conn, err := db.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn for migrate lock: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrateLockKey); err != nil {
		return nil, fmt.Errorf("acquire migrate advisory lock: %w", err)
	}
	defer func() {
		// Best-effort unlock on a fresh context so shutdown cancellation can't
		// strand the lock on the pooled connection.
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrateLockKey)
	}()

	if _, err := db.Exec(ctx, schemaTable); err != nil {
		return nil, fmt.Errorf("ensure schema_migrations: %w", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	var applied []string
	for _, name := range files {
		var exists bool
		if err := db.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
			name).Scan(&exists); err != nil {
			return applied, fmt.Errorf("check %s: %w", name, err)
		}
		if exists {
			continue
		}

		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return applied, fmt.Errorf("read %s: %w", name, err)
		}

		tx, err := db.Begin(ctx)
		if err != nil {
			return applied, fmt.Errorf("begin tx %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(content)); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("apply %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("record %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return applied, fmt.Errorf("commit %s: %w", name, err)
		}
		applied = append(applied, name)
	}
	return applied, nil
}

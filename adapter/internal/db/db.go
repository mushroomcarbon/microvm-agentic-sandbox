package db

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// DB wraps the pgx connection pool with a few helpers. Handlers and services
// take *DB; for direct query access they can use db.Pool.
type DB struct {
	Pool *pgxpool.Pool
}

// New opens a connection pool to the given Postgres URL and verifies it's
// reachable with a Ping. The caller is responsible for calling Close.
func New(ctx context.Context, url string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}
	// Sane pool defaults; we're a single-instance adapter so these don't need
	// to be aggressive. Adjust upward if we start seeing pool contention.
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &DB{Pool: pool}, nil
}

// Migrate applies the embedded schema. Uses IF NOT EXISTS throughout so it's
// safe to run on every startup. Run inside a transaction so any partial
// failure rolls back cleanly.
func (db *DB) Migrate(ctx context.Context) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrate tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if Commit succeeded

	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrate tx: %w", err)
	}
	return nil
}

// Close closes the underlying pool. Safe to call multiple times.
func (db *DB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}
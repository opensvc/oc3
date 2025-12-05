package cdb

import (
	"context"
	"database/sql"
)

type (
	execContexter interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}
)

type DBTxer interface {
	Commit() error
	Rollback() error
}

type DBOperater interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

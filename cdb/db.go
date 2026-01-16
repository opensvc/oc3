package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type (
	execContexter interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}

	// DB exposes opensvc collector data management functions
	DB struct {
		DB DBOperater

		// DBLck is a pointer to DBLocker, used to manage concurrent access to
		// the database via locking mechanisms.
		DBLck *DBLocker

		Session *Session

		dbPool *sql.DB
		hasTx  bool
	}

	// DBLocker combines a database connection and a sync.Locker
	// for managing concurrent access.
	DBLocker struct {
		DB *sql.DB
		sync.Locker
	}

	DBTxer interface {
		Commit() error
		Rollback() error
	}

	DBOperater interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	}
)

var (
	dbLocker         *DBLocker
	dbLockerInitOnce sync.Once
)

func InitDbLocker(db *sql.DB) *DBLocker {
	dbLockerInitOnce.Do(func() {
		dbLocker = &DBLocker{DB: db, Locker: &sync.Mutex{}}
	})
	return dbLocker
}

func New(dbPool *sql.DB) *DB {
	return &DB{DB: dbPool, DBLck: InitDbLocker(dbPool), dbPool: dbPool}
}

func (odb *DB) CreateTx(ctx context.Context, opts *sql.TxOptions) error {
	if odb.hasTx {
		return fmt.Errorf("already in a transaction")
	}
	if tx, err := odb.dbPool.BeginTx(ctx, opts); err != nil {
		return err
	} else {
		odb.DB = tx
		odb.hasTx = true
		return nil
	}
}

func (odb *DB) CreateSession(ev eventPublisher) {
	odb.Session = &Session{
		db:     odb.DB,
		ev:     ev,
		tables: make(map[string]struct{}),
	}
}

func (oDb *DB) Commit() error {
	if !oDb.hasTx {
		return nil
	}
	tx, ok := oDb.DB.(DBTxer)
	if !ok {
		return nil
	}
	return tx.Commit()
}

func (oDb *DB) Rollback() error {
	if !oDb.hasTx {
		return nil
	}
	defer func() { oDb.hasTx = false }()
	tx, ok := oDb.DB.(DBTxer)
	if !ok {
		return nil
	}
	if r := recover(); r != nil {
		tx.Rollback()
		panic(r)
	}
	return tx.Rollback()
}

func (oDb *DB) SetChange(s ...string) {
	oDb.Session.SetChanges(s...)
}

func (oDb *DB) DeleteBatched(ctx context.Context, table, dateCol, orderbyCol string, batchSize int64, retention int, where string) (totalDeleted int64, batchCount int64, err error) {
	// The base SQL query for the batched deletion.
	// ORDER BY is crucial for consistent performance and avoiding lock conflicts.
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` < DATE_SUB(NOW(), INTERVAL %d DAY) %s ORDER BY `%s` LIMIT %d",
		table, dateCol, retention, where, orderbyCol, batchSize)

	for {
		batchCount++

		ctx, cancel := context.WithTimeout(ctx, time.Minute)

		// Execute the DELETE statement
		result, err := oDb.DB.ExecContext(ctx, query)
		cancel()
		if err != nil {
			return totalDeleted, batchCount, fmt.Errorf("%s: error executing batch %d: %w", table, batchCount, err)
		}

		// Check the number of affected rows
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return totalDeleted, batchCount, fmt.Errorf("%s: error checking affected rows for batch %d: %w", table, batchCount, err)
		}

		totalDeleted += rowsAffected
		if rowsAffected > 0 {
			slog.Debug(fmt.Sprintf("DeleteBatched: %s: batch %d: deleted %d rows. total deleted: %d", table, batchCount, rowsAffected, totalDeleted))
		}

		// If less than the batch size was deleted, we've reached the end of the matching rows.
		if rowsAffected < batchSize {
			return totalDeleted, batchCount, nil
		}

		// Add a short sleep to yield CPU time, preventing resource monopolization
		time.Sleep(10 * time.Millisecond)
	}
}

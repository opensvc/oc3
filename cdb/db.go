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

func (oDb *DB) CreateTx(ctx context.Context, opts *sql.TxOptions) error {
	if oDb.hasTx {
		return fmt.Errorf("already in a transaction")
	}
	if tx, err := oDb.dbPool.BeginTx(ctx, opts); err != nil {
		return err
	} else {
		oDb.DB = tx
		oDb.hasTx = true
		return nil
	}
}

// BeginTxWithControl starts a database transaction with additional control for commit or rollback based on execution flow.
// It returns a function to mark the transaction for commit, a cleanup function to finalize the transaction, and an error.
func (oDb *DB) BeginTxWithControl(ctx context.Context, log *slog.Logger, opts *sql.TxOptions) (markSuccess func(), endTx func(), err error) {
	var needCommit bool
	markSuccess = func() { needCommit = true }
	if err = oDb.CreateTx(ctx, opts); err != nil {
		return nil, nil, err
	}
	endTx = func() {
		if needCommit {
			if err := oDb.Commit(); err != nil {
				if log != nil {
					log.Error("Commit failed", "error", err)
				}
			}
		} else {
			if err := oDb.Rollback(); err != nil {
				if log != nil {
					log.Error("Commit failed", "error", err)
				}
			}
		}
	}
	return
}

func (oDb *DB) CreateSession(ev eventPublisher) {
	oDb.Session = &Session{
		db:     oDb.DB,
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
	if err := tx.Commit(); err != nil {
		return err
	}
	oDb.hasTx = false
	return nil
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
		count, err := oDb.execCountContext(ctx, query)
		cancel()
		if err != nil {
			return totalDeleted, batchCount, fmt.Errorf("%s: error executing batch %d: %w", table, batchCount, err)
		}

		// Check the number of affected rows
		totalDeleted += count
		if count > 0 {
			slog.Debug(fmt.Sprintf("DeleteBatched: %s: batch %d: deleted %d rows. total deleted: %d", table, batchCount, count, totalDeleted))
		}

		// If less than the batch size was deleted, we've reached the end of the matching rows.
		if count < batchSize {
			return totalDeleted, batchCount, nil
		}

		// Add a short sleep to yield CPU time, preventing resource monopolization
		time.Sleep(10 * time.Millisecond)
	}
}

// ExecContextAndCountRowsAffected executes the oDb.DB.ExecContext query with the provided context, returning the number of rows affected and an error.
func (oDb *DB) ExecContextAndCountRowsAffected(ctx context.Context, query string, args ...any) (int64, error) {
	return oDb.execCountContext(ctx, query, args...)
}

// execCountContext executes the oDb.DB.ExecContext query with the provided context and arguments, returning the number of affected rows and an error.
func (oDb *DB) execCountContext(ctx context.Context, query string, args ...any) (int64, error) {
	result, err := oDb.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	if result == nil {
		// len data may be 0, so no rows affected.
		return 0, nil
	}
	return result.RowsAffected()
}

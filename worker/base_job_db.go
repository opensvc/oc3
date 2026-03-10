package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/opensvc/oc3/cdb"
)

type (
	JobDB struct {
		dbPool *sql.DB

		// db is the generic DB operator
		db cdb.DBOperater

		// oDb is the DB collector helper
		oDb *cdb.DB

		now time.Time
	}
)

var (
	_ cdb.DBTxer = &JobDB{}
)

func (j *JobDB) ODBSetter(odb *cdb.DB) error {
	if j == nil {
		return fmt.Errorf("nil JobDB")
	}
	j.oDb = odb
	j.db = odb.DB
	return nil
}

func (j *JobDB) CreateTx(ctx context.Context) error {
	return j.oDb.CreateTx(ctx, nil)
}

func (j *JobDB) CommitAndCreateTx(ctx context.Context) error {
	if err := j.oDb.Commit(); err != nil {
		return fmt.Errorf("CommitAndCreateTx pre-commit: %w", err)
	}
	if err := j.oDb.CreateTx(ctx, nil); err != nil {
		return fmt.Errorf("CommitAndCreateTx: %w", err)
	}
	return nil
}

func (j *JobDB) Commit() error {
	return j.oDb.Commit()
}

func (j *JobDB) Rollback() error {
	return j.oDb.Rollback()
}

func (j *JobDB) DB() cdb.DBOperater {
	return j.db
}

func (j *JobDB) dbNow(ctx context.Context) (err error) {
	rows, err := j.db.QueryContext(ctx, "SELECT NOW()")
	if err != nil {
		return err
	}
	if rows == nil {
		return fmt.Errorf("no result rows for SELECT NOW()")
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("no result rows next for SELECT NOW()")
	}
	if err := rows.Scan(&j.now); err != nil {
		return err
	}
	return nil
}

func (d *JobDB) pushFromTableChanges(ctx context.Context) error {
	return d.oDb.Session.NotifyChanges(ctx)
}

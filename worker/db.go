package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type (

	// opensvcDB implements opensvc db functions
	opensvcDB struct {
		db DBOperater

		tChanges map[string]struct{}
	}

	DBTxer interface {
		Commit() error
		Rollback() error
	}

	DBOperater interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
)

func (oDb *opensvcDB) translateEncapNodename(ctx context.Context, svcID, nodeID string) (subNodeID, vmName, vmType string, err error) {
	const (
		query = "" +
			"SELECT `svcmon`.`node_id`, `svcmon`.`mon_vmname`, `svcmon`.`mon_vmtype`, `svcmon`.`mon_containerstatus`" +
			" FROM `nodes`, `svcmon`" +
			" WHERE (" +
			"   ((`svcmon`.`mon_vmname` = `nodes`.`nodename`)" +
			"    AND (`nodes`.`node_id` = ?)" +
			"   ) AND (`svcmon`.`svc_id` = ?))"
	)
	var (
		rows    *sql.Rows
		hasRow1 bool

		containerStatusMatch = []string{"up", "stdby up", "n/a"}
	)

	rows, err = oDb.db.QueryContext(ctx, query, nodeID, svcID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var subNodeIDT, vmNameT, vmTypeT, containerStatusT string
		if err = rows.Scan(&subNodeIDT, &vmNameT, &vmTypeT, &containerStatusT); err != nil {
			return
		}
		for _, v := range containerStatusMatch {
			if containerStatusT == v {
				subNodeID = subNodeIDT
				vmName = vmNameT
				vmType = vmTypeT
				return
			}
		}
		if !hasRow1 {
			hasRow1 = true
			subNodeID = subNodeIDT
			vmName = vmNameT
			vmType = vmTypeT
		}
	}
	err = rows.Err()
	return
}

func (oDb *opensvcDB) tableChange(s ...string) {
	for _, table := range s {
		oDb.tChanges[table] = struct{}{}
	}
}

func (oDb *opensvcDB) tableChanges() []string {
	var r []string
	for s := range oDb.tChanges {
		r = append(r, s)
	}
	return r
}

func (oDb *opensvcDB) updateTableModified(ctx context.Context, tableName string) error {
	defer logDuration("updateTableModified", time.Now())
	const (
		query = "" +
			"INSERT INTO `table_modified` VALUES (NULL, ?, NOW())" +
			" ON DUPLICATE KEY UPDATE `table_modified` = NOW()"
	)
	_, err := oDb.db.ExecContext(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("updateTableModified %s: %w", tableName, err)
	}
	return nil
}

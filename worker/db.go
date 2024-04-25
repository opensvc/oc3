package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type (
	DBApp struct {
		id  int64
		app string
	}

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

func (oDb *opensvcDB) appByName(ctx context.Context, app string) (bool, *DBApp, error) {
	const query = "SELECT id, app FROM apps WHERE app = ?"
	var (
		foundID  int64
		foundApp string
	)
	if app == "" {
		return false, nil, fmt.Errorf("can't find app from empty value")
	}
	err := oDb.db.QueryRowContext(ctx, query, app).Scan(&foundID, &foundApp)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil, nil
	case err != nil:
		return false, nil, err
	default:
		return true, &DBApp{id: foundID, app: foundApp}, nil
	}
}

func (oDb *opensvcDB) isAppAllowedForNodeID(ctx context.Context, nodeID, app string) (bool, error) {
	// TODO: apps_responsibles PRIMARY KEY (app_id, group_id)
	const query = "" +
		"SELECT count(*) FROM (" +
		"  SELECT COUNT(`t`.`group_id`) AS `c`" +
		"  FROM (" +
		"      SELECT `ar`.`group_id` FROM `nodes` `n`, `apps` `a`, `apps_responsibles` `ar`" +
		"      WHERE `n`.`app` = `a`.`app` AND `ar`.`app_id` = `a`.`id` AND `n`.`node_id` = ?" +
		"    UNION ALL" +
		"      SELECT `ar`.`group_id` FROM `apps` `a`, `apps_responsibles` `ar`" +
		"      WHERE `ar`.`app_id` = `a`.`id` AND `a`.`app` = ?" +
		"   ) AS `t` GROUP BY `t`.`group_id`) `u`" +
		"WHERE `u`.`c` = 2"
	var found int64
	err := oDb.db.QueryRowContext(ctx, query, nodeID, app).Scan(&found)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return found > 0, nil
	}
}

func (oDb *opensvcDB) getAppFromNodeAndCandidateApp(ctx context.Context, candidateApp string, node *DBNode) (string, error) {
	app := candidateApp
	if candidateApp == "" {
		app = node.app
	} else if ok, err := oDb.isAppAllowedForNodeID(ctx, node.nodeID, candidateApp); err != nil {
		return "", fmt.Errorf("can't detect if app %s is allowed: %w", candidateApp, err)
	} else if !ok {
		app = node.app
	}
	if ok, _, err := oDb.appByName(ctx, app); err != nil {
		return "", fmt.Errorf("can't verify guessed app %s: %w", app, err)
	} else if !ok {
		return "", fmt.Errorf("can't verify guessed app %s: app not found", app)
	}
	return app, nil
}

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

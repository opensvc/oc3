package worker

import (
	"context"
	"database/sql"
	"sync"

	"github.com/opensvc/oc3/cdb"
)

type (

	// opensvcDB implements opensvc db functions
	opensvcDB struct {
		db cdb.DBOperater

		// dbLck is a pointer to DBLocker, used to manage concurrent access to
		// the database via locking mechanisms.
		dbLck *DBLocker

		session *cdb.Session
	}

	// DBLocker is a struct that combines a database connection and a sync.Locker
	// for managing concurrent access.
	DBLocker struct {
		DB *sql.DB
		sync.Locker
	}
)

var (
	dbLocker         *DBLocker
	dbLockerInitOnce sync.Once
)

func initDbLocker(db *sql.DB) *DBLocker {
	dbLockerInitOnce.Do(func() {
		dbLocker = &DBLocker{DB: db, Locker: &sync.Mutex{}}
	})
	return dbLocker
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

func (oDb *opensvcDB) SetChange(s ...string) {
	oDb.session.SetChanges(s...)
}
